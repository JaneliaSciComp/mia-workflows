"""The buffer ledger: one SQLite file holding buffers, their volumes, consumers, every draw and the frozen lists,
plus the registry: what exists on disk (`assets`, by kind and key: a staged directory, a record's raw cache) and
which buffers reference it (`refs`), so a buffer can reuse what an earlier round staged and a retire can count
what is still in use. The registry is an index over the on-disk manifests (stage.json, files.json), rebuildable
from them; the files stay the source of truth.

Spec v0.5 §5 and §9.5: the ledger is the record of a run; replaying a run means replaying its descriptors.
"""

from __future__ import annotations

import dataclasses
import gzip
import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from .sampler import Level, Volume, servable

# a dataset's row on a buffer's status board (DatasetStatus.status); the worker package imports these
BOARD_STATUSES = (
    "pending",
    "waiting-dataset",
    "waiting-lock",
    "waiting-drop",
    "fetching",
    "blocked",
    "staging",
    "servable",
    "failed",
    "dropped",
)
EVENT_KINDS = (  # a buffer's lifecycle; `sourcing` and `staged` since the staged gate (spec §15)
    "created",
    "planned",
    "sourcing",
    "approved",
    "staged",
    "ready",
    "idle",
    "retired",
    "needs-human",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS buffers (id TEXT PRIMARY KEY, config TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS volumes (buffer TEXT, id TEXT, data TEXT NOT NULL, PRIMARY KEY (buffer, id));
CREATE TABLE IF NOT EXISTS consumers (id TEXT PRIMARY KEY, buffer TEXT, info TEXT NOT NULL, last_seen REAL);
CREATE TABLE IF NOT EXISTS draws (buffer TEXT, seq INTEGER, consumer TEXT, descriptor TEXT NOT NULL, ts REAL,
                                  PRIMARY KEY (buffer, seq));
CREATE TABLE IF NOT EXISTS frozen (buffer TEXT, split TEXT, idx INTEGER, descriptor TEXT NOT NULL,
                                   PRIMARY KEY (buffer, split, idx));
CREATE TABLE IF NOT EXISTS assets (kind TEXT, key TEXT, data TEXT NOT NULL, updated REAL, PRIMARY KEY (kind, key));
CREATE TABLE IF NOT EXISTS refs (buffer TEXT, kind TEXT, key TEXT, PRIMARY KEY (buffer, kind, key));
CREATE TABLE IF NOT EXISTS catalogs (ref TEXT PRIMARY KEY, synced_at REAL, count INTEGER);
CREATE TABLE IF NOT EXISTS records (ref TEXT, id TEXT, path TEXT NOT NULL, data TEXT NOT NULL,
                                    PRIMARY KEY (ref, path));
CREATE TABLE IF NOT EXISTS datasets (id TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at REAL);
CREATE TABLE IF NOT EXISTS buffer_datasets (buffer TEXT, record TEXT, status TEXT, reason TEXT, job TEXT,
                                            updated_at REAL, PRIMARY KEY (buffer, record));
CREATE TABLE IF NOT EXISTS buffer_events (buffer TEXT, seq INTEGER, kind TEXT, at REAL, issue INTEGER, event_id TEXT,
                                          detail TEXT, PRIMARY KEY (buffer, seq));
CREATE TABLE IF NOT EXISTS usage (buffer TEXT, day TEXT, draws INTEGER, consumers INTEGER, empty INTEGER,
                                  PRIMARY KEY (buffer, day));
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS assessments (id TEXT PRIMARY KEY, gate TEXT, subject_kind TEXT, subject_ref TEXT,
                                        verdict TEXT, author TEXT, at TEXT, data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS assessments_subject ON assessments (subject_kind, subject_ref);
CREATE TABLE IF NOT EXISTS budgets (root TEXT PRIMARY KEY, data TEXT NOT NULL, updated REAL);
CREATE TABLE IF NOT EXISTS subjects (kind TEXT, number INTEGER, key TEXT NOT NULL, stage TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0, issue INTEGER, data TEXT NOT NULL, updated REAL,
    PRIMARY KEY (kind, number), UNIQUE (kind, key));
CREATE TABLE IF NOT EXISTS timeline (kind TEXT, number INTEGER, seq INTEGER, at TEXT, round TEXT, stage TEXT,
    body TEXT NOT NULL, marker TEXT NOT NULL, mirrored_at TEXT, comment_url TEXT,
    PRIMARY KEY (kind, number, seq), UNIQUE (kind, number, marker));
CREATE TABLE IF NOT EXISTS actions (id INTEGER PRIMARY KEY, kind TEXT, number INTEGER, action TEXT, actor TEXT,
    at TEXT, key TEXT UNIQUE, payload TEXT NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS actions_subject ON actions (kind, number);
"""

ACTION_COLUMNS = ("id", "kind", "number", "action", "actor", "at", "key", "payload", "state", "detail")
NUMBER_EPOCH = 1767225600  # 2026-01-01T00:00:00Z: a new subject's number is the whole seconds since then

TIMELINE_COLUMNS = ("seq", "at", "round", "stage", "body", "marker", "mirrored_at", "comment_url")

SUBJECT_COLUMNS = ("kind", "number", "key", "stage", "revision", "issue", "data", "updated")

BOARD_COLUMNS = ("record", "status", "reason", "job", "updated_at")
EVENT_COLUMNS = ("seq", "kind", "at", "issue", "event_id", "detail")


def _num(value: float) -> str:
    """A cap for a message: 14 not 14.0, 500000000000 not 5e+11."""
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def _volume_from(data: dict) -> Volume:
    data = dict(data)
    data["levels"] = tuple(Level(**{**level, "shape": tuple(level["shape"])}) for level in data["levels"])
    data["shape"] = tuple(data["shape"])
    data["split_planes"] = {split: tuple(planes) for split, planes in data["split_planes"].items()}
    data["samples"] = {split: tuple(candidates) for split, candidates in data.get("samples", {}).items()}
    return Volume(**data)


class Ledger:
    """All reads and writes go through one connection guarded by a lock; writes are single transactions."""

    # ponytail: one process, one connection; a pool if the service ever runs multi-process

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._lock = threading.Lock()
        self._retiring = threading.Lock()  # one retire at a time: two would write the same archive's temp file
        # buffer -> its parsed volumes, dropped on every write to the volumes table (put_volume, set_volume_missing)
        # ponytail: whole-buffer cache; per-volume rows if a buffer ever holds thousands of volumes
        self._volumes: dict[str, list[Volume]] = {}
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def put_buffer(self, buffer: str, config: dict) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO buffers VALUES (?, ?)", (buffer, json.dumps(config)))

    def buffer(self, buffer: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT config FROM buffers WHERE id = ?", (buffer,)).fetchone()
        return json.loads(row[0]) if row else None

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """One locked BEGIN IMMEDIATE ... COMMIT; rolls back and re-raises on error."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def put_volume(self, buffer: str, volume: Volume, refs: Iterable[tuple[str, str]] = ()) -> None:
        """Upsert the volume and add its (kind, key) references in one transaction."""
        with self._transaction() as db:
            db.execute(
                "INSERT OR REPLACE INTO volumes VALUES (?, ?, ?)",
                (buffer, volume.id, json.dumps(dataclasses.asdict(volume))),
            )
            db.executemany("INSERT OR IGNORE INTO refs VALUES (?, ?, ?)", [(buffer, kind, key) for kind, key in refs])
            self._volumes.pop(buffer, None)

    def clear_frozen(self, buffer: str) -> int:
        """Drop the buffer's frozen eval lists (every split); returns the rows removed. Called when a volume's
        candidates change, since a frozen row names a candidate by index into the volume it was drawn from."""
        with self._transaction() as db:
            return int(db.execute("DELETE FROM frozen WHERE buffer = ?", (buffer,)).rowcount)

    def volumes(self, buffer: str) -> list[Volume]:
        """The buffer's volumes by id, parsed once per write (every `next` and `split` request reads them); a new list
        each call, so a caller may replace entries."""
        with self._lock:
            if buffer not in self._volumes:
                rows = self._db.execute("SELECT data FROM volumes WHERE buffer = ? ORDER BY id", (buffer,)).fetchall()
                self._volumes[buffer] = [_volume_from(json.loads(row[0])) for row in rows]
            return list(self._volumes[buffer])

    def set_volume_missing(self, buffer: str, volume_id: str, missing: bool, path: str) -> None:
        """Set the flag only while the volume is still registered at `path`, the one stat'ed: a volume re-registered
        at a new path meanwhile keeps its own flag."""
        with self._lock:
            self._db.execute(
                "UPDATE volumes SET data = json_set(data, '$.missing', json(?)) WHERE buffer = ? AND id = ? "
                "AND json_extract(data, '$.path') = ?",
                (json.dumps(missing), buffer, volume_id, path),
            )
            self._volumes.pop(buffer, None)

    # --- a buffer's status board, lifecycle events and summary: what the page shows -------------

    def put_buffer_dataset(
        self, buffer: str, record: str, status: str, reason: str = "", job: str | None = None
    ) -> dict:
        """Replace the record's row on the buffer's board; returns it."""
        row = (record, status, reason, job, time.time())
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO buffer_datasets VALUES (?, ?, ?, ?, ?, ?)", (buffer, *row))
        return dict(zip(BOARD_COLUMNS, row, strict=True))

    def buffer_datasets(self, buffer: str) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                f"SELECT {', '.join(BOARD_COLUMNS)} FROM buffer_datasets WHERE buffer = ? ORDER BY record", (buffer,)
            ).fetchall()
        return [dict(zip(BOARD_COLUMNS, row, strict=True)) for row in rows]

    def add_event(self, buffer: str, kind: str, *, issue: int | None, event_id: str | None, detail: str) -> dict:
        """Append one event with the buffer's next seq, at now; returns it."""
        with self._transaction() as db:
            (last,) = db.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM buffer_events WHERE buffer = ?", (buffer,)
            ).fetchone()
            row = (last + 1, kind, time.time(), issue, event_id, detail)
            db.execute("INSERT INTO buffer_events VALUES (?, ?, ?, ?, ?, ?, ?)", (buffer, *row))
        return dict(zip(EVENT_COLUMNS, row, strict=True))

    def events(self, buffer: str, limit: int = -1) -> list[dict]:
        """The buffer's events, newest last; with `limit`, only the newest that many."""
        with self._lock:
            rows = self._db.execute(
                f"SELECT * FROM (SELECT {', '.join(EVENT_COLUMNS)} FROM buffer_events WHERE buffer = ? "
                "ORDER BY seq DESC LIMIT ?) ORDER BY seq",
                (buffer, limit),
            ).fetchall()
        return [dict(zip(EVENT_COLUMNS, row, strict=True)) for row in rows]

    def latest_event(self, buffer: str) -> dict | None:
        newest = self.events(buffer, limit=1)
        return newest[0] if newest else None

    def buffers(self) -> list[dict]:
        """One summary per buffer, from stored rows only (never a stat): servable excludes volumes flagged missing. A
        buffer with events but no config yet (planned, never sourced) is listed with `config: None`."""
        midnight = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        with self._lock:
            configs = self._db.execute(
                "SELECT id, config FROM buffers UNION "
                "SELECT DISTINCT buffer, NULL FROM buffer_events WHERE buffer NOT IN (SELECT id FROM buffers) ORDER BY 1"
            ).fetchall()
        out = []
        quarantined = self._quarantined_artifacts()
        for buffer, config in configs:
            volumes = self.volumes(buffer)
            live = [volume for volume in volumes if volume.artifact not in quarantined]
            with self._lock:
                (seen,) = self._db.execute(
                    "SELECT MAX(last_seen) FROM consumers WHERE buffer = ?", (buffer,)
                ).fetchone()
                (today,) = self._db.execute(
                    "SELECT COUNT(*) FROM draws WHERE buffer = ? AND ts >= ?", (buffer, midnight)
                ).fetchone()
            splits = sorted({split for volume in volumes for split in volume.split_planes})
            out.append(
                {
                    "id": buffer,
                    "config": json.loads(config) if config is not None else None,
                    "latest_event": self.latest_event(buffer),
                    "volumes": len(volumes),
                    "missing": sum(volume.missing for volume in volumes),
                    "servable": {split: len(servable(live, split)) for split in splits},
                    "consumers_seen": seen,
                    "draws_today": today,
                }
            )
        return out

    def retire(self, buffer: str, archive_dir: Path | None) -> dict:
        """Drop the buffer's references and, archived first to `<archive_dir>/<buffer>.draws.jsonl.gz`, its draws;
        with no `archive_dir` the draws are kept. Volumes and the config stay for the page. The archive is written
        from a read-only connection, outside the service's lock (a large draws table stalls no other request); then
        one short transaction deletes the refs and the archived draws (`seq` up to the max seen at the start). A
        later retire appends to the archive (copied into the new file), never overwrites it; seq restarts at 1
        after a retire, so it is not unique within an archive."""
        archive = archive_dir / f"{buffer}.draws.jsonl.gz" if archive_dir is not None else None
        with self._retiring:
            last = self.max_seq(buffer)
            if archive is not None and last:
                self._archive_draws(buffer, archive, last)
            # ponytail: draws recorded while the archive was written (seq > last) stay in the table; the next
            # retire archives them. A retired buffer answers 410 to next/split, so only a race leaves any.
            with self._transaction() as db:
                drawn = 0
                if archive is not None:
                    # the daily usage outlives the draws: fold the days being deleted into `usage` first
                    db.execute(
                        "INSERT INTO usage SELECT buffer, strftime('%Y-%m-%d', ts, 'unixepoch'), COUNT(*), "
                        "COUNT(DISTINCT consumer), 0 FROM draws WHERE buffer = ? AND seq <= ? GROUP BY 2 "
                        "ON CONFLICT (buffer, day) DO UPDATE SET draws = COALESCE(draws, 0) + excluded.draws, "
                        "consumers = MAX(COALESCE(consumers, 0), excluded.consumers)",
                        (buffer, last),
                    )
                    drawn = db.execute("DELETE FROM draws WHERE buffer = ? AND seq <= ?", (buffer, last)).rowcount
                refs = db.execute("DELETE FROM refs WHERE buffer = ?", (buffer,)).rowcount
        return {"refs": refs, "draws": drawn, "archive": str(archive) if archive and archive.exists() else None}

    # --- daily usage and the key-value rows: what the page charts -------------------------------

    def count_empty(self, buffer: str, day: str) -> None:
        """One more empty answer (a 204) for the buffer on `day` (YYYY-MM-DD, UTC)."""
        with self._lock:
            self._db.execute(
                "INSERT INTO usage VALUES (?, ?, NULL, NULL, 1) ON CONFLICT (buffer, day) DO UPDATE SET empty = empty + 1",
                (buffer, day),
            )

    def usage(self, buffer: str) -> list[dict]:
        """Per UTC day, oldest first: {day, draws, consumers, empty}. A day's draws are the materialised ones (a retire
        folded them in) plus the live rows still in `draws`; consumers the larger of the two counts."""
        # ponytail: consumers across a retire is a max, not a true distinct count; exact needs the archived ids
        with self._lock:
            stored = self._db.execute(
                "SELECT day, draws, consumers, empty FROM usage WHERE buffer = ?", (buffer,)
            ).fetchall()
            live = self._db.execute(
                "SELECT strftime('%Y-%m-%d', ts, 'unixepoch'), COUNT(*), COUNT(DISTINCT consumer) FROM draws "
                "WHERE buffer = ? GROUP BY 1",
                (buffer,),
            ).fetchall()
        days = {
            day: {"day": day, "draws": draws or 0, "consumers": consumers or 0, "empty": empty or 0}
            for day, draws, consumers, empty in stored
        }
        for day, draws, consumers in live:
            row = days.setdefault(day, {"day": day, "draws": 0, "consumers": 0, "empty": 0})
            row["draws"] += draws
            row["consumers"] = max(row["consumers"], consumers)
        return [days[day] for day in sorted(days)]

    def last_hour(self, buffer: str) -> dict:
        """{draws, requests}: the buffer's draws in the last hour and the `next` requests that served them (one `ts`
        per request: record_draws stamps a request's draws together). The empty answers are the service's, in memory.
        """
        # ponytail: a successful `split` request records no draw, so it is not counted; count it in the service like
        # the empty answers if the health line ever needs val/test traffic
        since = time.time() - 3600
        with self._lock:
            (draws,) = self._db.execute(
                "SELECT COUNT(*) FROM draws WHERE buffer = ? AND ts >= ?", (buffer, since)
            ).fetchone()
            (requests,) = self._db.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT consumer, ts FROM draws WHERE buffer = ? AND ts >= ?)",
                (buffer, since),
            ).fetchone()
        return {"draws": draws, "requests": requests}

    def put_kv(self, key: str, value: dict) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (key, json.dumps(value)))

    def kv(self, key: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def _archive_draws(self, buffer: str, archive: Path, last: int) -> None:
        """Append the buffer's draws with `seq <= last` to the gzipped archive, streamed from a read-only
        connection of its own (never the whole table in memory, never under the service's lock)."""
        archive.parent.mkdir(parents=True, exist_ok=True)
        tmp = archive.with_name(archive.name + ".tmp")
        reader = sqlite3.connect(f"{self._path.resolve().as_uri()}?mode=ro", uri=True)
        try:
            with gzip.open(tmp, "wt") as out:
                if archive.exists():
                    with gzip.open(archive, "rt") as earlier:
                        shutil.copyfileobj(earlier, out)
                rows = reader.execute(
                    "SELECT seq, consumer, ts, descriptor FROM draws WHERE buffer = ? AND seq <= ? ORDER BY seq",
                    (buffer, last),
                )
                for seq, consumer, ts, descriptor in rows:
                    line = {"seq": seq, "consumer": consumer, "ts": ts, "descriptor": json.loads(descriptor)}
                    out.write(json.dumps(line) + "\n")
        finally:
            reader.close()
        # ponytail: a failure between this replace and the delete keeps the draws, so a retry appends their lines
        # again; dedupe by whole line (seq restarts at 1 after a retire, so it is not unique)
        os.replace(tmp, archive)

    def backup(self, dest: Path) -> None:
        """Write a consistent copy of the ledger to `dest`, atomically (temp file, then rename)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".tmp")
        target = sqlite3.connect(tmp)
        try:
            with self._lock:
                self._db.backup(target)
        finally:
            target.close()
        os.replace(tmp, dest)

    # --- catalogs and records: the parsed dataset records of a git ref ---------------------------

    def put_records(self, ref: str, records: list[tuple[str, dict]]) -> int:
        """Replace the ref's records (path, record) and note the sync; returns how many."""
        with self._transaction() as db:
            db.execute("DELETE FROM records WHERE ref = ?", (ref,))
            db.executemany(
                "INSERT INTO records VALUES (?, ?, ?, ?)",
                [(ref, record.get("id"), path, json.dumps(record)) for path, record in records],
            )
            db.execute("INSERT OR REPLACE INTO catalogs VALUES (?, ?, ?)", (ref, time.time(), len(records)))
        return len(records)

    def records(self, ref: str) -> list[tuple[str, dict]]:
        with self._lock:
            rows = self._db.execute("SELECT path, data FROM records WHERE ref = ? ORDER BY path", (ref,)).fetchall()
        return [(path, json.loads(data)) for path, data in rows]

    def catalogs(self) -> list[dict]:
        """Every synced ref, newest first: {ref, synced_at, count}."""
        with self._lock:
            rows = self._db.execute("SELECT ref, synced_at, count FROM catalogs ORDER BY synced_at DESC").fetchall()
        return [{"ref": ref, "synced_at": synced_at, "count": count} for ref, synced_at, count in rows]

    def latest_catalog(self) -> str | None:
        catalogs = self.catalogs()
        return catalogs[0]["ref"] if catalogs else None

    # --- datasets: one row per record, the state the prep workflow and the sync keep ---------------

    @staticmethod
    def _merge_dataset(db: sqlite3.Connection, record: str, fields: dict, now: float) -> dict:
        row = db.execute("SELECT data FROM datasets WHERE id = ?", (record,)).fetchone()
        merged = {**(json.loads(row[0]) if row else {}), **fields}
        db.execute("INSERT OR REPLACE INTO datasets VALUES (?, ?, ?)", (record, json.dumps(merged), now))
        return {**merged, "id": record, "updated_at": now}

    def put_dataset(self, record: str, fields: dict) -> dict:
        """Merge `fields` into the record's row (new keys win); returns the merged row."""
        with self._transaction() as db:
            return self._merge_dataset(db, record, fields, time.time())

    def dataset(self, record: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT data, updated_at FROM datasets WHERE id = ?", (record,)).fetchone()
        return {**json.loads(row[0]), "id": record, "updated_at": row[1]} if row else None

    def datasets(self, state: str | None = None) -> list[dict]:
        query = "SELECT id, data, updated_at FROM datasets"
        with self._lock:
            if state is None:
                rows = self._db.execute(query + " ORDER BY id").fetchall()
            else:
                rows = self._db.execute(
                    query + " WHERE json_extract(data, '$.state') = ? ORDER BY id", (state,)
                ).fetchall()
        return [{**json.loads(data), "id": record, "updated_at": updated} for record, data, updated in rows]

    def in_progress_records(self) -> set[str]:
        """Dataset ids whose state is in-progress: a prep job may be reading their raw cache."""
        return {row["id"] for row in self.datasets("in-progress")}

    def sync_datasets(self, prepared: dict[str, dict], *, older_than: float | None = None) -> dict:
        """Mark each given record prepared and drop prepared rows no longer given; other states are untouched,
        and a `revoked` row is never merged over (the folder may still look prepared: a revocation deletes
        nothing). With `older_than`, only rows last written before it are dropped: one written during the scan
        stays."""
        now = time.time()
        kept = 0
        with self._transaction() as db:
            for record, fields in prepared.items():
                state = db.execute("SELECT json_extract(data, '$.state') FROM datasets WHERE id = ?", (record,))
                if (state.fetchone() or (None,))[0] == "revoked":
                    kept += 1
                    continue
                self._merge_dataset(db, record, {**fields, "state": "prepared"}, now)
            rows = db.execute("SELECT id, updated_at FROM datasets WHERE json_extract(data, '$.state') = 'prepared'")
            stale = [
                record
                for record, updated_at in rows
                if record not in prepared and (older_than is None or updated_at < older_than)
            ]
            db.executemany("DELETE FROM datasets WHERE id = ?", [(record,) for record in stale])
        return {"prepared": len(prepared) - kept, "demoted": len(stale), "revoked_kept": kept}

    # --- subjects: a dataset, a buffer spec or a preparation request (spec webapp-actions §4.1) ---

    @staticmethod
    def _subjects(db: sqlite3.Connection, where: str = "", values: tuple = ()) -> list[dict]:
        rows = db.execute(f"SELECT {', '.join(SUBJECT_COLUMNS)} FROM subjects {where} ORDER BY kind, number", values)
        return [{**dict(zip(SUBJECT_COLUMNS, row, strict=True)), "data": json.loads(row[6])} for row in rows]

    def adopt_subject(self, kind: str, number: int, key: str, *, stage: str = "new", data: dict | None = None) -> dict:
        """Create the subject, or refresh its stage and data while no workflow has written a stage (revision 0):
        after that the registry's stage is the truth and an issue's label is not. ValueError: the key is taken."""
        with self._transaction() as db:
            try:
                db.execute(
                    "INSERT INTO subjects VALUES (?, ?, ?, ?, 0, ?, ?, ?) ON CONFLICT (kind, number) DO UPDATE SET "
                    "stage = excluded.stage, data = excluded.data, updated = excluded.updated WHERE revision = 0",
                    (kind, number, key, stage, number, json.dumps(data or {}), time.time()),
                )
            except sqlite3.IntegrityError:
                holder = self._subjects(db, "WHERE kind = ? AND key = ?", (kind, key))[0]["number"]
                raise ValueError(f"{kind} {key!r} is already subject #{holder}") from None
            return self._subjects(db, "WHERE kind = ? AND number = ?", (kind, number))[0]

    def set_subject_stage(self, kind: str, number: int, stage: str) -> dict | None:
        """Write the stage a workflow reached and raise the revision; None when there is no such subject."""
        with self._transaction() as db:
            db.execute(
                "UPDATE subjects SET stage = ?, revision = revision + 1, updated = ? WHERE kind = ? AND number = ?",
                (stage, time.time(), kind, number),
            )
            found = self._subjects(db, "WHERE kind = ? AND number = ?", (kind, number))
        return found[0] if found else None

    def subject(self, kind: str, key: str) -> dict | None:
        with self._lock:
            found = self._subjects(self._db, "WHERE kind = ? AND key = ?", (kind, key))
        return found[0] if found else None

    def find_subjects(
        self, *, kind: str | None = None, number: int | None = None, issue: int | None = None, key: str | None = None
    ) -> list[dict]:
        """The subjects matching every filter given (all of them with none)."""
        given = (("kind", kind), ("number", number), ("issue", issue), ("key", key))
        filters = [(name, value) for name, value in given if value is not None]
        where = "WHERE " + " AND ".join(f"{name} = ?" for name, _ in filters) if filters else ""
        with self._lock:
            return self._subjects(self._db, where, tuple(value for _, value in filters))

    # --- timeline: what happened to a subject, in order; what the page shows and the mirror copies (§4.3) ---

    @staticmethod
    def _entries(db: sqlite3.Connection, where: str, values: tuple) -> list[dict]:
        rows = db.execute(f"SELECT {', '.join(TIMELINE_COLUMNS)} FROM timeline {where} ORDER BY seq", values)
        return [dict(zip(TIMELINE_COLUMNS, row, strict=True)) for row in rows]

    def append_entry(
        self,
        kind: str,
        number: int,
        *,
        body: str = "",
        marker: str = "",
        round_id: str | None = None,
        stage: str | None = None,
    ) -> tuple[dict, dict | None] | None:
        """Write a stage, an entry or both, in one transaction: (subject, entry), or None without such a subject.
        A `stage` always becomes the subject's and raises its revision. A `body` is appended once per marker: a
        second write with the marker (a retried activity, a stage entered twice in a round) returns the first
        entry and appends nothing."""
        key = (kind, number)
        with self._transaction() as db:
            if not self._subjects(db, "WHERE kind = ? AND number = ?", key):
                return None
            if stage is not None:
                db.execute(
                    "UPDATE subjects SET stage = ?, revision = revision + 1, updated = ? WHERE kind = ? AND number = ?",
                    (stage, time.time(), *key),
                )
            entry = None
            if body:
                seq = db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM timeline WHERE kind = ? AND number = ?", key)
                db.execute(
                    "INSERT OR IGNORE INTO timeline (kind, number, seq, at, round, stage, body, marker) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (*key, seq.fetchone()[0], datetime.now(UTC).isoformat(), round_id, stage, body, marker),
                )
                entry = self._entries(db, "WHERE kind = ? AND number = ? AND marker = ?", (*key, marker))[0]
            return self._subjects(db, "WHERE kind = ? AND number = ?", key)[0], entry

    def timeline(self, kind: str, number: int) -> list[dict]:
        with self._lock:
            return self._entries(self._db, "WHERE kind = ? AND number = ?", (kind, number))

    def mark_mirrored(self, kind: str, number: int, seq: int, comment_url: str | None) -> bool:
        """The mirror's write-back for one entry; False when there is no such entry."""
        with self._transaction() as db:
            done = db.execute(
                "UPDATE timeline SET mirrored_at = ?, comment_url = ? WHERE kind = ? AND number = ? AND seq = ?",
                (datetime.now(UTC).isoformat(), comment_url, kind, number, seq),
            )
            return done.rowcount > 0

    def unmirrored_subjects(self) -> list[tuple[str, int]]:
        """The subjects with an entry the mirror has not posted: what the service kicks when it starts."""
        with self._lock:
            rows = self._db.execute(
                "SELECT DISTINCT kind, number FROM timeline WHERE mirrored_at IS NULL ORDER BY kind, number"
            )
            return [(kind, number) for kind, number in rows]

    # --- new subjects and actions: the ids the registry issues (spec webapp-actions §4.1, §4.2) ---

    @staticmethod
    def _issue(db: sqlite3.Connection, name: str, clock: int) -> int:
        """The next id of sequence `name`: the clock's value, or one past the last issued when the clock has not
        moved on. Taken from the clock so a ledger restored from an older backup cannot hand out an id again."""
        row = db.execute("SELECT value FROM kv WHERE key = ?", (f"last_{name}",)).fetchone()
        issued = max(clock, (json.loads(row[0])["last"] if row else 0) + 1)
        db.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (f"last_{name}", json.dumps({"last": issued})))
        return issued

    def create_subject(self, kind: str, key: str, *, data: dict | None = None, number: int | None = None) -> dict:
        """The subject with this key, created when there is none: stage `new`, a number from the clock and no
        issue yet, or the given `number` as both (a dataset whose GitHub issue is known but was never adopted)."""
        with self._transaction() as db:
            found = self._subjects(db, "WHERE kind = ? AND key = ?", (kind, key))
            if found:
                return found[0]
            issue = number
            if number is None:
                number = self._issue(db, "number", int(time.time() - NUMBER_EPOCH))
            db.execute(
                "INSERT INTO subjects VALUES (?, ?, ?, 'new', 0, ?, ?, ?)",
                (kind, number, key, issue, json.dumps(data or {}), time.time()),
            )
            return self._subjects(db, "WHERE kind = ? AND number = ?", (kind, number))[0]

    def _set_subject(self, kind: str, number: int, column: str, value: object) -> dict | None:
        with self._transaction() as db:
            db.execute(
                f"UPDATE subjects SET {column} = ?, updated = ? WHERE kind = ? AND number = ?",
                (value, time.time(), kind, number),
            )
            found = self._subjects(db, "WHERE kind = ? AND number = ?", (kind, number))
        return found[0] if found else None

    def set_subject_data(self, kind: str, number: int, data: dict) -> dict | None:
        """Replace what the subject carries (a buffer spec's text); None when there is no such subject."""
        return self._set_subject(kind, number, "data", json.dumps(data))

    def set_subject_issue(self, kind: str, number: int, issue: int) -> dict | None:
        """The mirror's write-back: the GitHub issue it opened for the subject."""
        return self._set_subject(kind, number, "issue", issue)

    @staticmethod
    def _actions(db: sqlite3.Connection, where: str, values: tuple) -> list[dict]:
        rows = db.execute(f"SELECT {', '.join(ACTION_COLUMNS)} FROM actions {where} ORDER BY id", values)
        return [{**dict(zip(ACTION_COLUMNS, row, strict=True)), "payload": json.loads(row[7])} for row in rows]

    def add_action(
        self, kind: str, number: int, action: str, actor: str, *, key: str | None = None, payload: dict | None = None
    ) -> tuple[dict, bool]:
        """Record one thing somebody asked for, `pending` until Temporal has it: (the action, whether it is
        new). Its id is the workflow's event id. A `key` seen before is that same action and adds nothing
        (ValueError when it was used for another subject or action)."""
        with self._transaction() as db:
            if key is not None and (seen := self._actions(db, "WHERE key = ?", (key,))):
                if (seen[0]["kind"], seen[0]["number"], seen[0]["action"]) != (kind, number, action):
                    raise ValueError(f"the key {key!r} was used for another action")
                return seen[0], False
            now = time.time()
            action_id = self._issue(db, "action", int(now * 1000))
            at = datetime.fromtimestamp(now, UTC).isoformat()
            db.execute(
                "INSERT INTO actions VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', '')",
                (action_id, kind, number, action, actor, at, key, json.dumps(payload or {})),
            )
            return self._actions(db, "WHERE id = ?", (action_id,))[0], True

    def settle_action(
        self, action_id: int, state: str, detail: str = "", *, entry: tuple[str, str] | None = None
    ) -> dict | None:
        """Set an action's state (`sent`, `failed`, `refused`) and why; with `entry` (body, marker) the same
        transaction appends it to the subject's timeline, so a sent action is never missing from it. None when
        there is no such action."""
        with self._transaction() as db:
            found = self._actions(db, "WHERE id = ?", (action_id,))
            if not found:
                return None
            db.execute("UPDATE actions SET state = ?, detail = ? WHERE id = ?", (state, detail, action_id))
            if entry is not None:
                key = (found[0]["kind"], found[0]["number"])
                seq = db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM timeline WHERE kind = ? AND number = ?", key)
                db.execute(
                    "INSERT OR IGNORE INTO timeline (kind, number, seq, at, round, stage, body, marker) "
                    "VALUES (?, ?, ?, ?, NULL, NULL, ?, ?)",
                    (*key, seq.fetchone()[0], datetime.now(UTC).isoformat(), *entry),
                )
            return self._actions(db, "WHERE id = ?", (action_id,))[0]

    def action(self, action_id: int) -> dict | None:
        with self._lock:
            found = self._actions(self._db, "WHERE id = ?", (action_id,))
        return found[0] if found else None

    def actions(self, kind: str, number: int) -> list[dict]:
        with self._lock:
            return self._actions(self._db, "WHERE kind = ? AND number = ?", (kind, number))

    # --- assessments: the append-only record every gate decision leaves (spec autonomous-gates §5) ----

    def put_assessment(self, assessment: dict) -> dict:
        """Store the assessment unless its id is known already (a retry); returns the stored row either way."""
        subject = assessment.get("subject") or {}
        with self._transaction() as db:
            db.execute(
                "INSERT OR IGNORE INTO assessments VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    assessment["id"],
                    assessment.get("gate"),
                    subject.get("kind"),
                    subject.get("ref"),
                    assessment.get("verdict"),
                    assessment.get("by"),
                    assessment.get("at"),
                    json.dumps(assessment),
                ),
            )
            (data,) = db.execute("SELECT data FROM assessments WHERE id = ?", (assessment["id"],)).fetchone()
        return json.loads(data)

    def assessment(self, assessment_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT data FROM assessments WHERE id = ?", (assessment_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def assessments(
        self,
        *,
        subject_kind: str | None = None,
        subject_ref: str | None = None,
        gate: str | None = None,
        by: str | None = None,
        before: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        """The assessments matching every given filter, newest first. `before` is an assessment id (ids sort by
        time): only older rows, the page's cursor for loading further back."""
        with self._lock:
            rows = self._db.execute(
                "SELECT data FROM assessments WHERE (? IS NULL OR subject_kind = ?) AND (? IS NULL OR subject_ref = ?) "
                "AND (? IS NULL OR gate = ?) AND (? IS NULL OR author = ?) AND (? IS NULL OR id < ?) "
                "ORDER BY id DESC LIMIT ?",
                (subject_kind, subject_kind, subject_ref, subject_ref, gate, gate, by, by, before, before, limit),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def agreement(self, gate: str, limit: int = 5000) -> dict:
        """How the judge and the humans agree at a gate (spec autonomous-gates §9): per subject with a judge
        assessment, its newest judge verdict against the newest human verdict; no human verdict yet is `pending`.
        Returns {"judged", "agree", "disagree", "pending", "pairs": [{subject, judge, human, agree}]}."""
        newest_judge: dict[str, dict] = {}
        newest_human: dict[str, dict] = {}
        for row in self.assessments(gate=gate, limit=limit):  # newest first: the first seen per subject wins
            subject = row.get("subject") or {}
            key = f"{subject.get('kind')}:{subject.get('ref')}"
            by = str(row.get("by", ""))
            if by == "judge":
                newest_judge.setdefault(key, row)
            elif by.startswith("human:"):
                newest_human.setdefault(key, row)
        pairs = []
        for key, judge in newest_judge.items():
            human = newest_human.get(key)
            abstained = judge.get("verdict") == "escalate"  # the judge did not decide: neither agreement nor not
            pairs.append(
                {
                    "subject": key,
                    "judge": judge.get("verdict"),
                    "human": human.get("verdict") if human else None,
                    "agree": None if human is None or abstained else judge.get("verdict") == human.get("verdict"),
                    "at": judge.get("at"),
                }
            )
        return {
            "judged": len(pairs),
            "agree": sum(1 for pair in pairs if pair["agree"] is True),
            "disagree": sum(1 for pair in pairs if pair["agree"] is False),
            "abstain": sum(1 for pair in pairs if pair["judge"] == "escalate"),
            "pending": sum(1 for pair in pairs if pair["agree"] is None and pair["judge"] != "escalate"),
            "pairs": pairs[:200],
        }

    # --- budgets: what each root request has reserved and spent (spec autonomous-gates §8) ----------

    @staticmethod
    def _fresh_budget(root: str, now: float) -> dict:
        return {
            "root": root,
            "since": now,
            "reserved_usd": 0.0,
            "spent_usd": 0.0,
            "fetched_bytes": 0,
            "staged_bytes": 0,
            "jobs": 0,
            "running": 0,
            "open": {},
            "closed": [],
            "epochs": 1,
        }

    @staticmethod
    def _over_caps(row: dict, usd: float, caps: dict[str, float], now: float) -> tuple[str, bool]:
        """The first cap the reservation would break, as (reason, wait); ('', False) when none. `wait` is True for
        the concurrency cap alone: that one paces work, the others end it. A cap missing from `caps` is not checked."""
        root = row["root"]
        if "max_days" in caps and (days := (now - row["since"]) / 86400) > caps["max_days"]:
            return f"root {root} is {days:.1f} days old, over max_days {_num(caps['max_days'])}", False
        committed = row["reserved_usd"] + row["spent_usd"]
        if "max_usd" in caps and committed + usd > caps["max_usd"]:
            reason = f"root {root}: usd {committed:.2f} reserved or spent + {usd:.2f} would exceed max_usd "
            return reason + _num(caps["max_usd"]), False
        if "max_fetch_bytes" in caps and row["fetched_bytes"] > caps["max_fetch_bytes"]:
            cap = _num(caps["max_fetch_bytes"])
            return f"root {root}: {row['fetched_bytes']} bytes fetched, over max_fetch_bytes {cap}", False
        if "max_concurrent_jobs" in caps and row["running"] + 1 > caps["max_concurrent_jobs"]:
            cap = _num(caps["max_concurrent_jobs"])
            return f"root {root}: {row['running']} jobs running, max_concurrent_jobs {cap}", True
        if "max_jobs" in caps and row["jobs"] + 1 > caps["max_jobs"]:
            return f"root {root}: {row['jobs']} jobs launched, max_jobs {_num(caps['max_jobs'])}", False
        return "", False

    def reserve(
        self, root: str, reservation_id: str, kind: str, usd: float, caps: dict[str, float], now: float | None = None
    ) -> dict:
        """Charge a job to its root before it launches: {ok, reason, wait, usage}. The check and the add are one
        transaction; a known reservation id (a retry) answers ok without charging again. A root with nothing running
        whose epoch is older than `max_days` starts a fresh one (counters reset): the caps bound an episode of work,
        and a request busy for `max_days` straight is the runaway they stop."""
        now = time.time() if now is None else now
        with self._transaction() as db:
            stored = db.execute("SELECT data FROM budgets WHERE root = ?", (root,)).fetchone()
            row = json.loads(stored[0]) if stored else self._fresh_budget(root, now)
            if reservation_id in row["open"] or reservation_id in row["closed"]:
                return {"ok": True, "reason": "", "wait": False, "usage": row}
            if "max_days" in caps and not row["open"] and (now - row["since"]) / 86400 > caps["max_days"]:
                row = {**self._fresh_budget(root, now), "epochs": int(row.get("epochs", 1)) + 1}
            reason, wait = self._over_caps(row, usd, caps, now)
            if not reason:
                row["open"][reservation_id] = {"kind": kind, "usd": usd, "at": now}
                row["reserved_usd"] += usd
                row["jobs"] += 1
                row["running"] += 1
            db.execute("INSERT OR REPLACE INTO budgets VALUES (?, ?, ?)", (root, json.dumps(row), now))
        return {"ok": not reason, "reason": reason, "wait": wait, "usage": row}

    def settle(
        self,
        root: str,
        reservation_id: str,
        *,
        usd: float | None = None,
        fetched_bytes: int = 0,
        staged_bytes: int = 0,
    ) -> dict:
        """Close an open reservation with what the job cost (`usd` None: unknown, the reservation stands as the
        cost) and moved; returns the budget row. A closed or unknown reservation changes nothing."""
        now = time.time()
        with self._transaction() as db:
            stored = db.execute("SELECT data FROM budgets WHERE root = ?", (root,)).fetchone()
            row = json.loads(stored[0]) if stored else self._fresh_budget(root, now)
            opened = row["open"].pop(reservation_id, None)
            if opened is None:
                return row
            row["reserved_usd"] = max(0.0, row["reserved_usd"] - opened["usd"])
            row["spent_usd"] += usd if usd is not None else opened["usd"]
            row["running"] -= 1
            row["fetched_bytes"] += fetched_bytes
            row["staged_bytes"] += staged_bytes
            row["closed"].append(reservation_id)
            db.execute("INSERT OR REPLACE INTO budgets VALUES (?, ?, ?)", (root, json.dumps(row), now))
        return row

    def budget(self, root: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT data FROM budgets WHERE root = ?", (root,)).fetchone()
        return json.loads(row[0]) if row else None

    def budgets(self) -> list[dict]:
        with self._lock:
            rows = self._db.execute("SELECT data FROM budgets ORDER BY root").fetchall()
        return [json.loads(row[0]) for row in rows]

    # --- unregistering and inspecting volumes (a revocation leaves the buffer; spec autonomous-gates §6) --

    def delete_volume(self, buffer: str, volume_id: str) -> bool:
        """Drop the volume, its frozen rows and the staged reference no other volume of the buffer still needs;
        False when the buffer has no such volume. The raw reference stays (a retire drops it)."""
        with self._transaction() as db:
            found = db.execute(
                "SELECT json_extract(data, '$.artifact'), json_extract(data, '$.path') FROM volumes "
                "WHERE buffer = ? AND id = ?",
                (buffer, volume_id),
            ).fetchone()
            if found is None:
                return False
            artifact, path = found
            db.execute("DELETE FROM volumes WHERE buffer = ? AND id = ?", (buffer, volume_id))
            db.execute(
                "DELETE FROM frozen WHERE buffer = ? AND json_extract(descriptor, '$.volume') = ?", (buffer, volume_id)
            )
            staged = artifact or str(PurePosixPath(path).parent)
            still = {
                known_artifact or str(PurePosixPath(known_path).parent)
                for known_artifact, known_path in db.execute(
                    "SELECT json_extract(data, '$.artifact'), json_extract(data, '$.path') FROM volumes WHERE buffer = ?",
                    (buffer,),
                )
            }
            if staged not in still:
                db.execute("DELETE FROM refs WHERE buffer = ? AND kind = 'staged' AND key = ?", (buffer, staged))
            self._volumes.pop(buffer, None)
        return True

    def volume_rows(self, buffer: str) -> list[dict]:
        """One small row per volume: {id, artifact, record, missing}; the record is the id up to its first '/'."""
        return [
            {
                "id": volume.id,
                "artifact": volume.artifact,
                "record": volume.id.split("/", 1)[0],
                "missing": volume.missing,
            }
            for volume in self.volumes(buffer)
        ]

    def quarantined_volumes(self, buffer: str) -> list[str]:
        """The ids of the buffer's volumes whose staged artifact is quarantined."""
        quarantined = self._quarantined_artifacts()
        return [volume.id for volume in self.volumes(buffer) if volume.artifact in quarantined]

    def live_volumes(self, buffer: str) -> list[Volume]:
        """The buffer's volumes less those whose artifact is quarantined: what the service may draw from and count
        as servable (spec autonomous-gates §6: a revocation stops serving at once, before anything else reacts)."""
        quarantined = self._quarantined_artifacts()
        return [volume for volume in self.volumes(buffer) if volume.artifact not in quarantined]

    def _quarantined_artifacts(self) -> set[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT key FROM assets WHERE kind = 'staged' AND json_extract(data, '$.quarantined') = 1"
            ).fetchall()
        return {row[0] for row in rows}

    # --- the registry ------------------------------------------------------------------------

    @staticmethod
    def _merge_asset(db: sqlite3.Connection, kind: str, key: str, fields: dict, now: float) -> dict:
        row = db.execute("SELECT data FROM assets WHERE kind = ? AND key = ?", (kind, key)).fetchone()
        merged = {**(json.loads(row[0]) if row else {}), **fields}
        db.execute("INSERT OR REPLACE INTO assets VALUES (?, ?, ?, ?)", (kind, key, json.dumps(merged), now))
        return merged

    def put_asset(self, kind: str, key: str, data: dict) -> dict:
        """Merge `data` into the row (new keys win; a new row when none); returns the merged data."""
        with self._transaction() as db:
            return self._merge_asset(db, kind, key, data, time.time())

    def asset(self, kind: str, key: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT data FROM assets WHERE kind = ? AND key = ?", (kind, key)).fetchone()
        return json.loads(row[0]) if row else None

    def add_ref(self, buffer: str, kind: str, key: str) -> None:
        with self._lock:
            self._db.execute("INSERT OR IGNORE INTO refs VALUES (?, ?, ?)", (buffer, kind, key))

    def refs(self, kind: str, key: str) -> list[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT buffer FROM refs WHERE kind = ? AND key = ? ORDER BY buffer", (kind, key)
            ).fetchall()
        return [row[0] for row in rows]

    def assets(self, kind: str, state: str | None = None, record: str | None = None) -> list[dict]:
        """Every key of `kind` that has a row or a reference: {key, data (None without a row), refs}. With `state`
        or `record`, only rows whose data has that value; a reference without a row passes no filter."""
        with self._lock:
            if state is None and record is None:
                rows = self._db.execute(
                    "SELECT key, data FROM assets WHERE kind = ? UNION SELECT DISTINCT key, NULL FROM refs "
                    "WHERE kind = ? AND key NOT IN (SELECT key FROM assets WHERE kind = ?) ORDER BY key",
                    (kind, kind, kind),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT key, data FROM assets WHERE kind = ? AND (? IS NULL OR json_extract(data, '$.state') = ?) "
                    "AND (? IS NULL OR json_extract(data, '$.record') = ?) ORDER BY key",
                    (kind, state, state, record, record),
                ).fetchall()
        return [
            {"key": key, "data": json.loads(data) if data else None, "refs": self.refs(kind, key)} for key, data in rows
        ]

    def asset_bytes(self, kind: str) -> int:
        """Σ `bytes` over the rows of `kind` that are on disk (state neither missing nor evicted)."""
        with self._lock:
            (total,) = self._db.execute(
                "SELECT COALESCE(SUM(json_extract(data, '$.bytes')), 0) FROM assets WHERE kind = ? "
                "AND COALESCE(json_extract(data, '$.state'), '') NOT IN ('missing', 'evicted')",
                (kind,),
            ).fetchone()
        return int(total)

    def staged_last_used(self) -> dict[str, float]:
        """Artifact id → the newest draw from any volume that lives in that artifact."""
        with self._lock:
            rows = self._db.execute(
                "SELECT json_extract(volumes.data, '$.artifact'), MAX(draws.ts) FROM draws JOIN volumes "
                "ON volumes.buffer = draws.buffer AND volumes.id = json_extract(draws.descriptor, '$.volume') "
                "WHERE json_extract(volumes.data, '$.artifact') IS NOT NULL GROUP BY 1"
            ).fetchall()
        return {artifact: ts for artifact, ts in rows}

    def sync_assets(
        self,
        found: dict[tuple[str, str], dict],
        *,
        older_than: float | None = None,
        kinds: tuple[str, ...] = ("raw", "staged"),
    ) -> dict:
        """Merge what a scan found (a new, stateless, missing or evicted row becomes ready; other states are kept),
        mark `missing` every row of `kinds` the scan did not find unless a job is building it or it is evicted (a raw
        row with nothing cached, no bytes and no files, is dropped instead: the scan skips its empty directory), then
        merge each staged row's `last_used` from the draws (a new row, or a known one without it, starts with its
        `scanned_at`; later scans leave it). With `older_than`, a row written since (by a worker,
        during the scan) is neither merged nor marked: the scan may predate that write."""
        last_used = self.staged_last_used()
        now = time.time()
        with self._transaction() as db:
            rows = {
                (kind, key): (json.loads(data), updated)
                for kind, key, data, updated in db.execute("SELECT kind, key, data, updated FROM assets")
            }
            new = 0
            for (kind, key), fields in found.items():
                known = rows.get((kind, key))
                if known is not None and older_than is not None and known[1] >= older_than:
                    continue
                new += known is None
                if "scanned_at" in fields and (known is None or not known[0].get("last_used")):
                    # a new row (or one from before last_used) was last used when first seen, never later
                    fields = {"last_used": fields["scanned_at"], **fields}
                becomes_ready = known is None or known[0].get("state") in (None, "missing", "evicted")
                self._merge_asset(db, kind, key, {**fields, "state": "ready"} if becomes_ready else fields, now)
            missing = dropped = 0
            for (kind, key), (data, updated) in rows.items():
                if kind not in kinds or (kind, key) in found or (older_than is not None and updated >= older_than):
                    continue
                if data.get("state") in ("fetching", "staging", "waiting-drop", "evicted", "missing"):
                    continue
                if kind == "raw" and not data.get("bytes") and not data.get("files"):
                    db.execute("DELETE FROM assets WHERE kind = ? AND key = ?", (kind, key))
                    dropped += 1
                    continue
                self._merge_asset(db, kind, key, {"state": "missing"}, now)
                missing += 1
            for artifact, ts in last_used.items():
                row = db.execute("SELECT data FROM assets WHERE kind = 'staged' AND key = ?", (artifact,)).fetchone()
                if row and ts > (json.loads(row[0]).get("last_used") or 0):
                    self._merge_asset(db, "staged", artifact, {"last_used": ts}, now)
        return {"scanned": len(found), "new": new, "missing": missing, "dropped": dropped}

    def add_consumer(self, buffer: str, info: dict) -> str:
        consumer_id = uuid.uuid4().hex
        with self._lock:
            self._db.execute(
                "INSERT INTO consumers VALUES (?, ?, ?, ?)", (consumer_id, buffer, json.dumps(info), time.time())
            )
        return consumer_id

    def touch(self, buffer: str, consumer_id: str) -> bool:
        with self._lock:
            cursor = self._db.execute(
                "UPDATE consumers SET last_seen = ? WHERE id = ? AND buffer = ?", (time.time(), consumer_id, buffer)
            )
        return cursor.rowcount == 1

    def consumer(self, buffer: str, consumer_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT info FROM consumers WHERE id = ? AND buffer = ?", (consumer_id, buffer)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def max_seq(self, buffer: str) -> int:
        with self._lock:
            (last,) = self._db.execute("SELECT COALESCE(MAX(seq), 0) FROM draws WHERE buffer = ?", (buffer,)).fetchone()
        return int(last)

    def consumers(self, buffer: str, stale_s: float) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT id, info, last_seen FROM consumers WHERE buffer = ? ORDER BY last_seen", (buffer,)
            ).fetchall()
        now = time.time()
        return [
            {**json.loads(info), "consumer_id": consumer_id, "last_seen": seen, "stale": now - seen > stale_s}
            for consumer_id, info, seen in rows
        ]

    def record_draws(self, buffer: str, consumer_id: str, descriptors: list[dict]) -> list[dict]:
        """Assign the next sequence numbers and store the draws, atomically; returns the descriptors with seq."""
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                (last,) = self._db.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM draws WHERE buffer = ?", (buffer,)
                ).fetchone()
                out = [{**descriptor, "seq": last + i + 1} for i, descriptor in enumerate(descriptors)]
                self._db.executemany(
                    "INSERT INTO draws VALUES (?, ?, ?, ?, ?)",
                    [(buffer, d["seq"], consumer_id, json.dumps(d), now) for d in out],
                )
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
        return out

    def frozen(self, buffer: str, split: str) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT descriptor FROM frozen WHERE buffer = ? AND split = ? ORDER BY idx", (buffer, split)
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def extend_frozen(self, buffer: str, split: str, descriptors: list[dict]) -> None:
        with self._lock:
            (start,) = self._db.execute(
                "SELECT COUNT(*) FROM frozen WHERE buffer = ? AND split = ?", (buffer, split)
            ).fetchone()
            self._db.executemany(
                "INSERT INTO frozen VALUES (?, ?, ?, ?)",
                [(buffer, split, start + i, json.dumps(d)) for i, d in enumerate(descriptors)],
            )

    def draws_per_volume(self, buffer: str) -> dict[str, int]:
        with self._lock:
            rows = self._db.execute(
                "SELECT json_extract(descriptor, '$.volume'), COUNT(*) FROM draws WHERE buffer = ? GROUP BY 1 ORDER BY 1",
                (buffer,),
            ).fetchall()
        return {volume: count for volume, count in rows}
