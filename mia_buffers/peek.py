"""A peek at a served buffer: one crop descriptor through the harness's tensorizer, as pictures for the page.

buffer-api's environment reads no array, so the harness's python does it in a process of its own
(`harness tensorize --png`): what comes back is what a consumer's `BufferDataset[i]` would hold, drawn level by
level. Nothing here touches the ledger."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from mia_core.untrusted import read_untrusted
from mia_data.jobspec import harness_env

from .config import Config

HARNESS_MAIN = "import sys; from mia_harness.cli import main; sys.exit(main())"
PEEK_TIMEOUT_S = 60  # a crop takes about a second; this bounds a pyramid on a disk that does not answer
PNG_LIMIT = 8 << 20  # a 256x256 PNG is kilobytes; the cap guards the service's memory
ONE_THREAD = {name: "1" for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")}
INHERITED = ("PATH", "HOME", "TMPDIR", "LANG")  # all of the service's environment the harness is given: no token


class PeekFailed(RuntimeError):
    """The harness did not produce the crop's pictures; the text says why, for the page."""


@dataclass(frozen=True)
class Peeker:
    """Runs the harness for a peek: `python` is the harness environment's, `snippets` the checkout that holds
    `tensorizers/`."""

    python: Path
    snippets: Path

    @classmethod
    def from_config(cls, cfg: Config) -> Peeker:
        # ponytail: the checkout as it stands, not the snapshot a buffer's round pinned (a snapshot older than
        # `--png` cannot draw); pass the pinned snapshot as `snippets` if a tensorizer ever changes what it returns
        return cls(harness_env(cfg), cfg.harness.dir)

    def render(self, pyramid: str, descriptor: dict, tensorizer: str, params: dict, labels: list[str]) -> dict:
        """{"img": [PNG bytes per level], "labels": {key: [PNG bytes per level]}} for the descriptor's crop of the
        staged volume at `pyramid`; a label layer of `labels` that some level lacks is left out."""
        with tempfile.TemporaryDirectory(prefix="mia-peek-") as work:
            pictures = Path(work) / "png"
            command = [str(self.python), "-P", "-c", HARNESS_MAIN, "tensorize", pyramid]
            command += ["--tensorizers", str(self.snippets), "--name", tensorizer]
            command += ["--descriptor", json.dumps(descriptor), "--params", json.dumps(params)]
            command += ["--out", str(Path(work) / "sample.npz"), "--png", str(pictures)]
            try:
                run = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=PEEK_TIMEOUT_S,
                    cwd=work,
                    env={**{name: os.environ[name] for name in INHERITED if name in os.environ}, **ONE_THREAD},
                )
            except subprocess.TimeoutExpired:
                raise PeekFailed(f"the harness did not answer in {PEEK_TIMEOUT_S} s") from None
            except OSError as error:
                raise PeekFailed(f"cannot run the harness: {error}") from None
            if run.returncode != 0:
                raise PeekFailed(f"harness tensorize exited {run.returncode}: {run.stderr.strip()[-300:]}")
            levels = range(len(descriptor["levels"]))
            # the staged volume is a job's output, and so is what was drawn from it
            image = [read_untrusted(pictures, f"img-{level}.png", PNG_LIMIT, binary=True) for level in levels]
            if not all(image):
                raise PeekFailed("the harness wrote no picture of the crop: is it older than `tensorize --png`?")
            found = {
                key: [read_untrusted(pictures, f"label-{key}-{level}.png", PNG_LIMIT, binary=True) for level in levels]
                for key in labels
            }
            return {"img": image, "labels": {key: layer for key, layer in found.items() if all(layer)}}
