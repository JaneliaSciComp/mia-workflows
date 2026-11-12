"""The mirror's activity: copy a subject's timeline and stage to its GitHub issue (spec webapp-actions §7).

The registry holds what happened; the issue is a read-only log of it. A subject started from the page has no
issue: the mirror opens one, with a first line that names the subject so an issue opened just before a crash is
found again. Each timeline entry becomes one comment, in order, once: the entry's marker is in the comment, so a
pass that died between posting and writing back finds its comment again. The stage label follows the subject's
stage. Nothing here is read back as input.
The workflow that runs this (`MiaMirror`) is in `workflows.py`; each worker registers the activity for its kinds."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping
from urllib.parse import quote

from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.github import GitHub
from mia_core.registry import Registry

from . import catalog
from .config import Config
from .seed import render_issue

# what an issue of a subject says: (title, body, labels); the body's first line is the marker naming the subject
Describe = Callable[[dict], Awaitable[tuple[str, str, list[str]]]]


def read_only_line(page_url: str) -> str:
    """The line under an issue's marker: where to act, since nothing reads the issue."""
    return (
        f"This issue is a read-only log, written by MIA. Labels, comments and closing it change nothing: act on "
        f"[its page]({page_url})."
    )


READ_ONLY_MARKER = "<!-- mia: read-only log -->"  # on the one comment that tells an adopted issue what it now is


def dataset_page(cfg: Config) -> Callable[[dict], str]:
    """A dataset subject -> the URL of its page."""
    base = cfg.service.url.rstrip("/") if cfg.service else ""
    return lambda subject: f"{base}/#datasets/{quote(subject['key'], safe='')}"


def dataset_issue(cfg: Config, gh_catalog: GitHub) -> Describe:
    """How a dataset's issue reads: the record's facts as `mia-data seed` wrote them, under the record marker and
    the line that sends people to the dataset's page."""

    async def describe(subject: dict) -> tuple[str, str, list[str]]:
        record_id = subject["key"]
        path = (await catalog.record_paths(gh_catalog)).get(record_id)
        if path is None:
            raise ApplicationError(f"{record_id}: no record on the catalog's main", non_retryable=True)
        entry = (await catalog.downloaded(gh_catalog)).get(record_id)
        if entry is None:  # nobody downloaded it: jobs fetch on demand
            entry = {"record_commit": await gh_catalog.get_ref_sha("main"), "downloaders": []}
        record = await catalog.record(gh_catalog, path, entry["record_commit"])
        title, body, labels = render_issue(record, entry, gh_catalog.repo, path, record_id)
        marker, _, facts = body.partition("\n")
        return title, f"{marker}\n{read_only_line(dataset_page(cfg)(subject))}\n{facts}", labels

    return describe


class Mirror:
    """`labels` maps a subject kind to (stage -> label name, the stages that have a label); `openers` maps a kind
    to what its issue says when the mirror has to open one; `pages` maps a kind to the URL of a subject's page,
    for the one comment that tells an issue opened before the page acted that it is a log now; `bot` is the
    login whose issues and comments count as ours."""

    def __init__(
        self,
        gh: GitHub,
        registry: Registry,
        bot: str,
        labels: Mapping[str, tuple[Callable[[str], str], Iterable[str]]],
        openers: Mapping[str, Describe] | None = None,
        pages: Mapping[str, Callable[[dict], str]] | None = None,
    ) -> None:
        self.gh, self.registry, self.bot, self.labels = gh, registry, bot, labels
        self.openers, self.pages = openers or {}, pages or {}

    async def _open_issue(self, kind: str, number: int, subject: dict) -> int:
        """The subject's issue: ours that already carries its marker (opened before a crash), else a new one;
        its number is written back to the registry."""
        if kind not in self.openers:
            raise ApplicationError(f"{kind} #{number} has no issue and this worker opens none", non_retryable=True)
        title, body, labels = await self.openers[kind](subject)
        marker = body.partition("\n")[0]
        # ponytail: lists the repo's issues once per issue opened; a search by marker when there are thousands
        ours = [
            issue
            for issue in await self.gh.list_issues(state="all")
            if (issue.get("user") or {}).get("login") == self.bot and (issue.get("body") or "").startswith(marker)
        ]
        issue = ours[0] if ours else await self.gh.create_issue(title, body, labels)
        await asyncio.to_thread(self.registry.set_subject_issue, kind, number, issue["number"])
        return issue["number"]

    @activity.defn(name="mirror")
    async def mirror(self, kind: str, number: int) -> int:
        """Post the subject's unmirrored entries and set its stage label; the number of comments posted."""
        found = await asyncio.to_thread(self.registry.timeline, kind, number)
        if found is None or kind not in self.labels:
            raise ApplicationError(f"no {kind} #{number} to mirror here", non_retryable=True)
        subject = found["subject"]
        issue = subject["issue"]
        if issue is None:
            issue = await self._open_issue(kind, number, subject)
        posted = 0
        pending = [entry for entry in found["entries"] if entry["mirrored_at"] is None]
        if pending:
            comments = await self.gh.issue_comments(issue)
            ours = [comment for comment in comments if (comment.get("user") or {}).get("login") == self.bot]
            # a subject numbered by its issue was adopted from GitHub; one the mirror opened says so in its body
            adopted = issue == number
            if (
                adopted
                and kind in self.pages
                and not any(READ_ONLY_MARKER in (mine.get("body") or "") for mine in ours)
            ):
                # once, before the first thing the mirror writes there: labels do nothing now, act on the page
                await self.gh.comment(issue, f"{read_only_line(self.pages[kind](subject))}\n\n{READ_ONLY_MARKER}")
            for entry in pending:
                marker = entry["marker"]
                comment = next((mine for mine in ours if marker in (mine.get("body") or "")), None)
                if comment is None:
                    body = entry["body"] if marker in entry["body"] else f"{entry['body']}\n\n{marker}"
                    comment = await self.gh.comment(issue, body)
                    posted += 1
                await asyncio.to_thread(
                    self.registry.mark_mirrored, kind, number, entry["seq"], comment.get("html_url")
                )
        label_of, stages = self.labels[kind]
        if subject["stage"] in stages:  # a stage without a label (a request not yet started) leaves the labels
            want = label_of(subject["stage"])
            current = {label["name"] for label in (await self.gh.get_issue(issue)).get("labels", [])}
            for stage in stages:
                if (name := label_of(stage)) in current and name != want:
                    await self.gh.remove_label(issue, name)
            if want not in current:
                await self.gh.add_labels(issue, [want])
        return posted
