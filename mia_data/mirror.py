"""The mirror's activity: copy a subject's timeline and stage to its GitHub issue (spec webapp-actions §7).

The registry holds what happened; the issue is a read-only log of it. Each timeline entry becomes one comment,
in order, once: the entry's marker is in the comment, so a pass that died between posting and writing back finds
its comment again. The stage label follows the subject's stage. Nothing here is read back as input.
The workflow that runs this (`MiaMirror`) is in `workflows.py`; each worker registers the activity for its kinds."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping

from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.github import GitHub
from mia_core.registry import Registry


class Mirror:
    """`labels` maps a subject kind to (stage -> label name, the stages that have a label); `bot` is the login
    whose comments count as ours."""

    def __init__(
        self, gh: GitHub, registry: Registry, bot: str, labels: Mapping[str, tuple[Callable[[str], str], Iterable[str]]]
    ) -> None:
        self.gh, self.registry, self.bot, self.labels = gh, registry, bot, labels

    @activity.defn(name="mirror")
    async def mirror(self, kind: str, number: int) -> int:
        """Post the subject's unmirrored entries and set its stage label; the number of comments posted."""
        found = await asyncio.to_thread(self.registry.timeline, kind, number)
        if found is None or kind not in self.labels:
            raise ApplicationError(f"no {kind} #{number} to mirror here", non_retryable=True)
        subject = found["subject"]
        issue = subject["issue"]
        if issue is None:
            raise ApplicationError(f"{kind} #{number} has no issue to mirror to", non_retryable=True)
        posted = 0
        pending = [entry for entry in found["entries"] if entry["mirrored_at"] is None]
        if pending:
            comments = await self.gh.issue_comments(issue)
            ours = [comment for comment in comments if (comment.get("user") or {}).get("login") == self.bot]
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
