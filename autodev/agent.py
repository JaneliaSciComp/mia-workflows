"""One agent round: the prompt Claude Code gets, the script that runs it in the sandbox, and how its
output is read back.

The workspace the job sees holds no credentials. The agent commits locally; the script bundles the
commits and the worker pushes them (see gitops.py).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mia_core.hosts import HOST_RE, is_ip_literal, missing_hosts
from mia_core.jobrecord import STREAM_FLAGS, TRANSCRIPT_FILE
from mia_core.judge.evidence import EVIDENCE_DIR, QUESTION_FILE
from mia_core.judge.evidence import command as judge_command

from . import jobs
from .config import RAW_GITHUB, is_github_host
from .jobs import read_untrusted
from .models import JobOutcome, JobRef, Workspace, check_key

if TYPE_CHECKING:
    from .config import Config

PROMPT = """You are implementing GitHub issue #{number} in {repo}. The repository is checked out in the current
directory on branch `{branch}` at commit {base_sha}.

# Issue: {title}

{body}
{discussion}{revision}{evidence}
# How to work

- Read CLAUDE.md and README.md first, if present, and follow the repository's conventions.
- Make the change with tests. Run the test suite (`pixi run test` if the repo uses pixi) and make it pass.
- Commit your work with `git commit` (clear messages; several commits are fine). Do not push: you have no
  GitHub access, and your commits are collected when you exit.
- Do not modify anything under: {protected}.
- Finish with a short summary of what you changed and how you tested it. It is posted on the pull request.
- If the issue is unclear or cannot be done, make no commits and explain why in your summary.
- If the issue or the requested changes carry a line `Request: <id>` with findings about sample crops, and you
  find the crops right as they are, make no commits and write `{no_change}` in the current directory (do not
  commit it): `{{"request": "<id>", "findings": [{{"item": "<the finding's item>", "answer": "<what you looked at
  and why it is not a defect>"}}]}}`, one entry for every finding. Do not write code to satisfy a doubt.
"""


def render_prompt(
    cfg: Config, issue: dict, comments: list[dict], branch: str, base_sha: str, pr: int | None, feedback: str | None
) -> str:
    """Fill the prompt template with the issue, the human discussion on it, and any reviewer feedback.

    `issue` and `comments` are GitHub API dicts; the bot's own comments (carrying our marker) are left out."""
    human = [comment for comment in comments if cfg.marker_prefix not in (comment.get("body") or "")]
    discussion = (
        "\n# Discussion on the issue\n\n"
        + "\n\n".join(f"**{(comment.get('user') or {}).get('login')}**: {comment.get('body')}" for comment in human)
        + "\n"
        if human
        else ""
    )
    revision = (
        (
            f"\n# Requested changes\n\nPull request #{pr} has your earlier work on this branch. "
            f"A reviewer asked for:\n\n{feedback}\n"
        )
        if feedback
        else ""
    )
    return PROMPT.format(
        number=issue["number"],
        repo=cfg.github.repo,
        branch=branch,
        base_sha=base_sha,
        title=issue.get("title") or "",
        body=issue.get("body") or "(no description)",
        discussion=discussion,
        revision=revision,
        evidence=(
            f"\n# Evidence\n\nWhat this issue is about can be looked at, read-only, under `{folder}`.\n"
            if (folder := evidence_folder(cfg, issue["number"])) is not None
            else ""
        ),
        protected=", ".join(cfg.github.protected_paths) or "(nothing)",
        no_change=NO_CHANGE_FILE,
    )


def evidence_folder(cfg: Config, issue: int) -> Path | None:
    """The folder of evidence left for this issue under `agent.evidence_dir`, when there is one: found by the
    issue's number, never by a path an issue or a comment names. A link is not a folder of evidence."""
    if cfg.agent.evidence_dir is None:
        return None
    folder = cfg.agent.evidence_dir / f"issue-{int(issue)}"
    return folder if folder.is_dir() and not folder.is_symlink() else None


def job_script(cfg: Config, base_sha: str) -> str:
    """The shell script the csub job runs, with cwd = the round directory.

    It runs Claude Code on the prompt (its whole conversation goes to claude.jsonl for the job's record, the
    result object to result.json), bundles whatever was committed since `base_sha`, and records the exit code
    for the worker to read."""
    quote = shlex.quote
    path = (
        f'export PATH={":".join(quote(entry) for entry in cfg.agent.path_prepend)}:"$PATH"\n'
        if cfg.agent.path_prepend
        else ""
    )
    budget = f" --max-budget-usd {cfg.agent.max_budget_usd}" if cfg.agent.max_budget_usd is not None else ""
    return f"""set -u
{path}cd repo || exit 1
claude -p "$(cat ../prompt.md)" --model {quote(cfg.agent.model)} {STREAM_FLAGS}{budget} \\
  --allowedTools "Bash,Read,Edit,Write,Glob,Grep" > ../{TRANSCRIPT_FILE} 2> ../claude.err
rc=$?
tail -n 1 ../{TRANSCRIPT_FILE} > ../result.json
git bundle create -q ../result.bundle {quote(base_sha)}..HEAD 2>> ../claude.err
echo "$rc" > ../exit_code
exit "$rc"
"""


def judge_dir(round_dir: Path) -> Path:
    """The review judge's workspace: a sibling of the round dir (`<key>/judge-r<n>`), outside the author job's
    writable mount, so the author can plant nothing there (no CLAUDE.md, no settings, no exit_code)."""
    round_dir = Path(round_dir)
    return round_dir.parent / round_dir.name.replace("round-", "judge-r", 1)


def judge_script(cfg: Config) -> str:
    """The review judge's job script, cwd = the judge workspace: Claude Code with read-only tools over
    `evidence/` (spec autonomous-gates §4), the exit code recorded for the worker."""
    quote = shlex.quote
    path = (
        f'export PATH={":".join(quote(entry) for entry in cfg.agent.path_prepend)}:"$PATH"\n'
        if cfg.agent.path_prepend
        else ""
    )
    return f"""set -u
{path}{judge_command(cfg.judge.model, cfg.judge.max_budget_usd)}
rc=$?
echo "$rc" > exit_code
exit "$rc"
"""


def read_result(round_dir: Path, limit: int = 20000) -> tuple[str, float | None]:
    """The agent's summary and cost from result.json. Untrusted: any shape may be there."""
    try:
        data = json.loads(read_untrusted(round_dir, "result.json", 1 << 20) or "")
        text, cost = str(data.get("result") or ""), data.get("total_cost_usd")
    except (ValueError, AttributeError, RecursionError):
        return "(the agent left no readable result.json)", None
    if len(text) > limit:
        text = text[:limit] + "\n\n…(truncated)"
    return text or "(empty summary)", cost if isinstance(cost, (int, float)) else None


NO_CHANGE_FILE = "no_change.json"  # the agent's "nothing needs changing", finding by finding, in its checkout
NO_CHANGE_BYTES = 256 * 1024
REQUEST_RE = re.compile(r"^[ \t]*(?:/revise[ \t]+)?Request: (\S+)[ \t]*$", re.M)
FINDING_RE = re.compile(r"^- (.+?) \[(?:suspect|bad)\]:", re.M)


def read_no_change(round_dir: Path, request_text: str) -> tuple[str, dict[str, str]]:
    """The agent's validated "the crops are right as they are" (spec judge-decides §4.2): (the request's id, its
    answer to each finding), or ("", {}) for anything else. It counts only as an explicit answer to this
    request: `request_text` (the issue's body or the reviewer's feedback, as the agent was given it) names a
    request and lists findings; the agent's `no_change.json` names that request and answers exactly those
    findings, each with text; and the agent's run ended by itself (its result object is not an error). A round
    with no commits and no such file says nothing about the crops. Untrusted, like everything a job wrote."""
    asked, items = REQUEST_RE.search(request_text or ""), FINDING_RE.findall(request_text or "")
    if not asked or not items or len(set(items)) != len(items):
        return "", {}
    try:
        result = json.loads(read_untrusted(Path(round_dir), "result.json", 1 << 20) or "")
        data = json.loads(read_untrusted(Path(round_dir) / "repo", NO_CHANGE_FILE, NO_CHANGE_BYTES) or "")
    except (ValueError, RecursionError):
        return "", {}
    if not isinstance(result, dict) or result.get("is_error") is True or not isinstance(data, dict):
        return "", {}
    findings = data.get("findings")
    if data.get("request") != asked.group(1) or not isinstance(findings, list) or len(findings) != len(items):
        return "", {}
    answers: dict[str, str] = {}
    for finding in findings:
        item, answer = (finding.get("item"), finding.get("answer")) if isinstance(finding, dict) else (None, None)
        if not isinstance(item, str) or not isinstance(answer, str) or not answer.strip():
            return "", {}
        answers[item.strip()] = answer.strip()[:2000]
    return (asked.group(1), answers) if set(answers) == {item.strip() for item in items} else ("", {})


CUT_OFF_RE = re.compile(r"Not logged in|/login|OAuth|Invalid API key|API Error", re.IGNORECASE)


def cut_off(round_dir: Path) -> str:
    """Claude Code's own error when it, not the agent, ended the session: the error result in result.json that
    names the login or the API, on one line; "" otherwise. Untrusted, like everything a job wrote."""
    try:
        data = json.loads(read_untrusted(Path(round_dir), "result.json", 1 << 20) or "")
    except (ValueError, RecursionError):
        return ""
    said = data.get("result") if isinstance(data, dict) and data.get("is_error") is True else None
    return " ".join(said.split())[:200] if isinstance(said, str) and CUT_OFF_RE.search(said) else ""


def log_tail(round_dir: Path, lines: int = 40) -> str:
    """The last `lines` of claude.err and of the newest few csub job stderr files, for failure reports."""
    round_dir = Path(round_dir)
    parts = []
    job_logs = sorted((round_dir / ".csub" / "jobs").glob("*/stderr"))[-3:]  # newest few: bounds memory
    for rel in ["claude.err", *(str(path.relative_to(round_dir)) for path in job_logs)]:
        if text := (read_untrusted(round_dir, rel, 1 << 16, tail=True) or "").strip():
            parts.append(f"== {rel} ==\n" + "\n".join(text.splitlines()[-lines:]))
    return "\n".join(parts)[-5000:]


def csub_client(cfg: Config) -> Any:
    return jobs.csub_client(cfg.deployment)


MAX_JOB_HOSTS = 64  # csub refuses more hosts than this on one job
HOSTS_RE = re.compile(r"<!-- autodev: hosts([^>]*?)-->")


class TooManyHostsError(ValueError):
    """An agent's job needs more hosts than csub takes on one job."""


def hosts_marker(hosts: Sequence[str]) -> str:
    """The marker with which an issue names the hosts its agent's job may reach beyond the configured ones."""
    return f"<!-- autodev: hosts {' '.join(hosts)} -->"


def issue_hosts(body: str | None) -> list[str]:
    """The hosts an issue's body names in its first `hosts_marker`; none without one."""
    found = HOSTS_RE.search(body or "")
    return found.group(1).split() if found else []


def agent_hosts(configured: list[str], named: Sequence[str], policy: list[str]) -> list[str]:
    """What an agent's job may reach: the configured hosts, then those its issue names that the broker's policy
    allows (`policy`: its allowed_hosts patterns). The policy is the ceiling, never the list: a host no issue
    names is not granted. Never a name csub refuses as a job host, an IP address or a GitHub host other than
    `raw.githubusercontent.com` (which a dataset round names once a person approved it for that dataset); without
    repeats. More than MAX_JOB_HOSTS is a TooManyHostsError: none is dropped to fit."""
    usable = [
        host
        for host in named
        if HOST_RE.fullmatch(host)
        and not is_ip_literal(host)
        and (not is_github_host(host) or host == RAW_GITHUB)
        and not missing_hosts([host], policy)
    ]
    hosts = list(dict.fromkeys([*configured, *usable]))
    if len(hosts) > MAX_JOB_HOSTS:
        raise TooManyHostsError(
            f"the agent's job needs {len(hosts)} hosts ({len(configured)} configured, the rest named by its issue) "
            f"and one job takes at most {MAX_JOB_HOSTS}"
        )
    return hosts


class AgentJobs:
    """Submits and waits on agent jobs for one deployment, keyed by (issue, key, round)."""

    def __init__(
        self,
        cfg: Config,
        client_factory: Callable[[], Any] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
        poll_s: float = 60.0,
    ):
        self.cfg = cfg
        self.jobs = jobs.CsubJobs(
            cfg.deployment,
            cfg.paths.state_dir / "jobs",
            client_factory or (lambda: csub_client(cfg)),
            sleep,
            poll_s,
            work_root=cfg.paths.work_root,
        )

    async def submit(
        self, issue: int, key: str, round_number: int, workspace: Workspace, named_hosts: Sequence[str] = ()
    ) -> JobRef:
        """Submit once per (issue, key, round); a retry returns the same job. `named_hosts`: the hosts the issue
        names for its job (`issue_hosts`), granted where the broker's policy allows them."""
        agent_cfg = self.cfg.agent
        job_key = f"issue-{issue}-{check_key(key)}-r{round_number}"

        # built lazily: the broker is probed and the API key read only when no id file exists
        async def build_spec() -> jobs.JobSpec:
            policy_hosts = (await asyncio.to_thread(self.jobs.client.probe)).allowed_hosts
            return jobs.JobSpec(
                name=f"autodev-{issue}-r{round_number}",  # csub rejects names containing the username; session = deployment
                command=job_script(self.cfg, workspace.base_sha),
                workspace=workspace.dir,
                walltime_min=agent_cfg.walltime_min,
                cpus=agent_cfg.cpus,
                mem_mb=agent_cfg.mem_mb,
                allow_hosts=agent_hosts(agent_cfg.allow_hosts, named_hosts, policy_hosts),
                agent="claude",
                ro_mounts=[
                    *(mount.removesuffix(":ro") for mount in agent_cfg.extra_mounts),  # Config guarantees ":ro"
                    *([str(evidence)] if (evidence := evidence_folder(self.cfg, issue)) is not None else []),
                ],
                env={"ANTHROPIC_API_KEY": os.environ[agent_cfg.api_key_env]} if agent_cfg.api_key_env else {},
                prompt_file="prompt.md",
                transcript_file=TRANSCRIPT_FILE,
            )

        return JobRef(await self.jobs.submit(job_key, build_spec), agent_cfg.walltime_min)

    async def submit_judge(self, issue: int, key: str, round_number: int, judge_dir: str) -> JobRef:
        """Submit the review judge once per round: a read-only Claude Code job in `judge_dir`, no hosts of its own,
        its walltime and budget from `judge`."""
        agent_cfg, judge_cfg = self.cfg.agent, self.cfg.judge
        job_key = f"issue-{issue}-{check_key(key)}-r{round_number}-judge"

        def build_spec() -> jobs.JobSpec:
            return jobs.JobSpec(
                name=f"autodev-{issue}-r{round_number}-judge",
                command=judge_script(self.cfg),
                workspace=judge_dir,
                walltime_min=judge_cfg.walltime_min,
                cpus=1,
                mem_mb=agent_cfg.mem_mb,
                allow_hosts=[],
                agent="claude",
                ro_mounts=[],
                env={"ANTHROPIC_API_KEY": os.environ[agent_cfg.api_key_env]} if agent_cfg.api_key_env else {},
                prompt_file=f"{EVIDENCE_DIR}/{QUESTION_FILE}",
                transcript_file=TRANSCRIPT_FILE,
            )

        return JobRef(await self.jobs.submit(job_key, build_spec), judge_cfg.walltime_min)

    async def wait(self, job_id: str, workspace: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome:
        return await self.jobs.wait(job_id, workspace.dir, heartbeat)
