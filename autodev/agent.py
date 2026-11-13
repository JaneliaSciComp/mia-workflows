"""One agent round: the prompt Claude Code gets, the script that runs it in the sandbox, and how its
output is read back.

The workspace the job sees holds no credentials. The agent commits locally; the script bundles the
commits and the worker pushes them (see gitops.py).
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mia_core.judge.evidence import command as judge_command

from . import jobs
from .config import is_github_host
from .jobs import read_untrusted
from .models import JobOutcome, JobRef, Workspace, check_key

if TYPE_CHECKING:
    from .config import Config

PROMPT = """You are implementing GitHub issue #{number} in {repo}. The repository is checked out in the current
directory on branch `{branch}` at commit {base_sha}.

# Issue: {title}

{body}
{discussion}{revision}
# How to work

- Read CLAUDE.md and README.md first, if present, and follow the repository's conventions.
- Make the change with tests. Run the test suite (`pixi run test` if the repo uses pixi) and make it pass.
- Commit your work with `git commit` (clear messages; several commits are fine). Do not push: you have no
  GitHub access, and your commits are collected when you exit.
- Do not modify anything under: {protected}.
- Finish with a short summary of what you changed and how you tested it. It is posted on the pull request.
- If the issue is unclear or cannot be done, make no commits and explain why in your summary.
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
        protected=", ".join(cfg.github.protected_paths) or "(nothing)",
    )


def job_script(cfg: Config, base_sha: str) -> str:
    """The shell script the csub job runs, with cwd = the round directory.

    It runs Claude Code on the prompt, bundles whatever was committed since `base_sha`, and records the
    exit code for the worker to read."""
    quote = shlex.quote
    path = (
        f'export PATH={":".join(quote(entry) for entry in cfg.agent.path_prepend)}:"$PATH"\n'
        if cfg.agent.path_prepend
        else ""
    )
    budget = f" --max-budget-usd {cfg.agent.max_budget_usd}" if cfg.agent.max_budget_usd is not None else ""
    return f"""set -u
{path}cd repo || exit 1
claude -p "$(cat ../prompt.md)" --model {quote(cfg.agent.model)} --output-format json{budget} \\
  --allowedTools "Bash,Read,Edit,Write,Glob,Grep" > ../result.json 2> ../claude.err
rc=$?
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


# ponytail: csub refuses more than 64 hosts on a job, so the oldest policy hosts drop first; upgrade is a per-issue
# host list or a higher limit in csub.
MAX_JOB_HOSTS = 64


def agent_hosts(configured: list[str], policy_hosts: list[str]) -> list[str]:
    """The configured hosts, then the exact hosts the broker's policy allows (never patterns or GitHub hosts),
    without repeats; past MAX_JOB_HOSTS the oldest policy hosts are dropped and configured hosts stay."""
    usable = [host for host in policy_hosts if "*" not in host and not is_github_host(host)]
    hosts = list(dict.fromkeys(configured))
    extra = [host for host in dict.fromkeys(usable) if host not in hosts]
    return hosts + extra[max(0, len(hosts) + len(extra) - MAX_JOB_HOSTS) :]


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
            cfg.deployment, cfg.paths.state_dir / "jobs", client_factory or (lambda: csub_client(cfg)), sleep, poll_s
        )

    async def submit(self, issue: int, key: str, round_number: int, workspace: Workspace) -> JobRef:
        """Submit once per (issue, key, round); a retry returns the same job."""
        agent_cfg = self.cfg.agent
        job_key = f"issue-{issue}-{check_key(key)}-r{round_number}"

        async def build_spec() -> (
            jobs.JobSpec
        ):  # built lazily: the broker is probed and the API key read only when no id file exists
            policy_hosts = (await asyncio.to_thread(self.jobs.client.probe)).allowed_hosts
            return jobs.JobSpec(
                name=f"autodev-{issue}-r{round_number}",  # csub rejects names containing the username; session = deployment
                command=job_script(self.cfg, workspace.base_sha),
                workspace=workspace.dir,
                walltime_min=agent_cfg.walltime_min,
                cpus=agent_cfg.cpus,
                mem_mb=agent_cfg.mem_mb,
                allow_hosts=agent_hosts(agent_cfg.allow_hosts, policy_hosts),
                agent="claude",
                ro_mounts=[mount.removesuffix(":ro") for mount in agent_cfg.extra_mounts],  # Config guarantees ":ro"
                env={"ANTHROPIC_API_KEY": os.environ[agent_cfg.api_key_env]} if agent_cfg.api_key_env else {},
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
            )

        return JobRef(await self.jobs.submit(job_key, build_spec), judge_cfg.walltime_min)

    async def wait(self, job_id: str, workspace: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome:
        return await self.jobs.wait(job_id, workspace.dir, heartbeat)
