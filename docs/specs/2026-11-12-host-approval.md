# MIA: approving data hosts from the page — Spec

**Status:** v0.4 · 2026-11-12 · approved by the user 2026-11-12 (as v0.3). v0.4 corrects §3 after a review: two
generations at once are not harmless, and how they are made safe without a lock file; a `policy_dir` that
holds the base policy is refused; the placement rules cover every worker-writable mount and are checked by
every generation, not only by `mia up`.
v0.3 names the page's entry points as they stand at 4a37504 (§6). v0.2 took the user's rulings
on autodev and on csub (§2.1) and a reviewer's pass against the code: where the policy directory may live (§3),
where a buffer's pool comes from (§4), which broker `mia exec` talks to (§3), and what the installed csub does
and does not bound (§9).
Decided by the user: the page asks when a person starts a preparation or a buffer run; it lists every host the
run needs that the csub policy does not allow, with a "select all"; an approval takes effect without editing
`~/.config/csub/broker.toml`, because mia's brokers are started with their own `--policy`; autodev's agents are
allowed the approved hosts; every agent runs in the same csub with the same config.
Calls marked *my call* were made without the user and are listed again in §11.
Written from the code on `webapp-actions` (4a37504), which this builds on
([the page acts](2026-11-11-webapp-actions.md) §5 for action routes, §10 for the actor).
**Repos:** this repo (`mia_core/`, `mia_ops/`, `mia_buffers/`, `autodev/`). csub is not changed:
`csub-broker --policy FILE` exists and the broker hands the same path to every per-job broker it starts on a
compute node.

## 0. Summary

A dataset whose files are on a host the csub policy does not list stops one second into its round:
`HostsNotAllowed: hosts not allowed by the csub policy: data.4tu.nl`. The fix today is a hand edit of the
operator's `broker.toml`, found out about only after the failure. The policy lists 16 hosts; the catalog's records
name well over a hundred others.

After this spec:

- **mia has its own policy file.** `mia up` starts every broker with `--policy` pointing at a file mia generates:
  the operator's hand-written policy plus the hosts approved on the page. The hand-written file is never written.
- **The page asks before a run starts.** Pressing Start on a dataset, Prepare on a list of datasets, Run again on
  a preparation request or Run on a buffer's plan first asks the service which hosts the run needs and the policy
  lacks. If there are any, a dialog lists them; the person ticks hosts (or "select all") and approves.
- **An approval is part of the start.** The ticked hosts travel with the action that starts the run. The service
  approves them, regenerates the policy, writes a timeline entry, and then starts the run. The broker re-reads
  its policy on every request, so the first job of that run already sees the host.
- **Only hosts the run names.** The service refuses to approve a host that the run's own catalog records (or
  published bindings) do not name.
- **One policy for every agent.** All services' brokers read the generated policy, and an autodev agent job asks
  for the hosts it allows.
- **The workflows do not change.** Their refusals stay as the backstop: a dataset round on an unapproved host
  still stops at `needs-human`, and a buffer still treats such a dataset as a manual drop.

## 1. Why

- The failure comes after the click, as a stopped round, and the remedy is outside the system: a file in the
  operator's home directory that only the operator can edit.
- The hosts are known beforehand. A dataset round's jobs may reach only the hosts its catalog record names
  (`mia_data.binding.check_binding` rejects a binding that names any other), so the service can tell which are
  missing before anything is started.
- One preparation request for 200 datasets can need dozens of new hosts. Finding them one stopped round at a
  time does not scale.

## 2. Goals, non-goals, anti-goals

**Goals**

1. A person starting a run from the page is told, before the run starts, which hosts it needs that the policy
   does not allow, and can approve them there.
2. An approved host is usable by the first job of that run, with no restart and no edit by hand.
3. The operator's `broker.toml` stays the hand-written base: mia reads it and never writes it, and an edit to it
   reaches mia's brokers at the next `mia up` or approval.
4. Every approval is on record: which host, when, by whom, for which run; and on the subject's timeline, so the
   mirror copies it to GitHub.
5. An agent that writes a dataset's reader can reach that dataset's approved host.
6. Recorded workflow histories replay unchanged: no workflow or `models.py` change.

**Non-goals**

- **Redirect targets.** A host that redirects downloads to another host still needs `network.redirect_hosts`
  filled in by hand. Targets already configured there are part of what a run needs (§4), so they can be approved.
- **Identity.** The actor is `page`, as for every action (webapp-actions §10).
- **Withdrawing an approval from the page.** The operator removes the host's lines from the approvals file (§3);
  the next `mia up` or approval regenerates the policy without it.
- **Patterns.** An approval is one exact host. A wildcard is a hand edit of the base policy.
- **Changing csub.**
- **The three places that run outside csub today** (§2.1).

**Anti-goals** (a failure even if everything works)

- A sandboxed job, or anything a sandboxed job wrote, widens the policy without a person's click.
- The generated policy is narrower than the base, drops one of its keys, or is left half-written.
- Anything writes the operator's `broker.toml`.

### 2.1 The user's rulings on csub (2026-11-12)

For now every agent runs in the same csub with the same config; all agent-generated code runs in csub; everything
that runs on the cluster runs in csub. The services already follow this: no package calls `bsub`, and the
autodev, match and judge agents are csub jobs. Three things do not, and stay as they are under this spec:

- the judge's evaluation set (`mia_core/judge/eval.py`) runs the judge unsandboxed on the operator's host;
- `mia-buffers check` runs `harness fetch --help` from the harness checkout on the host;
- the `BufferDataset` client runs the harness tensorizer inside the consumer's own training job.

## 3. mia's policy file

Two files in one directory, `csub.policy_dir`:

| File | Written by | Holds |
|---|---|---|
| `broker.toml` | generated (below) | the base policy with the approved hosts added to `broker.allowed_hosts` |
| `approved-hosts.jsonl` | buffer-api only, append only | one line per approved host: `host`, `at`, `by`, `kind`, `number` |

**Config.** A top-level `csub` block in `mia.yaml`, shared by every service (*my call*):

```yaml
csub:
  base_policy: ~/.config/csub/broker.toml   # the operator's; read, never written
  policy_dir: <paths.state_root>/csub       # default; holds the two files above
```

**Where `policy_dir` may be** (*my call*). Three rules, stated once in the generating code and checked by every
generation before it writes, whoever starts it. `mia up` and a `mia-buffers serve` started by hand both exit
with a message naming the path:

1. **It is under the user's home directory.** csub refuses any job mount under home or containing it, which is
   what keeps a job from mounting the directory and rewriting either file. The installed csub does not protect a
   policy's own directory, so a `policy_dir` under one of the base policy's `allowed_roots` could be mounted
   read-write by a job. Home is also where compute nodes already read the default policy, so per-job brokers can
   load the generated one.
2. **It neither contains nor lies inside any directory a worker sandbox binds read-write.** That is each
   service's state dir, `work_root` and socket dir, and the roots its app mounts read-write: `storage_root`
   for mia-data; `raw_root`, `staged_root` and the lock directory for mia-buffers. `mia up` checks the mounts
   of every service, from the same list it builds the sandboxes with. buffer-api started by hand checks the
   roots of its own config.
3. **Its `broker.toml` is not the base policy.** A `policy_dir` of `~/.config/csub` passes the first two
   checks and would make the generated file the operator's own. Both paths are resolved before they are
   compared, so a symlinked directory is caught.

**Generation** is one function in `mia_core` (it imports nothing from the other packages), a pure function of
the base policy and the approvals file:

1. Parse the base with `tomllib`. A missing or invalid base is an error that names the file; nothing is written.
2. Add, in the order they were approved, each approved host that no `broker.allowed_hosts` pattern of the base
   already matches (the same match as `mia_data.binding.missing_hosts`).
3. Serialize, check that parsing the text gives back exactly the merged data, and replace `broker.toml` by a
   rename within `policy_dir`. A value the serializer cannot write is an error that names its key. The file
   starts with a comment saying it is generated and where the base is.

There is no TOML writer in the environment. *My call:* a small serializer for the shapes a csub policy has
(tables one or two deep holding strings, numbers, booleans and lists of strings), guarded by the round-trip check
in step 3, over a new dependency. Marked `# ponytail:`.

**Who generates.** `mia up <service>`, for every app, before it starts the broker; buffer-api when it starts and
after each approval.

**Two generations at once.** A rename alone is not enough: a generation that read the approvals before an append
could publish after the one that read them later, and the newest approval would be missing from the policy. So:

- buffer-api, the only appender, holds one in-process lock across an approval's append and its generation;
- each generation writes to a temp file of its own, renames it into place, then reads the approvals again and
  generates once more if they changed. Whichever generation publishes last has therefore seen every approval
  made before it published.

No lock file is used (home may be NFS).

**The brokers.** `mia up` starts every service's broker as `csub-broker --policy <policy_dir>/broker.toml
--serve <socket>`: one policy for the deployment (the user's ruling, §2.1).

**`mia exec`** (*my call*). `mia exec <service> ...` exports `CSUB_TRANSPORT=unix` and
`CSUB_SOCKET=<the service's socket>` when that socket exists, so a hand-run `check` or job submission goes
through the service's broker and its generated policy. Without a running service it uses the operator's own csub
client config, as today.

## 4. What a run needs

"Needed" is the host list the run's jobs will ask csub for, computed by the code the jobs use, so the page's
answer and the worker's backstop cannot disagree.

| Run | Needed hosts |
|---|---|
| A dataset round | `mia_data.binding.hosts(record_urls(record), cfg)`: the hosts of every URL the catalog record names (`s3://` rewritten), their configured redirect targets, and the package hosts |
| A preparation request | the union over the records its text names. A request that selects by rule names none: nothing is asked, and the backstop applies |
| A buffer's Run (approve at `plan-review`) | the union over the round's pool of the hosts of each published `binding.yaml`'s URLs under `storage_root`, with their redirect targets and the package hosts. `manual://` URLs have no host |

**A buffer's pool** (*my call*) is the `in_pool` rows of the plan the round itself wrote:
`<state_dir>/rounds/<round>/plan.json`, where `<round>` is the round of the spec's `plan-review` stage entry. A
plan recomputed from the spec could differ, since a dataset may have become prepared after planning. When that
file cannot be read the check answers 409 and says so.

**Missing** is needed minus what the generated policy's `broker.allowed_hosts` patterns match. buffer-api reads
the generated file itself; it does not call csub.

A buffer's Plan is not checked: planning downloads nothing, and the pool is not known until the plan exists.

## 5. Routes

**`POST /hosts/missing`** is a read route (a POST only to carry its body, as `/plan` is; it writes nothing). The
body names the run one of three ways:

```json
{"records": ["4tu-bf11171a-carbon-fibre-composite-masks"]}
{"prep": 12}
{"spec": "buffer-2d-gray"}
```

It answers `{"hosts": [{"host": "data.4tu.nl", "records": ["…"]}]}`, sorted by host, each with the records that
need it. An empty list means there is nothing to ask. A record that no synced catalog holds is a 404, as for a
`go`.

**`approve_hosts`** is a new optional list on the bodies of the actions that start jobs:

| Action | Route |
|---|---|
| a dataset's `go` | `POST /datasets/<record>/actions` |
| filing a preparation request | `POST /prep` |
| a request's `go` | `POST /prep/<number>/actions` |
| a buffer's `approve` at `plan-review` | `POST /specs/<name>/actions` |

On any other action a non-empty `approve_hosts` is a 422. For these four, in order:

1. The stage and revision checks the action has today (`_accepts`). A refused action approves nothing.
2. Each host must be a lowercase exact hostname, not a GitHub host, and one of the run's needed hosts (§4).
   Anything else is a 422 that names the host.
3. Append a line per host not yet approved, then regenerate the policy. If generation fails the answer is a 503
   that says why, and the action is not sent.
4. A timeline entry on the subject: ``**Hosts approved:** `data.4tu.nl` by `page` ``, hosts passed through
   `neutralize`.
5. The action is recorded and sent as today.

A repeat of the same `key` finds its hosts approved already and adds no line. These are action routes: no token,
JSON only, this origin only. A caller with the worker token gets no wider bound.

## 6. The page

One dialog, opened from four places:

- a dataset's Start (also Start again and Run again);
- Prepare, both on New buffer ("Prepare these N datasets") and for the ticked datasets of the Datasets list. Both
  are `prepareButton`, which files one request with `POST /prep`, so the dialog is added once there. A request
  that says `again: true` names its records like any other;
- a preparation request's Run again;
- a buffer's Run at `plan-review`.

1. The click posts to `/hosts/missing`. If the check itself fails, a banner says so and nothing is started.
2. No missing host: the button does what it does today.
3. Otherwise the dialog shows:
   - one line on what approving means: jobs of this deployment may then download from that host;
   - a row per host with a checkbox, unticked, and how many of the run's datasets need it (their ids on hover);
   - a "Select all" checkbox;
   - **Approve and start** (enabled once a host is ticked): sends the action with the ticked hosts;
   - **Start without approving**: sends the action as today. Its hover text says what follows: a dataset round
     on such a host stops and needs a person; a buffer waits for that dataset's files to be copied in by hand,
     or refuses its job;
   - **Cancel**.
4. Hosts left unticked behave as under "Start without approving".

The dialog hangs off the body, as the reject dialog does, so a redraw behind it does not take the ticks. Where a
button already asks for confirmation (Prepare), that question comes first. Every button carries a hover text, as
the page's other buttons do. The page holds no token and calls only read and action routes
(`tests/test_buffers_ui.py`).

## 7. autodev's agents

An autodev agent job (both the `autodev` and the `snippets` service) asks for its configured `agent.allow_hosts`
and then for the exact hosts its broker's policy allows, read with a fresh `csub probe` when the job is built.
Patterns (`*`) are skipped, since a job names hosts, and GitHub hosts are never added. autodev reads nothing of
mia-data's: it asks its own broker.

csub takes at most 64 hosts per job. *My call:* the configured hosts come first, then the policy's hosts from the
most recently approved backwards, until 64 are named; the reader being written now is most likely for a recent
approval. Marked `# ponytail:` with the upgrade path (a per-issue host list, or a higher limit in csub).

The data hosts that `mia.yaml` repeats under the `snippets` service's `agent.allow_hosts` become redundant and
can be removed there.

## 8. What does not change

- `submit` still refuses a job whose hosts a fresh `csub probe` does not allow, and `read_binding` still turns a
  buffer's dataset whose URL hosts are not allowed into a manual drop. They now see the generated policy.
  (`read_binding` tests the binding's URL hosts only; an unallowed redirect target or package host is refused at
  `submit`.)
- No stage, no entry in a stage table, no workflow command, no `models.py` field.
- A `go` sent by `MiaPrep` for one of its datasets carries no `approve_hosts` and is never refused for hosts.

## 9. Security model

The policy is what stops a sandboxed job from reaching an arbitrary host. An approval widens it, so:

- A host can be approved only if a catalog record in the run names it (or a published binding, whose URLs
  `check_binding` already held to the record's hosts). The trust root is the catalog repo's content plus a
  person's click on a page that lists the hosts by name.
- The approvals file and the generated policy are under home, which no job can mount, and outside every worker
  sandbox. Only buffer-api and `mia up`, both on the host, write there.
- Jobs cannot reach buffer-api: its host is not in the policy. The worker can, and is bounded as anyone.
- **The policy is the bound, not a job's own host list.** Every job gets a broker socket and the csub client, and
  the installed csub build does not hold a child job to its parent's hosts or mounts. Code in any job can
  therefore submit a child job that reaches any host the policy allows. An approval so widens what every job of
  the deployment can reach, which is what "the same csub with the same config" means today. A csub build that
  bounds children by their parent would make the per-job lists real; that is a csub upgrade, outside this spec.
- No identity: anyone who can open the page can approve a host that a catalog record names. Sign-in changes
  `_actor` and nothing here.

## 10. Rollout, tests and slices

**Rollout.** Deploy, then `mia down` and `mia up` each service once: a broker started before this has no
`--policy`. With no approvals the generated policy equals the base, so nothing changes for running work beyond
the restart. The datasets that stopped on a host (the first is the `data.4tu.nl` one) are then started again
from the page; the dialog offers the host.

**Slices.** Each is deployable on its own.

- **A. The policy file.** Generation (merge and order, round trip, a base that is missing or invalid, a value
  that cannot be written, an approved host the base already matches), the `csub` config block with its three path
  checks, `mia up` passing `--policy`, and `mia exec` using the service's socket. With no approvals the
  deployment behaves as before.
- **B. The service.** Needed and missing hosts for the three kinds of run; `/hosts/missing`; `approve_hosts` on
  the four actions (the bound's 422, the 422 on other actions, the 503 when generation fails, the timeline
  entry, the repeat by key, a refused action approving nothing).
- **C. The page.** The dialog and its four entry points; `tests/test_buffers_ui.py` still holds.
- **D. autodev.** The agent job's host list: configured first, patterns and GitHub hosts left out, the 64 cap.

A small change gets the tests that cover it; the full suite runs before a push.

## 11. Calls made without the user

1. The paths live in a top-level `csub` block; `policy_dir` defaults to `<paths.state_root>/csub` (§3).
2. `policy_dir` must be under home, apart from every worker-writable mount, and not the base's own (§3).
3. A small serializer with a round-trip check, not a new dependency (§3).
4. Approved hosts are kept in an append-only file beside the policy, not in the ledger, so `mia up` can generate
   without buffer-api running (§3).
5. `mia exec` talks to the service's broker when it is running (§3).
6. A buffer's pool is read from the round's own `plan.json` (§4).
7. The approval travels with the start action rather than through a route of its own, so the subject exists for
   the timeline entry and the bound is that run's hosts (§5).
8. The service does not refuse a start for missing hosts; the page asks, and the worker's refusals stay the
   backstop. "Start without approving" keeps the manual-drop path for buffers (§6, §8).
9. A buffer is asked at Run, not at Plan (§4).
10. Checkboxes start unticked (§6).
11. Past 64 hosts an autodev agent job names the configured hosts and the most recent approvals that fit (§7).
