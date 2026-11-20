# MIA: a prod and a dev installation — Spec

**Status:** v0.3 · approved 2026-11-20; builder check of 2026-11-20 folded in; §8 (the cutover) added after the
user's decision that this installation becomes prod. The user's decisions of 2026-11-20 are §2; §8 lists the writer's
calls. Rests on `mia` (`mia_ops`), the one config file `mia.yaml` and the csub `relay` transport. It replaces
the plan in `docs/deployment.md`, which is rewritten when this is built.

## 1. Intent

Two installations of this code run as the developer's account on two hosts: **prod**, which the user depends
on, and **dev**, where the developer goes on building with a small cache. The installation running today
becomes prod, on this host, keeping everything it has: its Temporal server and every workflow, its ledger,
its caches, crops and job records, moved to a MIA data share. Dev is a new, empty installation on another
host. They share nothing: each has its own checkout, `mia.yaml`, Temporal server, ledger, raw and staged
caches, work and storage roots, harness checkout, token files, csub policy and GitHub issue repos. Neither host need be an LSF submit host: jobs are submitted through a
broker on a submit host, reached over ssh. A release is a git tag made in dev and put live on prod with one
command, and a release disturbs the user as little as a restart does today.

**Goals**

- One command takes a tag from dev to prod.
- Rolling back is the same command with an earlier tag.
- A release loses no activity and no workflow: what `mia restart` promises today, a release keeps.
- `mia up` works on a host without `bsub`.

**Non-goals**

- A service account, systemd units, Temporal in Kubernetes, TLS. These stay the later steps of
  `docs/deployment.md` and nothing here makes them harder.
- A buffer-api that stays up through a release. Its gap is the seconds between `down` and `up`.
- Pinning the agentic-sandbox or mia-snippets checkouts in a release. Each host has its own, deployed by
  pulling it, as today; a round pins the harness commit it ran.
- Keeping anything of dev: it starts empty, with its own small cache.

**Anti-goals**

- Two sets of code paths, one for dev and one for prod. Both run the same `mia` on the same `mia.yaml` shape;
  the differences are values.
- Anything the release command does that `mia up` and `mia down` do not already promise.

## 2. Decisions (the user's, 2026-11-20)

1. Prod runs as the developer's own account for now; a service account comes later.
2. Dev and prod are two hosts, two complete stacks, nothing shared.
3. Jobs are submitted through `csub relay` to a broker on a submit host (an installation whose host can
   `bsub` may keep a local broker; both are config).
4. Dev gets repos of its own for everything written (a fork of mia-snippets, a dev datasets repo, a dev
   buffers repo); the catalog is only read and stays shared; this repo's own autodev runs in dev only.
5. This installation becomes prod and starts with everything done so far; dev moves to a new host. Prod's
   shared roots move to a MIA data share, whose paths the user gives later.

## 3. Constraints

- `paths.local_root` and a managed `temporal.state_dir` are host-local disk (`mia up` refuses NFS): each host
  needs a `/scratch` or the like.
- `csub-broker` runs only on an LSF host; the worker's sandbox talks to a unix socket outside it. The relay
  serves that socket and forwards each request over ssh to the broker, which does every check.
- `csub.policy_dir` is under home, and home is the same filesystem on the submit host, so the broker there
  reads the policy buffer-api regenerates on this host, on every request, as now.
- The broker's state (`state_dir` in the operator's base policy) is on the submit host, under the same home:
  prod's base policy names a `state_dir` of its own, or the two installations share one.
- Workers import code from the checkout `mia` runs in, so a release is a new checkout, never a `git pull` in
  the running one.
- Workflow code is deterministic: a release whose `workflows.py` changed still needs `workflow.patched()` for
  the runs in flight (`docs/development.md`). The release command cannot and does not check this.

## 4. Approaches considered

**Isolation.** (a) Two stacks as one user on one host: everything but the tmux session names is config, and
those are the service names. (b) One Temporal server, two namespaces: a dev restart or a broken SQLite file
takes prod down. (c) Two hosts, two stacks: nothing to keep apart. The user chose (c).

**Broker.** (a) `csub relay` to a broker on a submit host: what csub provides for exactly this; the gap is
only that `mia up` knows how to start a local broker. (b) The broker as a long-running LSF job: bills while
idle, dies at the queue limit. The user chose (a).

**Release.** (a) `git pull` in place and restart: the running workers' code changes under them, and there is
no rollback. (b) A checkout per tag under `releases/` and a `current` symlink, flipped after install and
checks: rollback is a flip, the old code stays bound into the old workers until they drain. (b) is the
writer's choice; it is what `docs/deployment.md` planned.

## 5. The installation layout

Home is one filesystem on every host here, dev, prod and the submit host alike, and the relay depends on that
(§6). So an installation is named, not a host's: its root is `~/<installation>` (`~/mia-prod`, `~/mia-dev`),
and nothing of it sits at a path the other installation would also read.

```
~/mia-prod/                   the installation's root
  mia.yaml                    its config, the one copy; `host:` names the host it runs on
  releases/<tag>/             one checkout per released tag, `pixi install`ed; mia.yaml -> ../../mia.yaml
  current -> releases/<tag>   the release that runs
```

`mia.yaml` is found as it is today (`$MIA_CONFIG`, else `mia.yaml` in the checkout): the symlink in each
checkout points at the installation's one copy, and `mia` resolves it before mounting it into the sandbox.
`mia` is run as `~/mia-prod/current/.pixi/envs/default/bin/mia`; an alias in the operator's shell is theirs to
make, and nothing is put on a shared PATH. `mia.yaml` gains an optional `host:`; when it is set, every `mia`
command refuses to run on another host (`socket.gethostname()`), so prod's stack cannot be started on dev by
mistake. `sandbox_dir` and `harness.dir` are absolute paths. What is host-local (`paths.local_root`, a managed
`temporal.state_dir`, the broker socket dir below) is under the host's scratch volume, named for the
installation (`/scratch/mia-prod/...`). Everything else prod needs that is not in the repo is under its root or
home under a name of its own: the base policy (with its own `state_dir`), the GitHub token file, the service
token, the ssh key to the submit host.

Where each path may live follows from who reads it. **Shared** (the cluster filesystem, seen by compute
nodes and the submit host): what a job mounts or runs, and what the broker reads. **Local** (`/opt`, the
host's disk): what only this host's processes read, and what must not be on NFS.

| path | where | who reads it |
|---|---|---|
| the checkout (`releases/`, `current`) | shared | the worker's sandbox on this host; with the relay, the submit host runs its `csub-broker` |
| `sandbox_dir` | local or shared | `mia up`, wrapping the worker on this host. The compute-side sandbox is the one the csub base policy names (`sandbox_scripts_dir`), which must be shared; one shared checkout serves both |
| `harness.dir` | shared | harness jobs on compute nodes |
| `paths.work_root`, `storage_root`, `raw_root`, `staged_root` | shared | jobs mount them; buffer-api and `BufferDataset` clients read them |
| `paths.state_root` | local or shared | the worker and buffer-api on this host; never mounted into a job. Backed-up storage is the better place |
| `csub.policy_dir`, `csub.base_policy` | under home (shared) | the broker, on the submit host with the relay; `policy_dir` must be under home in any case, so when `state_root` is local it is set explicitly |
| `paths.local_root` | local, required | locks and the ledger; refused on NFS |
| `temporal.state_dir` (managed) | local, required | the Temporal dev server's SQLite; refused on NFS |
| `csub.run_dir` | local | the broker or relay socket |
| `github.token_file`, `service.token_file`, the ssh key | local or home | this host's worker, buffer-api and relay |

The csub broker's unix socket dir, `/scratch/<user>/<service>` today, becomes `csub.run_dir` (default
`/scratch/<user>`), checked host-local like `local_root`, so a prod host whose local disk is elsewhere, and two
installations on one host, both work.

## 6. The relay

`csub` gets a `broker` key:

```yaml
csub:
  base_policy: ~/.config/mia-prod/broker.toml
  broker: relay                 # `local` (default): csub-broker --serve on this host, as today
  ssh_host: submit.example.org  # relay only; required
  ssh_user: null                # relay only; default: this user
  ssh_key: ~/.ssh/csub_ed25519  # relay only
```

With `broker: relay`, the broker window of a service's tmux session runs, instead of `csub-broker --policy
<policy> --serve <sock>`:

```
env CSUB_TRANSPORT=ssh CSUB_SSH_HOST=<ssh_host> [CSUB_SSH_USER=<ssh_user>] CSUB_SSH_KEY=<ssh_key> \
    CSUB_BROKER_CMD="<checkout>/.pixi/envs/default/bin/csub-broker --policy <policy>" \
    csub relay <sock>
```

The remote command is this checkout's own `csub-broker`, which the submit host sees through the shared home,
on this installation's generated policy; so a release puts the broker on the new code with the workers, and
the two installations' brokers never read each other's policy. Nothing else changes: the worker gets the same
`CSUB_TRANSPORT=unix CSUB_SOCKET=<sock>`, `mia exec` goes through the same socket, host approval regenerates
the same file, `placement_problem` keeps the same rules. After the relay is up, `mia up` runs one `csub probe`
through the socket and fails with csub's message when the submit host does not answer, so a missing key or a
host that refuses the user is found at start, not at the first job.

The key is a plain key of the user's, kept outside every sandbox. Binding it to a forced command
(`restrict,command="csub-broker --policy ..."` in `authorized_keys` on the submit host) is the hardening that
comes with the service account; `CSUB_BROKER_CMD` is then dropped from the window.

## 7. The release

`mia release <tag>` runs on the host being released to. It:

1. finds `<root>` from the checkout it runs in (`<root>/current` or `<root>/releases/<x>`; any other layout
   is refused with a message saying what the layout is);
2. clones the tag from the running checkout's `origin` into `<root>/releases/<tag>` when that directory does
   not exist (`git clone --branch <tag> --depth 1`), and runs `pixi install --locked` there, so a release has
   exactly the lockfile's packages;
3. symlinks `mia.yaml`, then runs the new checkout's `mia exec <service> check` for every service in it, and
   stops before anything is touched if one fails;
4. flips `current` (a new symlink beside it, renamed over the old: atomic);
5. runs the new checkout's `mia restart`: `down` drains every worker and stops buffer-api last, `up` starts
   buffer-api first, then the workers on the new code, with the relay (or broker) beside them, and Temporal is
   left running (`mia restart` never touches it unless named).

Rolling back is `mia release <earlier tag>`: the clone exists, the install is a no-op, the flip and restart
happen. Released checkouts are kept; removing one is a hand `rm` of a directory that is not `current`.

From dev, `mia release <tag> --to <host>:<root>` pushes the tag to `origin` (`git push origin <tag>`) and runs
`ssh <host> <root>/current/.pixi/envs/default/bin/mia release <tag>`, so the whole release is one command
typed on dev. The user tags; the command never makes a tag.

What a user sees: buffer-api is away for the seconds between down and up, the page's buttons fail during
them, running jobs run on, every workflow continues. A release whose `workflows.py` changed carries its
`workflow.patched()` guard by the existing rule.

## 8. The cutover: this installation becomes prod

What is here today is the stack `mia` runs from the developer's checkout: a managed Temporal server and
`local_root` on this host's local disk, the ledger under it, and the work, crops and staged roots and the
harness checkout under one prefix on the cluster filesystem. The cutover keeps the first two where they are
and moves the rest. Four kinds of absolute path point at the prefix that moves:

- the ledger: `volumes.data` (`path`), `frozen.descriptor`, `assets.key` and `assets.data`, `cases.data`;
- workflow states of open rounds: workspaces (`crop_workspace`, `pass_jobs`, ...) are stored as the absolute
  path `str(work_root / ...)`, and a round at `review` saves its crops from that path when approved;
- timeline entries: browser links to workspaces and crops, built from the roots' `url` at the time;
- job records: addressed relative to `work_root`, so they need nothing.

The cutover, in order, by hand from a runbook in `docs/deployment.md`:

1. `mia pause`; wait until no round is `working` (the Overview); `mia down` (Temporal keeps running).
2. Copy the roots to the share (`rsync -a`; the staged pyramids are the bulk) and the harness checkout.
3. Back up the ledger, then rewrite the prefix in the five columns above with one SQL script (string
   replacement of the old prefix by the new; `assets.key` is the primary key, so it is an update of the key).
4. Leave each old root as a symlink to its new place, so every absolute path a round still holds resolves.
   The symlinks go when no round that was open at the cutover is open any more. New jobs mount the new
   paths, which csub requires to be real directories, and `mia.yaml` names only the new ones.
5. Adopt the layout of §5: `~/mia-prod/mia.yaml` is this `mia.yaml` with the new roots and their `url`s,
   `harness.dir`, `host:` and the csub block; the user tags the commit to run; `mia release <tag>` runs from
   the clone it makes (the first time, the clone and `current` are made by hand as the command would).
6. `mia up` from `current`; `mia resume`. Old timeline links point at the old paths and are left as they are.

The checkout at `~/dev/mia-workflows` stops running anything on this host; it is dev's working checkout,
run from the dev host. Dev's `mia.yaml` is written from the example with dev's repos, roots and `host:`.

## 9. The writer's calls, and what is left

Calls the user can change:

- Dev runs from its working checkout (`~/dev/mia-workflows`) as the live one, as now; the release layout is
  prod's, and dev may adopt it later with the same command. `mia release --to` works from any checkout with
  an `origin`.
- An installation is named and rooted at `~/<name>`, with `host:` as the guard, because home is shared (§5).
- The broker socket dir is `csub.run_dir`, not a fixed `/scratch` (§5).
- The remote broker command uses `CSUB_BROKER_CMD`, not a forced command, until the service account.
- The ledger's prefix rewrite is a one-time SQL script in the runbook, not a `mia` command; the old roots
  stay as symlinks for the open rounds instead of rewriting workflow histories.
- Each installation's `mia.yaml` is written by hand from `mia.yaml.example`; no templating.

Still the user's: the paths on the MIA data share, the tag of the first prod release, dev's three repos, and
whether prod keeps a local broker on this host.

Bring-up of dev, once, by hand (also in `docs/deployment.md`): a host with a local scratch volume and the
cluster filesystems; the checkout; an agentic-sandbox checkout and a mia-snippets checkout; a base policy of
its own with its own `state_dir`; the ssh key, tested with `csub probe` under the relay's variables; the
GitHub token file, `mia init-token`; a Temporal `state_dir` and `local_root` on the scratch volume; `mia up`.

Left to the builder: the exact argument parsing, the one test per piece (the relay's command line; the
release's layout resolution and its refusal; the `host:` guard), the rewrite of `docs/deployment.md` as the
system is, with the cutover and bring-up runbooks, and the `mia.yaml.example` comments.
