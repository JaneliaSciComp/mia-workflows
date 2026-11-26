# Installations, releases and the relay

How the services run as installations an operator keeps: a **prod** installation the user depends on and a
**dev** installation the developer works in, on two hosts, sharing nothing; how a tagged release goes live; and how
a host that cannot `bsub` submits jobs through a broker on a submit host. Developer setup of a single checkout is
[`autodev-local-setup.md`](autodev-local-setup.md); the service-account checklist (GitHub App, branch protection,
broker policy) is in [`autodev-runbook.md`](autodev-runbook.md). Back to the [README](../README.md).

## The two installations

An installation is one `mia.yaml` and everything it names: a checkout, a Temporal server, buffer-api's ledger, the
raw and staged caches, the work and storage roots, a harness checkout, token files, a csub policy and the GitHub
repos it writes. Prod and dev are two installations on two hosts; nothing is shared between them. The home
directory *is* shared between every host here, dev, prod and the submit host alike (the relay depends on it), so an
installation is named, not a host's: its root is `~/<installation>` (`~/mia-prod`, `~/mia-dev`), and nothing of it
sits at a path the other installation would also read.

```
~/mia-prod/                   the installation's root
  mia.yaml                    its config, the one copy; `host:` names the host it runs on
  releases/<tag>/             one checkout per released tag, `pixi install`ed; mia.yaml -> ../../mia.yaml
  current -> releases/<tag>   the release that runs
```

`mia` is run as `~/mia-prod/current/.pixi/envs/default/bin/mia` (an alias in the operator's shell is theirs to make;
nothing goes on a shared PATH). `mia.yaml` is found as always (`$MIA_CONFIG`, else `mia.yaml` in the checkout): the
symlink in each checkout points at the installation's one copy, and `mia` resolves it before mounting it into the
sandbox. `host:` in `mia.yaml` makes every `mia` command refuse to run on another host, matched by the first label of
the hostname, so prod's stack cannot be started on dev by mistake. `sandbox_dir` and `harness.dir` are absolute paths
there.

**Where each path may live** follows from who reads it. *Shared* (the cluster filesystem, seen by compute nodes and
the submit host): what a job mounts or runs, and what the broker reads. *Local* (`/opt`, `/scratch`: the host's own
disk): what only this host's processes read, and what must not be on NFS.

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
| `csub.run_dir` | local | the broker or relay socket (default `/scratch/<user>`) |

**Who may write it.** The operator's account alone writes what an installation runs or trusts: the checkout,
`mia.yaml`, `sandbox_dir`, `harness.dir`, the state and host-local directories, and any script a timer runs for it
(a backup). The same holds for every directory above them: whoever can write one of these files, or rename a
directory on the way to it, runs code as the operator at the next start. Jobs write the roots as the operator and
clients only read, so nothing needs group write: clone and install a release under `umask 022`, and where a parent
directory belongs to a group, keep what a timer runs on the host's own disk.
| `github.token_file`, `service.token_file`, the ssh key | local or home | this host's worker, buffer-api and relay |

**GitHub.** Everything an installation writes on GitHub is its own: autodev's `snippets` deployment answers every
`snippets:go` label on `github.snippets_repo`, the mirror finds a subject's issue by `<deployment>: subject` in
`github.data_repo` and `buffers_repo`, and this repository's own `autodev` service polls its issues. Two
installations on one repo would both answer. Dev points at repos of its own for all of these (a fork of mia-snippets,
a dev datasets repo, a dev buffers repo); `catalog_repo` is only read and stays shared; the `autodev` service for this
repository runs in one installation only.

## The relay

`csub-broker` runs only on an LSF host, and the worker's sandbox talks to a unix socket outside it. On a host that
cannot `bsub`, `csub.broker: relay` makes `mia up` serve that socket with `csub relay`, which forwards each request
over ssh to a broker on a submit host; every check stays the broker's.

```yaml
csub:
  base_policy: ~/.config/mia-prod/broker.toml
  broker: relay                 # `local` (default): csub-broker --serve on this host
  ssh_host: submit.example.org  # relay only; required
  ssh_user: null                # relay only; default: this user
  ssh_key: ~/.ssh/csub_ed25519  # relay only
  run_dir: /opt/mia-prod/run    # the sockets; host-local (default /scratch/<user>)
```

The broker window of a service's tmux session then runs

```
env CSUB_TRANSPORT=ssh CSUB_SSH_HOST=<ssh_host> [CSUB_SSH_USER=<ssh_user>] CSUB_SSH_KEY=<ssh_key> \
    CSUB_BROKER_CMD="<checkout>/.pixi/envs/default/bin/csub-broker --policy <policy>" csub relay <sock>
```

The remote command is this checkout's own `csub-broker`, which the submit host sees through the shared home, on this
installation's generated policy: a release puts the broker on the new code with the workers, and two installations'
brokers never read each other's policy. Nothing else changes: the worker gets the same `CSUB_TRANSPORT=unix
CSUB_SOCKET=<sock>`, `mia exec` goes through the same socket, host approval regenerates the same file, and the
placement rules of `policy_dir` are the same. Once the relay serves its socket, `mia up` runs one `csub probe`
through it and fails with csub's message when the submit host does not answer (a missing key, a refused user), so
that is found at start, not at the first job; the session stays for `mia logs <service> broker`.

The key is a plain key of the user's, kept outside every sandbox. The broker's own state (`state_dir` in the base
policy) is on the submit host under the same home, so each installation's base policy names a `state_dir` of its
own. Binding the key to a forced command (`restrict,command="csub-broker --policy ..."` in `authorized_keys` on the
submit host) is the hardening that comes with the service account; `CSUB_BROKER_CMD` is then dropped.

## Releases

`mia release <tag>` runs on the installation being released to, from its `current` (or any `releases/<x>`):

1. finds `<root>` from the checkout it runs in; any other layout is refused with a message saying what the layout is;
2. when `releases/<tag>` is not there, or is there without the `mia.yaml` link that marks a finished install, clones
   the tag from the running checkout's `origin` (`git clone --branch <tag> --depth 1`) into it and runs
   `pixi install --locked` there, where the environment stays (a pixi environment hard-codes its own path), so a
   release has exactly the lockfile's packages; a failed clone or install leaves nothing behind. A tag is letters,
   digits, `.`, `_` and `-`;
3. links `mia.yaml` last, then runs the new checkout's `mia exec <service> check` for every service, and stops
   before anything is touched if one fails;
4. flips `current` (a new symlink beside it, renamed over the old: atomic);
5. runs the new checkout's `mia restart`: `down` drains every worker and stops buffer-api last, `up` starts buffer-api
   first, then the workers on the new code with their broker or relay; Temporal is left running.

Rolling back is `mia release <earlier tag>`: the clone exists, so only the flip and the restart happen. Released
checkouts are kept; removing one is a hand `rm` of a directory that is not `current`.

From dev, `mia release <tag> --to <host>:<root>` pushes the tag to `origin` and runs `ssh <host>
<root>/current/.pixi/envs/default/bin/mia release <tag>`, so the whole release is one command typed on dev. The
operator tags; the command never makes a tag.

What a user sees: buffer-api is away for the seconds between down and up, the page's buttons fail during them,
running jobs run on, every workflow continues. A release whose `workflows.py` changed carries its
`workflow.patched()` guard by the rule in [`development.md`](development.md).

## Runbook: the cutover of a single checkout to prod

A developer's checkout that runs the stack becomes the prod installation keeping everything it has: its managed
Temporal server and `local_root` stay on the host's local disk; the work, crops and staged roots and the harness
checkout move from their prefix on the cluster filesystem to the data share. Four kinds of absolute path point at the
prefix that moves: the ledger (`volumes.data`'s `path`, `frozen.descriptor`, `assets.key` and `assets.data`,
`cases.data`); the workflow states of open rounds (workspaces stored as `str(work_root / ...)`; a round at `review`
saves its crops from that path when approved); timeline entries (browser links built from the roots' `url` at the
time); job records are addressed relative to `work_root` and need nothing.

1. `mia pause`; wait until no round is `working` (the Overview); `mia down` (Temporal keeps running).
2. Copy the roots and the harness checkout to the share (`rsync -a`; the staged pyramids are the bulk).
3. Back up the ledger (`cp <local_root>/service/buffers.sqlite <somewhere safe>`), then rewrite the prefix:

   ```sql
   UPDATE volumes SET data = replace(data, '/OLD/prefix', '/NEW/prefix');
   UPDATE frozen SET descriptor = replace(descriptor, '/OLD/prefix', '/NEW/prefix');
   UPDATE assets SET key = replace(key, '/OLD/prefix', '/NEW/prefix'), data = replace(data, '/OLD/prefix', '/NEW/prefix');
   UPDATE cases SET data = replace(data, '/OLD/prefix', '/NEW/prefix');
   ```

4. Leave each old root as a symlink to its new place (`mv <old> <old>.moved && ln -s <new> <old>` once the copy is
   verified, or remove the old copy and link), so a worker still reads every absolute path a round holds. The
   symlinks go when no round that was open at the cutover is open any more. New jobs mount the new paths, and
   `mia.yaml` names only the new ones. csub refuses a mount that goes through a link, so a round that stopped
   before the move is restarted, not resumed: a resumed one would mount the workspaces its state holds.
5. Adopt the layout above: `~/mia-prod/mia.yaml` is the checkout's `mia.yaml` with the new roots and their `url`s,
   `harness.dir`, `host:` and the `csub` block; tag the commit to run; make the first release by hand as the command
   would (`git clone --branch <tag> --depth 1 <origin> ~/mia-prod/releases/<tag>`, `pixi install --locked` there,
   `ln -s ../../mia.yaml` inside it, `ln -s releases/<tag> ~/mia-prod/current`).
6. `mia up` from `current`; `mia resume`. Old timeline links point at the old paths and are left as they are.

The developer's checkout then runs nothing on this host; it is dev's working checkout, run from the dev host.

## Runbook: bringing up a new installation

On a host with a local scratch volume and the cluster filesystems mounted:

1. The installation's root and its first release, as in step 5 above; an agentic-sandbox checkout and a mia-snippets
   checkout on the shared filesystem.
2. A base policy of its own (`csub.base_policy`), with its own `state_dir` and the LSF project; `csub.policy_dir`
   under home.
3. For the relay: the ssh key to the submit host, tested by hand with `CSUB_TRANSPORT=ssh CSUB_SSH_HOST=<host>
   CSUB_SSH_KEY=<key> CSUB_BROKER_CMD="<checkout>/.pixi/envs/default/bin/csub-broker --policy <policy>" csub probe`.
4. The GitHub token file (`gh auth token > <file>`, mode 600) and `mia init-token` for the service token; for dev,
   `github.snippets_repo`, `data_repo` and `buffers_repo` naming dev's own repos.
5. `temporal.state_dir`, `paths.local_root` and `csub.run_dir` on the scratch volume, named for the installation;
   `host:` naming this host; the roots on the shared filesystem, small for dev.
6. `mia up`.

## Not built: the service account

What a prod installation run as a service account still needs, none of it touching `workflows.py`:

- A systemd runner for `mia_ops` (`runner: systemd`), units from the same `Stack`/`Service` objects the tmux runner
  uses, with the socket dir under `$XDG_RUNTIME_DIR` once `loginctl enable-linger` is set.
- Temporal outside the host (Kubernetes), `temporal.managed: false`, TLS or an API key on `TemporalCfg`, namespaces
  created by an operator and only checked by `ensure_namespace`.
- Agent billing from a file (`agent.api_key_file` bound read-only into the job) instead of an interactive login.
- A GitHub App identity (`github.auth: app`) per the runbook.
- `paths.state_root` on backed-up storage and a backup of the Temporal database.
- The ssh key to the submit host bound to a forced command, as above.
