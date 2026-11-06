# Stages

A **stage** is one computation step in a Sunbeam pipeline: it declares the
signals it needs, the signals it produces, and the frequency it runs at, and
implements a pure(ish) function from one to the other. Stages are the unit
everyone actually writes when adding a new derived quantity — this document
covers the contract, how stages become a running pipeline, and the checklist
for adding a new one.

For how stages fit into the rest of the codebase, see
[`CONTRACTS.md`](CONTRACTS.md#stage). For how to *run* a pipeline made of
stages, see [`USAGE.md`](USAGE.md).

## The `Stage` contract

Every stage subclasses `stage.stage.Stage` (`stage/stage.py`) and declares
four **class-level** attributes:

```python
from typing import ClassVar
from data_tools.localization import CanonicalName
from state.frame import Frame, FrameView
from stage.stage import Stage

class Array(Stage):
    stage_name: ClassVar[str] = "Array"
    inputs: ClassVar[list[CanonicalName]] = [
        CanonicalName.MPPTInputVoltageA, CanonicalName.MPPTInputCurrentA,
        CanonicalName.MPPTInputVoltageB, CanonicalName.MPPTInputCurrentB,
    ]
    outputs: ClassVar[list[CanonicalName]] = [CanonicalName.ArrayPower]
    frequency: ClassVar[float] = 5.0  # Hz

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # per-instance state, if any (accumulators, config loaded from
        # event_name, ...) goes here

    def run(self, input_frame: FrameView) -> Frame:
        new_frame = Frame.from_view(input_frame)
        power_a = input_frame.read(CanonicalName.MPPTInputVoltageA) * \
                  input_frame.read(CanonicalName.MPPTInputCurrentA)
        power_b = input_frame.read(CanonicalName.MPPTInputVoltageB) * \
                  input_frame.read(CanonicalName.MPPTInputCurrentB)
        new_frame.write(CanonicalName.ArrayPower, power_a + power_b)
        return new_frame
```

- **`stage_name`** — unique within a pipeline edition; becomes the node
  name in the dependency graph and shows up in `current_stage` on the
  dashboard and in `TimingStats` logs.
- **`inputs`** / **`outputs`** — lists of `CanonicalName` (or plain string)
  signal identifiers. These are what wire stages together — see
  "How stages become a graph" below.
- **`frequency`** — the rate, in Hertz, this stage should tick at. Stages with
  the same frequency and no cross-rate dependency get grouped onto the same
  scheduled `Pipeline` (see below); different frequencies always run on
  separate schedules.
- **`run(self, input_frame: FrameView) -> Frame`** — the actual computation.
  `input_frame` has `.read(signal)`; build the output with
  `Frame.from_view(input_frame)` then `.write(signal, value)` for each
  declared output. Only write signals listed in `outputs` — anything else
  is discarded by `Pipeline.run` when it folds the frame back into `State`.

`Stage.__init_subclass__` enforces the first four at **class-definition
time** (i.e. at import), not at instantiation — forgetting one of them
raises `TypeError` the moment the module is imported, so a broken stage
can't accidentally reach `StageLibrary` at all.

### The `run()` method

`run()` should be a function of `input_frame` and the stage's own instance
attributes (accumulators like `IntegratedPackPower.total_energy`, or
one-time config loaded in `__init__` like `LatitudeLongitude`'s track
bounding box). It should **not** read `Context()`, hit the database, or do
network I/O — with one deliberate exception:

### The ingress stages

`Ingress` (`stage/v3_0/ingress.py`; each edition registers its own) is not a normal computation stage — it's
how *external* data (from InfluxDB, via `RealtimeIngressQuerier`) enters the
pipeline as signals during a realtime run. It's special in three ways worth
knowing if you ever touch it:

1. It overrides `frequency`, `outputs`, and `stage_name` as **properties**
   instead of class attributes, because one `Ingress` *instance* exists per
   frequency bin — `RealtimeIngressFactory` creates one per distinct
   frequency among the signals no other stage produces.
2. Its `run()` does real I/O (an InfluxDB query per tick) — the one place
   in the stage layer where that's intentional.
3. It enforces a **data grace period**: a signal with no data for longer
   than the class's `data_grace_s` (1.0s in `v3_0`; `v4_0`'s subclass
   raises it to 1000s) causes `run()` to raise, which the executor treats as an
   ingress crash and fails the worker. Brief gaps (queries racing the data
   landing in InfluxDB) are tolerated via `MissingDataTracker`; a signal
   that's *actually* gone dark is not silently ignored. A poll InfluxDB
   rejects outright (bad token, unknown bucket) raises at once.
4. It may leave out a signal that had no data this poll
   (`may_omit_outputs = True`). Every other stage must write all of its
   declared outputs on every run, or the pipeline raises — otherwise
   downstream stages would silently keep reading the previous value.

`Offline_Ingress` (`stage/v3_0/offline_ingress.py`) is its replay
counterpart (see [`USAGE.md`](USAGE.md#realtime-vs-replay-worker-runs)): a single
instance, built by `OfflineIngressFactory`, that fetches *every* ingress
signal over the whole event window in one go (via
`OfflineIngressQuerier`) and outputs each as a `TimeSeries` rather than a
single value. It runs exactly once, before the compute pipelines start, so
it is never scheduled — its `frequency` of `0` is never used. During the
replay, `State` samples each `TimeSeries` at the tick's timestamp, so your
compute stages still read plain values and don't need to know which mode
they're running in.

You will not normally write another stage like this — ingress is generated
automatically for whatever signals your compute stages need but no stage
produces. You only touch `Ingress`/`Offline_Ingress` themselves if you're
changing how telemetry enters the system (e.g. a new backing store). An
edition needs both registered to support both realtime and replay runs.

## Pipeline Construction

This is performed by `pipeline/pipeline_generator.py`, and it's worth understanding even
though you rarely call it directly:

1. **Build a dependency graph** (`build_node_graph`): one node per stage,
   one edge `producer -> consumer` per signal a stage consumes that another
   stage in the set produces. Two stages declaring the same output is a
   hard error (`ValueError: Signal ... is produced by multiple nodes`), as
   are two stages with the same `stage_name` (it's the graph's node key, so
   one would silently replace the other) and a cycle.
2. **Find every signal nobody in the set produces** — those are the
   *ingress signals*, the real telemetry your compute stages ultimately
   depend on (`collect_signals_for_ingress`).
3. **Generate ingress stages for them**, via an `IngressFactory`: for a
   realtime run, `RealtimeIngressFactory` bins the signals by frequency
   (from `data_tools`'s localization tables) and makes one `Ingress` stage
   per bin; for a replay, `OfflineIngressFactory` makes a single
   `Offline_Ingress` stage for all of them.
4. **Split the compute stages and the ingress stages, separately, into
   same-rate subgraphs** (`same_rate_components`): cross-rate edges are
   removed first, then each connected component becomes one `Pipeline`,
   running at its single shared frequency. `generate_pipelines` returns
   the two lists (compute pipelines, ingress pipelines), which the executor
   schedules separately.
5. Each `Pipeline` executes its nodes in **topological order** every tick
   (computed once at construction, not per-tick). A stage whose inputs
   aren't in `State` yet (e.g. a fast pipeline's first tick, before a
   slower cross-rate producer has run once) skips that tick quietly.
   Telemetry that stops arriving is caught where it goes missing, by the
   ingress stage's grace period (above). There is no deterministic order in which different pipelines run (_note: perhaps there should be!_).

The upshot: **you never manually wire a pipeline together**. You write
stages with correct `inputs`/`outputs`/`frequency`, list the stages an
event should run when you create it in the dashboard, and the generator does the rest — figures
out what needs to come from ingress, groups by rate, and orders execution.

### Cross-Rate Dependencies

If a 10 Hz stage depends on a signal a 2 Hz stage produces, that's a
cross-rate edge — it's cut when building subgraphs (step 4 above), and the
10 Hz consumer reads whatever value the 2 Hz producer last wrote into
`State`. This is intentional: `State` always holds the *latest* value of
every signal regardless of which pipeline last wrote it, so a fast
consumer naturally holds a slightly-stale value between slow-producer
ticks. If your stage design assumes otherwise (e.g. it wants to know
*when* the slow signal last changed), that's a sign it should either share
the slow stage's frequency or read a timestamp signal explicitly.

## Pipeline Editions and `stage_registry.toml`

A **pipeline edition** (`v3_0`, `v4_0`, ...) is a named, versioned set of
stage implementations — it exists because the vehicle's telemetry format,
signal set, and computation logic might change between hardware generations, and
old recorded events still need to replay against the stage code that
matches *their* era.

`stage/stage_registry.toml` is the map from `(edition, stage_name)` to the
Python class that implements it:

```toml
[v3_0]
name = "Brightside 2024"

[v3_0.Array]
module = "stage.v3_0.array"
class = "Array"

[v3_0.Ingress]
module = "stage.v3_0.ingress"
class = "Ingress"

[v3_0.Offline_Ingress]
module = "stage.v3_0.offline_ingress"
class = "OfflineIngress"
```

`StageLibrary(pipeline_edition)` (`stage/stage_library.py`) reads this file
and resolves `get_stage_by_name("Array")` to the `Array` class via
`importlib.import_module`. An event's `pipeline_edition` (chosen when
it's created) selects which top-level table
(`[v3_0]`/`[v4_0]`/...) is used.

### Directory Convention

Each edition's stage implementations live in `stage/<edition>/`, e.g.
`stage/v3_0/array.py`, `stage/v3_0/motor_power.py`. There's no code-level
requirement that the directory name match the registry key — it's a
convention, not enforced — but there is no reason to break it.

### Reusing a stage from another edition

Most stages carry over unchanged from one edition to the next. Don't copy
the file; either:

- **Reuse it as is**, with `from` in place of `module`/`class`. The edition
  then runs exactly that edition's class:
  ```toml
  [v4_0.Offline_Ingress]
  from = "v3_0"
  ```
- **Subclass it** when only a little changes, and override just that. A
  stage inherits `stage_name`, `inputs`, `outputs`, and `frequency` from
  its parent. `stage/v4_0/ingress.py` is the example:
  ```python
  from stage.v3_0 import ingress

  class Ingress(ingress.Ingress):
      data_grace_s: ClassVar[float] = 1000.0
  ```

Either way the stage's code, and its imports, come from the other edition,
so whatever it needs must also be installed in this edition's worker image
(its extra). Once the original would have to change in a way that doesn't
suit both editions, give the new edition its own copy.

## Adding A New Stage

1. **Write the class** in `stage/<edition>/your_stage.py`, following the
   contract above. Look at an existing stage of similar shape first —
   `stage/v3_0/array.py` for a stateless one, `stage/v3_0/
   integrated_pack_power.py` for one that accumulates state across ticks,
   `stage/v3_0/latitude_longitude.py` for one that loads per-event config
   in `__init__`.
2. **Register it** in `stage_registry.toml` under the right edition:
   ```toml
   [v3_0.YourStage]
   module = "stage.v3_0.your_stage"
   class = "YourStage"
   ```
   Skip this and `StageLibrary.get_stage_by_name` raises `ValueError:
   Stage 'YourStage' not found.` the moment an event tries to use it.
3. **Add it to the event(s) that should run it**: tick it in the event's
   dialog on the dashboard (click the event's row). A stage not listed for an event simply never
   runs for that event — no error, it's just absent from the graph.
4. **If it depends on a new third-party package**, add that package to the
   pipeline edition's `[project.optional-dependencies]` extra in
   `pyproject.toml` (e.g. `v3_0 = [...]`), *not* to `executor` or `server`.
   This is the reason editions are separate `uv` extras at all — see
   [`USAGE.md`](USAGE.md#per-edition-extras) for the full
   rationale. If your stage needs nothing new, skip this step.
5. **Write tests** under `tests/<edition>/test_your_stage.py`, following
   the existing pattern: a `conftest.py` fixture constructing the stage,
   then `stage.run(make_frame_view({...}))` assertions. These are pure
   unit tests — no database, no pipeline machinery, just `FrameView` in,
   `Frame` out. See `tests/v3_0/test_array.py` for the shape.
6. **Run the checks**: `uv run ruff check .`, `uv run mypy .`,
   `uv run pytest`. See the [README](../README.md#development) for the
   full command set.
7. **If you're changing an existing stage's inputs/outputs**, remember
   that changes to `outputs` also change what ingress needs to supply if
   nothing else produces the old output's downstream consumers — the
   generator handles this automatically, but run the worker locally with
   `--serverless` and check the startup log line (`Executor got N compute
   pipeline(s) and M ingress pipeline(s): [...]`) if a pipeline's shape
   looks unexpected.

## Adding A Pipeline Edition

Pipeline editions need a few things to become valid. If the new version is `v6_7`. `6` is the major version number, indicating the car version, and `7` is the minor, indicating a revision of the telemetry system. Patch numbers are not used. For example, Brightside is our 3rd edition vehicle and so it's first usage in 2024 maps to `v3_0`. In 2025 some new sensors had been added allowing for new stages, hence a new revision to `v3_1`.
1. A directory in `stage/` holding all of its stages, named according to the new version, in this case `stage/v6_7`.
2. An entry in the `stage/stage_registry.toml` registering all of the edition's stages (including `Ingress` and `Offline_Ingress`), using the `v6_7` identifier. Stages that don't change can be reused from an earlier edition rather than copied (see [Reusing a stage from another edition](#reusing-a-stage-from-another-edition)).
3. A new dependency group `v6_7` declared in the `pyproject.toml`'s `[project.optional-dependencies]` table listing the dependencies that the stages require. The new entry should also be added to the group of `conflicts` in `[tool.uv]` to conflict with the existing pipeline editions to ensure only one is activated at a time.

### A note on `CanonicalName`

Signal identifiers (`CanonicalName.VehicleSpeed`, etc.) come from the
`ubc-solar-data-tools` package (`data_tools.localization`), not from
Sunbeam itself. If a signal you need doesn't have a `CanonicalName` member
yet, that's a `data-tools` change, not a Sunbeam one — this repo only
consumes the enum and the localization tables that map a `CanonicalName` +
date to a physical field name, source, unit, and frequency in InfluxDB.

## Versioning stages: what "v3_0" vs "v4_0" means

A pipeline edition is a **complete, independently-versioned snapshot** of
stage behavior — not a feature flag or a diff. When the vehicle's telemetry
changes enough that old stage logic would misinterpret new data (or new
stage logic would misinterpret old data), you cut a new edition
(`stage/v4_0/` and a new `[v4_0]` table in `stage_registry.toml`)
rather than branching inside existing
stage code. Recorded events keep their original `pipeline_edition` in the
database (`Event.pipeline_edition`) forever, so replaying `FSGP_2024_Day_1`
always runs `v3_0`'s `Array`, even after `v4_0` exists and is the default
for new events.

Each edition has its own `uv` extra for its stages' dependencies (see
[`USAGE.md`](USAGE.md#per-edition-extras)), so the **worker Docker image is
built per edition**: `pipeline/Dockerfile` takes `PIPELINE_EDITION` as a
build arg, and the server builds one `sunbeam-worker:<edition>` image per
edition in `stage_registry.toml` at startup. No edition's dependencies leak
into another's image.
