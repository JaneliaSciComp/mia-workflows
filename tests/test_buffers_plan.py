import copy
import hashlib
from pathlib import Path

import pytest
import yaml

from mia_buffers.plan import (
    NOT_PREPARED,
    UNKNOWN_SHAPE_NOTE,
    Fit,
    Plan,
    bound_shapes,
    build_plan,
    crops_for,
    fit,
    plan_digest,
    positions,
    split_counts,
    weights,
)
from mia_buffers.spec import Spec, load_spec
from mia_buffers.specmap import Facts, facts

FIXTURES = Path(__file__).parent / "fixtures" / "buffers"
CATALOG = FIXTURES / "catalog"
BCSS = "github-bcss-breast-cancer-semantic-segmentation"
U937 = "zenodo-20797339-u937-edu-h2ax-dapi-confocal"
MITOLAB = "empiar-11037-cem-mitolab-mitochondria"
HEMIBRAIN = "janelia-hemibrain-v1-2-fib-sem-connectome"
ONE_CH, DUP = "synthetic-em-2d-1ch", "synthetic-em-2d-dup"


def records() -> list[tuple[str, dict]]:
    paths = sorted(CATALOG.glob("datasets/**/*.yaml"))
    return [(str(p.relative_to(CATALOG)), yaml.safe_load(p.read_text())) for p in paths]


F: dict[str, Facts] = {f.id: f for f in (facts(path, record) for path, record in records())}


def spec_with(base: Spec, **edits) -> Spec:
    """A copy of `base` with dotted-path edits applied to its dict form, revalidated."""
    data = base.model_dump(by_alias=True, exclude={"warnings"})
    data = copy.deepcopy(data)
    for dotted, value in edits.items():
        node = data
        *parents, leaf = dotted.split(".")
        for key in parents:
            node = node[key]
        node[leaf] = value
    return Spec.model_validate(data)


@pytest.fixture
def spec() -> Spec:
    return load_spec(FIXTURES / "spec.yaml")


@pytest.fixture
def spec_2d_mixed(spec) -> Spec:  # 2D only, any annotation source: BCSS, U937 and the 1-channel record admitted
    return spec_with(spec, **{"select.where.dims": {"in": ["2D"]}, "select.where.annotations.source": "any"})


@pytest.fixture
def spec_2d(spec_2d_mixed) -> Spec:
    return spec_with(
        spec_2d_mixed, **{"select.exclude.ids": [ONE_CH], "select.exclude.reasons": {ONE_CH: "one channel"}}
    )


def test_fit_rules(spec):
    assert fit(F[MITOLAB], spec).reason == "footprint 1024×1024 exceeds 224×224"
    assert fit(F[BCSS], spec) == Fit(
        True, "fits (shape varies: checked against the representative item)", (1024, 1024), True
    )
    assert fit(F[DUP], spec).reason == "no array shape"
    assert fit(F[U937], spec).reason == "fits"
    assert fit(F[HEMIBRAIN], spec).reason == "fits"


def test_fit_is_refused_when_no_bound_array_holds_the_footprint(spec, spec_2d):
    """The record lists a large array, but the binding reads smaller ones: no crop can be drawn from them."""
    small = [{"z": 27, "y": 355, "x": 389}, {"z": 73, "y": 817, "x": 686}]
    assert fit(F[U937], spec, small).reason == "footprint 1024×1024 exceeds every bound array (largest 817×686)"
    assert fit(F[U937], spec, [*small, {"c": 3, "y": 1024, "x": 2000}]).reason == "fits"  # one array is enough
    assert fit(F[U937], spec, [{"y": 2000, "x": 1023}]).ok is False
    assert fit(F[MITOLAB], spec, [{"y": 4096, "x": 4096}]).reason == "footprint 1024×1024 exceeds 224×224"
    plan = build_plan(spec_2d, "h", None, records(), prepared={U937, BCSS}, bound={U937: small})
    rows = plan.rows_by_id
    assert not rows[U937].in_pool and rows[U937].fit.reason.endswith("(largest 817×686)")
    assert rows[BCSS].in_pool and rows[BCSS].weight == 1.0  # no shapes on its row: the record's array decides


def test_bound_shapes_takes_the_rows_that_have_them():
    rows = [{"id": "a", "raw_shapes": [{"y": 8, "x": 8}]}, {"id": "b", "raw_shapes": None}, {"id": "c"}]
    assert bound_shapes(rows) == {"a": [{"y": 8, "x": 8}]}


def test_positions_and_weights(spec_2d):
    assert positions(F[U937], spec_2d) == 100 and positions(F[BCSS], spec_2d) == 906
    plan = build_plan(spec_2d, "h", None, records())
    rows = [plan.rows_by_id[U937], plan.rows_by_id[BCSS]]
    w = weights(rows, ["modality", "dataset"], 0.5)
    assert w == pytest.approx({U937: 10 / (10 + 906**0.5), BCSS: 906**0.5 / (10 + 906**0.5)}, rel=1e-3)
    assert weights(rows, ["dataset"], 0.0) == {U937: 0.5, BCSS: 0.5}
    assert weights(rows, ["dataset"], 1.0) == pytest.approx({U937: 100 / 1006, BCSS: 906 / 1006})


def test_split_counts():
    assert split_counts(25, {"train": 0.8, "val": 0.1, "test": 0.1}) == {"train": 20, "val": 3, "test": 2}
    assert split_counts(151, {"train": 0.8, "val": 0.1, "test": 0.1}) == {"train": 121, "val": 15, "test": 15}


def test_example_spec_is_invalid_for_mixed_dims(spec):
    plan = build_plan(spec_with(spec, **{"select.where.annotations.source": "any"}), "h", None, records())
    assert "one buffer serves one dimensionality; pool has ['2D', '3D']: narrow select.where.dims" in plan.errors
    assert not plan.valid


def test_valid_2d_plan(spec_2d):
    plan = build_plan(spec_2d, "h", None, records())
    assert plan.valid and plan.errors == []
    assert {r.id for r in plan.rows if r.admitted and r.fit is not None and r.fit.ok} == {BCSS, U937}
    assert ONE_CH not in plan.rows_by_id and plan.excluded["excluded by id: one channel"] == 1
    assert plan.n_seen == len(records()) and set(plan.rows_by_id) == {BCSS, U937}
    assert plan.rows_by_id[BCSS].notes == ["download_method other: needs a manual drop (spec v0.4 §3)"]
    assert plan.rows_by_id[U937].splits == {"train": 20, "val": 3, "test": 2}
    assert plan.rows_by_id[U937].weight == pytest.approx(10 / (10 + 906**0.5), rel=1e-3)
    assert DUP not in plan.rows_by_id and MITOLAB not in plan.rows_by_id
    assert any(rule.startswith("largest raw array under min_size_voxels") for rule in plan.excluded)
    assert plan.warnings == [
        "sampling.label_axis is ignored: labels are a separate tensor (spec v0.4 §8.5)",
        "set and without effect (nothing reads them): sampling.emit, cache",
        "dedup.by content_hash is not available; doi used",
    ]


def test_channel_policy(spec_2d_mixed):
    plan = build_plan(spec_2d_mixed, "h", None, records())
    assert f"patch.c: all needs one channel count; pool has {{1: ['{ONE_CH}'], 3: ['{BCSS}', '{U937}']}}" in plan.errors
    assert build_plan(spec_with(spec_2d_mixed, **{"sampling.patch.c": 1}), "h", None, records()).valid


def test_holdout_goes_to_test(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.holdout_ids": [U937]}), "h", None, records())
    assert plan.rows_by_id[U937].splits == {"train": 0, "val": 0, "test": 25}


def test_no_dataset_admitted(spec):
    plan = build_plan(spec_with(spec, **{"select.paths": ["nowhere/**"]}), "h", None, records())
    assert plan.errors == ["no dataset admitted"]


def test_markdown_has_one_row_per_admitted_record_and_errors_first(spec, spec_2d):
    plan = build_plan(spec_with(spec, **{"select.where.annotations.source": "any"}), "h", "abc", records())
    text = plan.render_markdown()
    assert text.index("## Errors") < text.index("| id |")
    for record_id in F:
        assert text.count(f"| {record_id} |") == (1 if record_id in plan.rows_by_id else 0)
    assert f"## Excluded by selection ({sum(plan.excluded.values())})" in text
    for rule, count in plan.excluded.items():
        assert f"| {rule} | {count} |" in text
    assert f"({len([r for r in plan.rows if r.in_pool])} in the pool, {len(F)} seen)" in text
    valid = build_plan(spec_2d, "h", None, records())
    assert "## Errors" not in valid.render_markdown() and isinstance(valid, Plan)
    assert [row["id"] for row in valid.to_dict()["rows"]] == [
        row.id for row in sorted(valid.rows, key=lambda r: r.path)
    ]


def test_record_without_technical_is_a_row_not_a_crash(spec_2d):
    row = build_plan(
        spec_with(spec_2d, **{"select.where.license.allow_unknown": True}), "h", None, records()
    ).rows_by_id[DUP]
    assert (
        row.rule == "admitted" and row.fit == Fit(False, "no array shape", (1024, 1024), False) and row.positions == 0
    )


def _record(**overrides) -> dict:
    base = yaml.safe_load((CATALOG / "datasets/2D/TEM/synthetic-em-2d-1ch.yaml").read_text())
    base.update(overrides)
    return base


def test_markdown_escapes_record_text(spec_2d_mixed):
    evil = _record(id="evil | pool | admitted", doi=None, license={"spdx": "x\ny|z", "url": None})
    text = build_plan(spec_2d_mixed, "h", None, [*records(), ("datasets/2D/TEM/zz-evil.yaml", evil)]).render_markdown()
    assert "| license x y\\|z not in allowed | 1 |" in text  # the excluded-rule table escapes the record's text too
    assert "\n| pool |" not in text


def test_malformed_record_is_a_row_not_a_crash(spec_2d_mixed):
    bad = _record(id="bad-arrays", doi=None, technical={"arrays": ["nope"]})
    unreadable = {"id": "bad-yaml", "_unreadable": "ParserError: while parsing"}
    plan = build_plan(
        spec_2d_mixed,
        "h",
        None,
        [*records(), ("datasets/2D/TEM/zz-bad.yaml", bad), ("datasets/2D/TEM/zz-yaml.yaml", unreadable)],
    )
    assert plan.rows_by_id["bad-arrays"].rule.startswith("record unreadable: AttributeError")
    assert plan.rows_by_id["bad-yaml"].rule == "record unreadable: ParserError: while parsing"
    assert plan.valid is False or plan.valid is True  # the plan was built either way
    assert plan.n_seen == len(records()) + 2 and plan.to_dict()["n_seen"] == plan.n_seen


def test_positions_counts_depth_for_3d(spec):
    assert positions(F[HEMIBRAIN], spec) == 1 * (41394 // 32) * (39725 // 1024) * (34427 // 1024)


def test_assumed_voxel_size_admits_mitolab_unless_disabled(spec):
    small = spec_with(spec, **{"sampling.patch.2D": {"y": 64, "x": 64}, "sampling.levels": [1, 2]})
    assert fit(F[MITOLAB], small).reason == (
        "fits (shape varies: checked against the representative item; voxel size assumed for x, y)"
    )
    strict = spec_with(small, **{"sampling.assume_voxel_size": False})
    assert fit(F[MITOLAB], strict).reason == "voxel size unknown"


def test_plan_rows_carry_voxel_size_and_recipe(spec_2d_mixed):
    one = spec_with(spec_2d_mixed, **{"sampling.patch.c": 1})
    plan = build_plan(one, "h", None, records())
    assert plan.valid
    assert plan.rows_by_id[U937].voxel_size_nm == {"x": 45.0718, "y": 45.0718} and plan.rows_by_id[U937].assumed == []
    assert plan.rows_by_id[U937].channel_recipe == {"op": "mean"}
    assert plan.rows_by_id[U937].bytes == 1153470226  # the record's size_bytes, for the raw quota
    assert plan.rows_by_id[ONE_CH].channel_recipe == {"op": "channel", "index": 0}
    three = build_plan(spec_with(spec_2d_mixed, **{"sampling.patch.c": 3}), "h", None, records())
    assert three.valid and three.rows_by_id[ONE_CH].channel_recipe == {"op": "replicate"}
    assert three.rows_by_id[BCSS].channel_recipe == {"op": "rgb"}
    two = build_plan(spec_with(spec_2d_mixed, **{"sampling.patch.c": 2}), "h", None, records())
    assert two.errors == [f"patch.c: 2 needs at least 2 channels; ['{ONE_CH}'] have fewer"]
    assert "| 45.0718×45.0718 |" in plan.render_markdown()
    placeable = records()  # the fixture lists two modalities; this row is planned as one organism's FIB-SEM
    for _, record in placeable:
        if record["id"] == MITOLAB:
            record["imaging"].update(organism=["Mus musculus"], modality=["FIB-SEM"])
    mito = build_plan(
        spec_with(
            one,
            **{"sampling.patch.2D": {"y": 64, "x": 64}, "sampling.levels": [1, 2], "select.where.min_size_voxels": 0},
        ),
        "h",
        None,
        placeable,
    )
    assert "| 8×8 (assumed) |" in mito.render_markdown() and mito.rows_by_id[MITOLAB].assumed == ["x", "y"]


def test_policy_error_lists_are_capped(spec_2d_mixed):
    from mia_buffers.plan import _some

    assert _some(["a", "b"]) == ["a", "b"] and _some([str(i) for i in range(8)]) == [
        "0",
        "1",
        "2",
        "3",
        "4",
        "… 3 more",
    ]


def test_not_prepared_rows_are_out_with_their_rule(spec_2d):
    plan = build_plan(spec_2d, "h", None, records(), prepared={U937})
    rows = plan.rows_by_id
    assert rows[U937].in_pool and rows[BCSS].rule == NOT_PREPARED and not rows[BCSS].in_pool
    assert rows[BCSS].fit is not None and rows[BCSS].fit.ok and rows[BCSS].weight == 0.0 and rows[BCSS].splits == {}
    assert plan.valid and rows[U937].weight == 1.0
    assert "## Not prepared (1)" in plan.render_markdown()
    assert build_plan(spec_2d, "h", None, records()).rows_by_id[BCSS].in_pool  # None: rule not applied
    by_id = {row["id"]: row for row in plan.to_dict({U937: "prepared"})["rows"]}
    assert by_id[U937]["prepared"] == "prepared" and by_id[BCSS]["prepared"] == "unprepared"
    assert "prepared" not in plan.to_dict()["rows"][0]


def test_rows_carry_the_split_policy(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.holdout_ids": [U937]}), "h", None, records())
    fractions = {"train": 0.8, "val": 0.1, "test": 0.1}
    assert plan.rows_by_id[U937].split == {"policy": "holdout", "fractions": fractions, "seed": 0}
    assert plan.rows_by_id[BCSS].split["policy"] == "fractions" and plan.to_dict()["rows"][0]["split"]


def test_stratify_by_is_a_plan_error(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.stratify_by": ["modality"]}), "h", None, records())
    assert "splits.stratify_by is not implemented; remove it" in plan.errors


@pytest.fixture
def plan_2d(spec_2d) -> Plan:
    return build_plan(spec_2d, "h", None, records())


@pytest.fixture
def records_single_plane() -> list[tuple[str, dict]]:
    """The catalog with every record cut to one item and every raw array to one plane (t and z of 1): one plane
    lands in one split, so the expectation of val and test is under one sample."""
    single = records()
    for _, record in single:
        record.setdefault("data", {})["n_items"] = 1
        for array in (record.get("technical") or {}).get("arrays") or []:
            if array.get("role") == "raw" and array.get("axes") and array.get("shape"):
                sizes = zip(array["axes"], array["shape"], strict=True)
                array["shape"] = [1 if axis in "tz" else size for axis, size in sizes]
    return single


def _share(row, spec: Spec) -> dict[str, int]:
    """What `plan_rule` gives the row: its weight's share of each split's crops."""
    sampling = spec.sampling
    return {
        name: crops_for(row.weight, total, sampling.max_crops_per_dataset) for name, total in sampling.crops.items()
    }


def test_crops_for_is_the_share_rounded_up_to_a_power_of_two():
    assert crops_for(0.004, 1_000_000, 1 << 20) == 4096
    assert crops_for(0.0038, 1_000_000, 1 << 20) == 4096  # a pool that grew a little: the same count
    assert crops_for(0.003, 1000, 4096) == 4
    assert crops_for(0.0, 1000, 4096) == 1  # every dataset gets at least one
    assert crops_for(0.5, 1_000_000, 4096) == 4096  # the cap
    assert crops_for(0.5, 1_000_000, 3000) == 3000


def test_rows_carry_their_share_as_a_rule_and_digest(plan_2d, spec_2d):
    row = next(r for r in plan_2d.rows if r.in_pool)
    assert row.plan_rule is not None
    assert set(row.plan_rule) == {"seed", "patch", "levels", "samples", "split", "channel_recipe"}
    assert row.plan_rule["samples"] == _share(row, spec_2d) and "weight" not in row.plan_rule
    assert row.plan_digest == plan_digest(row.plan_rule) and len(row.plan_digest) == 64
    other = next(r for r in plan_2d.rows if r.in_pool and r.id != row.id)
    assert (row.plan_digest == other.plan_digest) == (row.plan_rule == other.plan_rule)
    assert row.rule == "admitted"  # the selection rule stays a string beside the sample rule


def test_rule_json_matches_the_harness_dump(plan_2d):
    row = plan_2d.rows_by_id[U937]
    counts = row.plan_rule["samples"]
    assert (
        row.plan_digest
        == hashlib.sha256(
            b'{"channel_recipe":null,"levels":[1,2,4],"patch":256,'
            + f'"samples":{{"test":{counts["test"]},"train":{counts["train"]},"val":{counts["val"]}}},'.encode()
            + b'"seed":0,"split":{"fractions":{"test":0.1,"train":0.8,"val":0.1},"policy":"fractions","seed":0}}'
        ).hexdigest()
    )


def test_planned_is_the_share_a_record_can_supply(plan_2d, spec_2d):
    fractions = spec_2d.splits.fractions
    bcss, u937 = plan_2d.rows_by_id[BCSS], plan_2d.rows_by_id[U937]
    # BCSS: 151 one-plane images, each whole in one split: every split all but surely holds one of them
    assert bcss.planned == bcss.plan_rule["samples"]
    # U937: 25 one-plane items (n_items and t count the same fields): a split holds none with (1 - fraction) ** 25
    assert u937.planned == {
        name: round(count * (1 - (1 - fractions[name]) ** 25)) for name, count in u937.plan_rule["samples"].items()
    }
    # up to 4 chunks per crop per level, never more than the record's whole pyramids (25 planes of 2048², 3 channels)
    crops = sum(u937.planned.values())
    whole = int(25 * 2048 * 2048 * 3 * 2 * (1 + 1 / 4 + 1 / 16))
    assert u937.planned_bytes == min(3 * crops * 4 * 256 * 256 * 3 * 2, whole)
    assert plan_2d.valid


def test_split_unsupplied_by_every_record_is_an_error(spec_2d, records_single_plane):
    # one record of one plane: it lands whole in one split, so 1 val crop is expected 0.1 times
    one = [(path, record) for path, record in records_single_plane if record["id"] == U937]
    spec = spec_with(spec_2d, **{"sampling.crops": {"train": 8, "val": 1, "test": 1}})
    plan = build_plan(spec, "h", None, one)
    assert any(e.startswith("split 'test': no record can supply it") for e in plan.errors)
    assert any(e.startswith("split 'val': no record can supply it") for e in plan.errors)
    assert not any(e.startswith("split 'train'") for e in plan.errors)


def test_a_pool_of_single_images_supplies_every_split(spec_2d_mixed, records_single_plane):
    # three one-plane records of 4 val crops each (the cap): each expects 0.4, which rounds to none, the pool 1.2
    edits = {"sampling.crops": {"train": 1000, "val": 100, "test": 100}, "sampling.max_crops_per_dataset": 4}
    plan = build_plan(spec_with(spec_2d_mixed, **edits), "h", None, records_single_plane)
    pool = [row for row in plan.rows if row.in_pool]
    assert len(pool) == 3 and all(row.planned["val"] == 0 for row in pool)
    assert not any(e.startswith("split ") for e in plan.errors)


def test_planned_cuts_planes_exactly_like_the_harness(spec_2d):
    # one 5-plane volume: split_counts(5, .8/.1/.1) gives train 4, val 1 (the tie), test 0 planes
    five_planes = [(path, record) for path, record in records() if record["id"] == U937]
    record = five_planes[0][1]
    record["data"]["n_items"] = 1
    record["technical"]["arrays"] = [{**record["technical"]["arrays"][0], "shape": [5, 3, 4096, 4096]}]
    plan = build_plan(spec_2d, "h", None, five_planes)
    row = plan.rows_by_id[U937]
    assert row.planned == {**row.plan_rule["samples"], "test": 0}
    assert any(e.startswith("split 'test': no record can supply it") for e in plan.errors)
    assert not any(e.startswith(("split 'train'", "split 'val'")) for e in plan.errors)


def test_plan_digest_golden():
    """The cross-repo pin: the harness's PlanRule.digest() of the same rule must give this hex."""
    rule = {
        "seed": 0,
        "patch": 256,
        "levels": [1, 2, 4],
        "samples": {"train": 1000, "val": 20, "test": 20},
        "split": {"policy": "fractions", "fractions": {"train": 0.8, "val": 0.1, "test": 0.1}, "seed": 0},
        "channel_recipe": None,
    }
    assert plan_digest(rule) == "2827e3d249deb0a8bb8ecc10cb579ce0f37c9a4d2667f04af2914388bc04ae74"


def test_raw_array_without_extents_counts_the_full_request(spec_2d):
    with_odd_array = records()
    for _, record in with_odd_array:
        if record["id"] == U937:
            record["technical"]["arrays"].append({"role": "raw", "axes": "t", "shape": [5]})
    row = build_plan(spec_2d, "h", None, with_odd_array).rows_by_id[U937]
    assert row.planned == row.plan_rule["samples"]  # an array with no shape may hold planes of every split
    assert UNKNOWN_SHAPE_NOTE in row.notes


def test_holdout_only_pool_cannot_supply_train(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.holdout_ids": [U937, BCSS]}), "h", None, records())
    row = plan.rows_by_id[U937]
    assert row.planned == {"train": 0, "val": 0, "test": row.plan_rule["samples"]["test"]}
    assert any(e.startswith("split 'train': no record can supply it") for e in plan.errors)


def test_a_heavy_dataset_stops_at_the_cap(spec_2d, plan_2d):
    capped = build_plan(spec_with(spec_2d, **{"sampling.max_crops_per_dataset": 8}), "h", None, records())
    pool = [row for row in capped.rows if row.in_pool]
    assert pool and all(max(row.plan_rule["samples"].values()) <= 8 for row in pool)
    assert any(row.plan_rule["samples"]["train"] == 8 for row in pool)
    # the cap leaves the weights alone: a capped dataset's crops are drawn more often
    assert {row.id: row.weight for row in pool} == {row.id: row.weight for row in plan_2d.rows if row.in_pool}


def test_the_plan_table_shows_each_datasets_crops_and_coverage(plan_2d, spec_2d):
    row = plan_2d.rows_by_id[U937]
    md = plan_2d.render_markdown()
    cell = " · ".join(f"{name} {row.planned[name]}" for name in spec_2d.sampling.crops)
    coverage = sum(row.planned.values()) / row.positions
    assert f"| {cell} | {coverage:.2g}× |" in md
    assert "| split preview | crops | coverage | est. bytes |" in md
    planned = sum(sum(r.planned.values()) for r in plan_2d.rows if r.in_pool)
    assert f"planned {planned} of requested {sum(spec_2d.sampling.crops.values())} crops; ~" in md
    assert "expectation over each record's items" in md
    assert plan_2d.crops == spec_2d.sampling.crops


def test_a_dataset_estimated_over_the_stage_cap_is_a_plan_warning(spec_2d, plan_2d):
    assert not any("limits.stage_bytes" in warning for warning in plan_2d.warnings)  # no cap given: nothing to say
    tight = build_plan(spec_2d, "h", None, records(), stage_bytes=1)
    over = [warning for warning in tight.warnings if "limits.stage_bytes" in warning]
    assert len(over) == 1 and over[0].startswith("2 datasets are estimated over limits.stage_bytes (1 B)")
    assert BCSS in over[0] and U937 in over[0] and "sampling.max_crops_per_dataset" in over[0]
    assert tight.valid  # a warning: the stage job measures the real bytes
    roomy = build_plan(spec_2d, "h", None, records(), stage_bytes=1e15)
    assert not any("limits.stage_bytes" in warning for warning in roomy.warnings)


FILTERS = [{"name": "content"}, {"name": "labelled"}]


def test_plan_digest_with_filters_golden():
    """The cross-repo pin: the harness's PlanRule.digest() of the same rule with these filters gives this hex."""
    rule = {
        "seed": 0,
        "patch": 256,
        "levels": [1, 2, 4],
        "samples": {"train": 1000, "val": 20, "test": 20},
        "split": {"policy": "fractions", "fractions": {"train": 0.8, "val": 0.1, "test": 0.1}, "seed": 0},
        "channel_recipe": None,
        "filters": FILTERS,
    }
    assert plan_digest(rule) == "543be40667f56b2ed78ee2da2472d6bf56a12649a3711589d3db493e1d5dbc5d"


def test_a_rule_has_filters_only_when_the_definition_does(spec_2d):
    plain = build_plan(spec_2d, "h", None, records()).rows_by_id[U937]
    assert "filters" not in plain.plan_rule
    filtered = build_plan(spec_with(spec_2d, **{"sampling.filters": FILTERS[::-1]}), "h", None, records())
    row = filtered.rows_by_id[U937]
    assert row.plan_rule["filters"] == FILTERS  # name order, whatever the definition's
    assert {**row.plan_rule, "filters": None} == {**plain.plan_rule, "filters": None}
    assert row.plan_digest != plain.plan_digest and row.planned == plain.planned


def test_planned_reads_up_to_with_filters(spec_2d):
    plain = build_plan(spec_2d, "h", None, records()).render_markdown()
    assert "up to" not in plain
    text = build_plan(spec_with(spec_2d, **{"sampling.filters": FILTERS}), "h", None, records()).render_markdown()
    assert "| up to train " in text and "\nplanned up to " in text
