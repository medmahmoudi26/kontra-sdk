from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from kontra.contract import OfferingError, OfferingSpec, load_offering, parse_offering
from kontra.contract.names import OfferingId, Version

BASE: dict[str, Any] = {
    "name": "acme/enrich",
    "version": "1.2.0",
    "description": "Resolve a URL list to company records",
    "max_duration": "6h",
}


def parse(**changes: Any) -> OfferingSpec:
    data = {**BASE, **changes}
    return parse_offering({k: v for k, v in data.items() if v is not ...})


def refused(path: str, **changes: Any) -> OfferingError:
    with pytest.raises(OfferingError) as info:
        parse(**changes)
    assert info.value.path == path, info.value
    return info.value


def test_load_the_prd_example(enrich_spec: OfferingSpec) -> None:
    s = enrich_spec
    assert s.name == "acme/enrich"
    assert s.version == "1.2.0"
    assert s.description == "Resolve a URL list to company records"
    assert s.pricing_hint == "$0.001 per url"
    assert s.tags == ["companies", "enrichment"]  # sorted: one spelling per fact
    assert s.max_duration == timedelta(hours=6)
    assert s.review_window == timedelta(hours=24)
    assert s.max_concurrent_jobs == 10
    assert s.max_output_bytes == 5_000_000_000
    assert s.artifact_retention == timedelta(days=7)
    assert s.offering_id == OfferingId("acme", "enrich", Version(1, 2, 0))


def test_canonical_facts(enrich_spec: OfferingSpec) -> None:
    assert enrich_spec.canonical() == {
        "name": "acme/enrich",
        "version": "1.2.0",
        "description": "Resolve a URL list to company records",
        "pricing_hint": "$0.001 per url",
        "tags": ["companies", "enrichment"],
        "max_duration": "6h",
        "review_window": "1d",
        "max_concurrent_jobs": 10,
        "max_output_bytes": "5GB",
        "artifact_retention": "7d",
    }


def test_defaults() -> None:
    s = parse()
    assert s.pricing_hint is None
    assert s.tags == []
    assert s.review_window == timedelta(hours=24)
    assert s.max_concurrent_jobs is None
    assert s.max_output_bytes == 1_000_000_000
    assert s.artifact_retention == timedelta(days=7)
    assert s.uses_hosted_model is False
    assert "uses_hosted_model" not in s.canonical()  # false is absent: one spelling


def test_uses_hosted_model_is_a_fact_only_when_true() -> None:
    assert parse(uses_hosted_model=True).canonical()["uses_hosted_model"] is True
    assert "uses_hosted_model" not in parse(uses_hosted_model=False).canonical()


def test_lenient_spellings_are_accepted() -> None:
    s = parse(max_duration="1h30m", max_output_bytes="1024MiB", review_window="0s")
    assert s.max_duration == timedelta(minutes=90)
    assert s.max_output_bytes == 1024**3
    assert s.review_window == timedelta(0)  # accept on delivery


def test_sizes_may_be_bytes_and_durations_may_be_timedeltas() -> None:
    s = parse(max_output_bytes=1234, max_duration=timedelta(minutes=5))
    assert s.max_output_bytes == 1234
    assert s.max_duration == timedelta(minutes=5)


@pytest.mark.parametrize("key", ["workflow", "quote", "activities", "task_queue", "handler"])
def test_keys_that_bind_code_point_at_worker_py(key: str) -> None:
    err = refused(key, **{key: "workflows.EnrichFlow"})
    assert "worker.py" in err.message


def test_unknown_keys_are_refused() -> None:
    err = refused("colour", colour="blue")
    assert "offering.yaml holds" in err.message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("description", "workflows.EnrichFlow"),
        ("description", "quote:quote"),
        ("pricing_hint", "acme_enrich.pricing:price_for"),
    ],
)
def test_values_that_look_like_import_paths_are_refused(field: str, value: str) -> None:
    err = refused(field, **{field: value})
    assert "import path" in err.message


def test_prose_is_not_mistaken_for_an_import_path() -> None:
    s = parse(description="Resolves e.g. acme.com to records. Version 1.2 adds retries.")
    assert s.description.startswith("Resolves")


@pytest.mark.parametrize(
    ("value", "fragment"),
    [
        (1.2, 'quote it: version: "1.2.0"'),
        ("1.2", "MAJOR.MINOR.PATCH"),
        ("v1.2.0", "MAJOR.MINOR.PATCH"),
        ("1.2.0\n", "MAJOR.MINOR.PATCH"),
    ],
)
def test_version(value: Any, fragment: str) -> None:
    assert fragment in refused("version", version=value).message


@pytest.mark.parametrize(
    "value",
    [
        "Acme/enrich",
        "acme",
        "acme/enrich@1.2.0",
        "kontra/enrich",
        "acme/en_rich",
        "acme/enrich\n",
        7,
    ],
)
def test_name(value: Any) -> None:
    refused("name", name=value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_duration", "31d"),
        ("max_duration", "0s"),
        ("max_duration", 3600),
        ("max_duration", "6 hours"),
        ("review_window", "31d"),
        ("artifact_retention", "91d"),
        ("artifact_retention", "0s"),
        ("max_output_bytes", "6TiB"),
        ("max_output_bytes", -1),
        ("max_output_bytes", True),
        ("max_output_bytes", "5gb"),
        ("max_concurrent_jobs", 0),
        ("max_concurrent_jobs", 10_001),
        ("max_concurrent_jobs", True),
        ("max_concurrent_jobs", "10"),
        ("description", ""),
        ("description", "x" * 2001),
        ("description", "bell\x07"),
        ("pricing_hint", "two\nlines"),
        ("pricing_hint", ""),
        ("tags", "enrichment"),
        ("tags", ["Enrichment"]),
        ("tags", ["a", "a"]),
        ("tags", [f"t{i}" for i in range(17)]),
        ("uses_hosted_model", "yes"),
        ("uses_hosted_model", 1),
        ("uses_hosted_model", None),
    ],
)
def test_bounds_and_shapes(field: str, value: Any) -> None:
    refused(field, **{field: value})


def test_missing_required_key() -> None:
    err = refused("max_duration", max_duration=...)
    assert err.message == "is required"


def test_every_error_is_reported() -> None:
    with pytest.raises(OfferingError) as info:
        parse_offering({"name": "Bad", "version": "x", "description": "ok", "max_duration": "1y"})
    assert [p for p, _ in info.value.errors] == ["name", "version", "max_duration"]


def test_load_reports_the_file(tmp_path: Path) -> None:
    p = tmp_path / "offering.yaml"
    p.write_text(
        "name: acme/enrich\nversion: 1.2.0\ndescription: d\nmax_duration: 6h\nworkflow: EnrichFlow\n"
    )
    with pytest.raises(OfferingError) as info:
        load_offering(p)
    assert info.value.path == "workflow"
    assert str(p) in str(info.value)


def test_load_refuses_a_list(tmp_path: Path) -> None:
    p = tmp_path / "offering.yaml"
    p.write_text("- name: acme/enrich\n")
    with pytest.raises(OfferingError, match="mapping"):
        load_offering(p)


def test_load_refuses_bad_yaml(tmp_path: Path) -> None:
    p = tmp_path / "offering.yaml"
    p.write_text("name: acme/enrich\nname: acme/other\n")
    with pytest.raises(OfferingError, match="duplicate key"):
        load_offering(p)


def test_load_missing_file(tmp_path: Path) -> None:
    with pytest.raises(OfferingError, match="does not exist"):
        load_offering(tmp_path / "nope.yaml")


def test_spec_is_frozen(enrich_spec: OfferingSpec) -> None:
    with pytest.raises(ValueError, match="frozen"):
        enrich_spec.name = "acme/other"  # type: ignore[misc]


@pytest.mark.parametrize("field", ["max_duration", "review_window", "artifact_retention"])
def test_absurd_durations_are_offering_errors(field: str) -> None:
    """``9999999999d`` overflowed timedelta and escaped as OverflowError."""
    data = {"name": "acme/x", "version": "1.0.0", "description": "d", "max_duration": "1h"}
    with pytest.raises(OfferingError) as info:
        parse_offering({**data, field: "9999999999d"})
    assert info.value.path == field
