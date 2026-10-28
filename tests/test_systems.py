"""System resources are complete and discoverable without connector imports."""

import json
import sys
from unittest import mock

import pytest

from dblib.systems import load_system, system_catalog, system_dir


def test_catalog_contains_complete_system_packages_without_importing_connectors() -> None:
    complete = {
        "chronos", "dolt", "dolt_mysql", "file_copy",
        "matrixone", "neon", "seekdb", "xata",
    }
    before = set(sys.modules)
    assert set(system_catalog()) == complete | {"tiger"}
    imported = set(sys.modules) - before
    assert not {name for name in imported if name.endswith(".connector")}
    for name in complete:
        directory = system_dir(name)
        assert (directory / "adapter.py").is_file()
        assert (directory / "connector.py").is_file()
        assert (directory / "README.md").is_file()


def test_engine_only_connectors_are_not_published_to_the_leaderboard() -> None:
    catalog = system_catalog()
    for name in ("bauplan", "terminusdb"):
        assert name not in catalog
        directory = system_dir(name)
        assert (directory / "adapter.py").is_file()
        assert (directory / "connector.py").is_file()
        assert (directory / "README.md").is_file()


def test_metadata_only_system_is_published_without_an_engine_connector() -> None:
    assert "tiger" in system_catalog()
    directory = system_dir("tiger")
    assert (directory / "system.json").is_file()
    assert (directory / "README.md").is_file()
    assert not (directory / "adapter.py").exists()
    assert not (directory / "connector.py").exists()


def test_self_hosted_system_requires_lifecycle_files(tmp_path) -> None:
    metadata = load_system("dolt")
    (tmp_path / "system.json").write_text(json.dumps(metadata))
    for name in ("adapter.py", "connector.py", "README.md", "start", "stop"):
        (tmp_path / name).touch()
    with mock.patch("dblib.systems.system_dir", return_value=tmp_path):
        with pytest.raises(ValueError, match="missing required files: check"):
            load_system("dolt")
