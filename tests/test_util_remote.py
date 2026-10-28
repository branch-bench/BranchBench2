"""Storage measurement over ssh when the servers run on another machine
(DBFORK_SERVER_SSH), and the DBFORK_SERVER_HOST fallback."""

import importlib
import subprocess

from dblib import util as dbutil


class _Done:
    def __init__(self, out, rc=0, err=""):
        self.stdout, self.returncode, self.stderr = out, rc, err


def test_directory_size_runs_du_over_ssh(monkeypatch):
    calls = []
    monkeypatch.setattr(dbutil, "SERVER_SSH", "ubuntu@10.0.0.5")
    monkeypatch.setattr(subprocess, "run",
                        lambda cmd, **kw: calls.append(cmd) or _Done("123456\t/srv/data\n"))
    assert dbutil.get_directory_size_bytes("/srv/data") == 123456
    assert calls[0][:4] == ["ssh", "-o", "BatchMode=yes", "ubuntu@10.0.0.5"]
    assert calls[0][-4:] == ["du", "-s", "-B1", "--", "/srv/data"][-4:]


def test_volume_usage_runs_df_over_ssh(monkeypatch):
    monkeypatch.setattr(dbutil, "SERVER_SSH", "ubuntu@10.0.0.5")
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: _Done("Used\n99\n"))
    assert dbutil.get_volume_usage_bytes("/mnt/pgbench") == 99


def test_missing_remote_path_is_zero(monkeypatch):
    monkeypatch.setattr(dbutil, "SERVER_SSH", "ubuntu@10.0.0.5")
    monkeypatch.setattr(subprocess, "run",
                        lambda cmd, **kw: _Done("", 1, "du: cannot access '/x': No such file or directory"))
    assert dbutil.get_directory_size_bytes("/x") == 0


def test_local_measurement_without_ssh(monkeypatch, tmp_path):
    monkeypatch.setattr(dbutil, "SERVER_SSH", "")
    (tmp_path / "f").write_bytes(b"x" * 8192)
    assert dbutil.get_directory_size_bytes(str(tmp_path)) >= 8192
    assert dbutil.get_volume_usage_bytes(str(tmp_path)) > 0


def test_server_host_fallback(monkeypatch):
    monkeypatch.setenv("DBFORK_SERVER_HOST", "10.0.0.5")
    monkeypatch.delenv("SEEKDB_HOST", raising=False)
    from dblib.systems.seekdb import adapter as seekdb
    importlib.reload(seekdb)
    assert seekdb.SEEKDB_HOST == "10.0.0.5"
    monkeypatch.setenv("SEEKDB_HOST", "127.0.0.1")
    importlib.reload(seekdb)
    assert seekdb.SEEKDB_HOST == "127.0.0.1"
    monkeypatch.delenv("DBFORK_SERVER_HOST")
    monkeypatch.delenv("SEEKDB_HOST")
    importlib.reload(seekdb)


def test_per_backend_ssh_target_overrides_global(monkeypatch):
    monkeypatch.setattr(dbutil, "SERVER_SSH", "ubuntu@10.0.0.5")
    monkeypatch.setenv("SEEKDB_SSH", "ubuntu@10.0.0.7")
    monkeypatch.delenv("MO_SSH", raising=False)
    assert dbutil.ssh_target("SEEKDB_SSH") == "ubuntu@10.0.0.7"
    assert dbutil.ssh_target("MO_SSH") == "ubuntu@10.0.0.5"
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: calls.append(cmd) or _Done("5\t/d\n"))
    dbutil.get_directory_size_bytes("/d", ssh=dbutil.ssh_target("SEEKDB_SSH"))
    assert calls[0][3] == "ubuntu@10.0.0.7"
    monkeypatch.setattr(dbutil, "SERVER_SSH", "")
    assert dbutil.get_directory_size_bytes("/nonexistent-dir-xyz", ssh="") == 0


def test_chronos_and_file_copy_hosts_can_differ(monkeypatch):
    monkeypatch.setenv("PGSQL_HOST", "10.0.0.5")
    monkeypatch.setenv("CHRONOS_HOST", "10.0.0.6")
    monkeypatch.setenv("FILE_COPY_HOST", "10.0.0.7")
    from dblib.systems.chronos import adapter as chronos
    from dblib.systems.file_copy import adapter as file_copy
    importlib.reload(chronos); importlib.reload(file_copy)
    assert chronos.PGSQL_HOST == "10.0.0.6" and file_copy.PGSQL_HOST == "10.0.0.7"
    for v in ("CHRONOS_HOST", "FILE_COPY_HOST", "PGSQL_HOST"):
        monkeypatch.delenv(v)
    importlib.reload(chronos); importlib.reload(file_copy)
