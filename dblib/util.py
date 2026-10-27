import os
import shutil
import subprocess
from os import walk
from os.path import exists, join

# Where the servers run when the driver is on another machine.
# DBFORK_SERVER_HOST is the default host every local adapter connects to
# (its own <BACKEND>_HOST variable still wins). DBFORK_SERVER_SSH
# ("user@host") makes the directory and volume measurements below run
# `du` and `df` over ssh on that machine, since the data directories are
# not on the driver's filesystem; the *_DATA_DIR paths are then paths on
# the server. Without it they are measured locally as before.
SERVER_SSH = os.environ.get("DBFORK_SERVER_SSH", "")


def server_host(default: str = "127.0.0.1") -> str:
    """Host local adapters connect to unless their own variable is set."""
    return os.environ.get("DBFORK_SERVER_HOST", default)


def ssh_target(var: str) -> str:
    """The ssh target for one backend's server: its own variable (e.g.
    DOLT_SSH) when set, else DBFORK_SERVER_SSH, else "" (local)."""
    return os.environ.get(var) or SERVER_SSH


def _ssh_int(target: str, cmd: list) -> int:
    out = subprocess.run(["ssh", "-o", "BatchMode=yes", target, "--", *cmd],
                         capture_output=True, text=True, timeout=600)
    if out.returncode != 0:
        raise RuntimeError(f"ssh {target} {' '.join(cmd)}: {out.stderr.strip()}")
    for tok in out.stdout.split():  # du: "<bytes>\t<path>", df: "Used\n<bytes>"
        if tok.isdigit():
            return int(tok)
    raise RuntimeError(f"ssh {target} {' '.join(cmd)}: no number in {out.stdout!r}")


def format_db_uri(
    user: str, password: str, host: str, port: int, db_name: str
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_directory_size_bytes(path: str, ssh: str = None) -> int:
    """Get physical disk usage of a directory using st_blocks.

    ``ssh`` (default DBFORK_SERVER_SSH) measures on that machine instead.

    Uses os.stat().st_blocks * 512 instead of os.path.getsize() to measure
    actual allocated disk blocks rather than logical file sizes. Tracks inodes
    to avoid double-counting hard-linked files.
    """
    ssh = SERVER_SSH if ssh is None else ssh
    if ssh:
        # du counts allocated blocks and hard links once, like the walk.
        try:
            return _ssh_int(ssh, ["du", "-s", "-B1", "--", path])
        except RuntimeError as e:
            if "No such file" in str(e):
                return 0
            raise
    if not exists(path):
        return 0
    total = 0
    seen_inodes = set()
    for dirpath, _, filenames in walk(path):
        for f in filenames:
            st = os.stat(join(dirpath, f))
            if st.st_ino not in seen_inodes:
                seen_inodes.add(st.st_ino)
                total += st.st_blocks * 512
    return total


def get_volume_usage_bytes(path: str, ssh: str = None) -> int:
    """Get physical disk usage of the filesystem/volume containing ``path``.

    Uses ``shutil.disk_usage(path).used`` which queries statvfs().
    On APFS this correctly accounts for CoW deduplication — cloned blocks
    are counted only once.

    IMPORTANT: Only meaningful when ``path`` is on an isolated volume
    dedicated to the database under test.
    """
    ssh = SERVER_SSH if ssh is None else ssh
    if ssh:
        try:
            return _ssh_int(ssh, ["df", "-B1", "--output=used", "--", path])
        except RuntimeError as e:
            if "No such file" in str(e):
                return 0
            raise
    if not exists(path):
        return 0
    return shutil.disk_usage(path).used
