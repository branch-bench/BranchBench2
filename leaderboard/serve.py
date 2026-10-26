"""Serve the generated site of one phase on loopback; standard library only.

From the repository root:
    python -m leaderboard.serve                 # production preview of leaderboard/.site/prod
    python -m leaderboard.serve --phase alpha   # DAPLab AWS staging preview of leaderboard/.site/alpha
    python -m leaderboard.serve --phase local   # development preview with the Terminal launcher

All assemble the phase's site first (leaderboard.site), then serve only that
directory. Production serves index.html, styles.css, the two data files, the six
workflow diagram PNGs and the published raw records. It has no wizard, launcher routes,
README or hardware requirements. Alpha serves all validated records, architecture.html,
README.md and the hardware requirements without mock rows or launcher routes.
"""

import argparse
import json
import shlex
import subprocess
import sys
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

from leaderboard import site
from leaderboard.connector import system_catalog
from leaderboard.validate import BOARD, MACHINE_RE
from leaderboard.validate_macro import ATTEMPT_RE, CAMPAIGN_FILE_RE, PRESETS, SCENARIOS

ROOT = BOARD.parent
PYTHON = str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else sys.executable
STATIC = {phase: {"/", "/data.js", "/macro-data.js", *(f"/{rel}" for rel in (*site.PAGES[phase], *site.WORKFLOW_DIAGRAMS))} for phase in site.PHASES}
SUITES = ("micro", "macro")
BUILD = ["-m", "leaderboard.site", "--phase", "local"]  # the wizard only runs in the local phase


def macro_commands(config: dict, systems: list, machine: str, catalog: dict[str, dict]) -> list[str]:
    """One run_macro per system, then one build that also runs after a failed run,
    so a failed or incomplete campaign still reaches the page; the first failing
    run's exit status is the script's. Every campaign runs all six workflows."""
    preset = config.get("preset", "mini")
    if not isinstance(preset, str) or preset not in PRESETS:
        raise ValueError("Choose the Quick or Full workload.")
    if any(catalog[system]["hosted"] == "yes" for system in systems):
        raise ValueError("Agent workflows run only on local systems.")
    lines = ["", "# Run one database at a time and always rebuild this machine's leaderboard; the first failure's exit status is the script's.",
             "status=0", 'keep() { [ "$status" -ne 0 ] || status=$1; }']
    for system in systems:
        lines.append(shlex.join([PYTHON, "-m", "leaderboard.run_macro", system, "--preset", preset, "--machine", machine]) + " || keep $?")
    return lines + [shlex.join([PYTHON, *BUILD]) + " || keep $?", 'exit "$status"']


def script_for(config: dict) -> str:
    systems, machine, location = config.get("systems"), config.get("machine"), config.get("location", "")
    catalog = system_catalog(BOARD)
    runnable = system_catalog(BOARD, runnable=True)
    suite = config.get("suite", "micro")
    if not isinstance(suite, str) or suite not in SUITES:
        raise ValueError("Choose the microbench or the agent-workflow suite.")
    if (not isinstance(systems, list) or not 1 <= len(systems) <= len(catalog)
            or any(not isinstance(system, str) or system not in catalog for system in systems)
            or len(set(systems)) != len(systems)):
        raise ValueError("Choose one or more distinct supported databases.")
    if any(system not in runnable for system in systems):
        raise ValueError("A selected database has no runnable leaderboard connector.")
    if not isinstance(machine, str) or not MACHINE_RE.fullmatch(machine):
        raise ValueError("Use a valid machine name.")
    if not isinstance(location, str) or len(location) > 120 or any(ord(c) < 32 or ord(c) == 127 for c in location):
        raise ValueError("Use a valid client location.")
    keys = [key for system in systems for key in catalog[system]["credentials"]]
    if keys and not location.strip():
        raise ValueError("Enter the client location for managed services.")
    lines = ["#!/bin/bash", "set -e", f"cd {shlex.quote(str(ROOT))}"]
    if suite == "macro":
        return "\n".join(lines + macro_commands(config, systems, machine, catalog)) + "\n"
    if keys:
        lines += ["", "# Terminal prompts for missing credentials; input is hidden.",
                  f"for benchmark_key in {' '.join(keys)}; do",
                  '  if [ -z "${!benchmark_key:-}" ]; then',
                  '    read -r -s -p "$benchmark_key: " "$benchmark_key"',
                  "    printf '\\n'", "  fi", '  export "$benchmark_key"', "done"]
    lines += ["", "# Run one database at a time, then rebuild this machine's leaderboard from validated current-suite results."]
    for system in systems:
        args = [PYTHON, "-m", "leaderboard.run", system, "--machine", machine]
        if catalog[system]["hosted"] == "yes":
            args += ["--client-location", location.strip()]
        lines.append(shlex.join(args))
    lines += [shlex.join([PYTHON, *BUILD])]
    return "\n".join(lines) + "\n"


def open_terminal(script: str) -> None:
    if sys.platform != "darwin":
        raise ValueError("One-click Terminal launch is available on macOS. You can still copy the script.")
    # Script is data to AppleScript, and a single shell-quoted argument to bash.
    applescript = '''on run argv
tell application "Terminal"
    activate
    do script (item 1 of argv)
end tell
end run'''
    subprocess.run(["/usr/bin/osascript", "-e", applescript, "/bin/bash -c " + shlex.quote(script)],
                   check=True, capture_output=True, text=True, timeout=20)


class Handler(SimpleHTTPRequestHandler):
    phase = "prod"  # main() sets it; a request can never change it

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(site.SITE / self.phase), **kwargs)

    @property
    def origin(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "frame-ancestors 'none'")
        super().end_headers()

    def send_head(self):
        if self.headers.get("Host") != self.origin.removeprefix("http://"):
            self.send_error(403, "Use the local launcher URL.")
            return None
        path = unquote(urlsplit(self.path).path)
        parts = Path(path).parts[1:]
        result = (len(parts) == 4 and parts[0] in system_catalog(BOARD) and len(parts[2]) == 8 and parts[2].isdigit()
                  and ((parts[1] == "results" and parts[3].endswith(".json"))
                       or (parts[1] == "macro" and parts[3].endswith(".json")
                           and any(r.fullmatch(parts[3].removesuffix(".json")) for r in (ATTEMPT_RE, CAMPAIGN_FILE_RE)))))
        if ((path not in STATIC[self.phase] and not result)
                or not Path(self.translate_path(path)).resolve().is_relative_to((site.SITE / self.phase).resolve())):
            self.send_error(404)
            return None
        return super().send_head()

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.phase != "local":
            self.reply(404, {"error": "The launcher exists only in the local development phase."})
            return
        # No CORS: only this loopback origin can request a terminal launch.
        if (self.headers.get("Host") != self.origin.removeprefix("http://")
                or self.headers.get("Origin") != self.origin
                or self.headers.get("Content-Type") != "application/json"):
            self.reply(403, {"error": "Open the wizard from the local launcher URL."})
            return
        if self.path not in ("/api/preview", "/api/run"):
            self.reply(404, {"error": "Unknown launcher endpoint."})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 16384:
                raise ValueError("Invalid request size.")
            config = json.loads(self.rfile.read(length))
            if not isinstance(config, dict):
                raise ValueError("Invalid benchmark configuration.")
            script = script_for(config)
            if self.path == "/api/run":
                if config.get("script") != script:
                    raise ValueError("The script changed. Return to setup and review it again.")
                open_terminal(script)
                self.reply(200, {"message": "Opened Terminal. Follow the run there, then return to the leaderboard."})
            else:
                self.reply(200, {"script": script, "can_launch": sys.platform == "darwin"})
        except (ValueError, UnicodeError) as error:
            self.reply(400, {"error": str(error)})
        except (OSError, subprocess.SubprocessError):
            self.reply(503, {"error": "Could not open Terminal. Check macOS Automation access, or copy the script."})


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=site.PHASES, help="prod (default), alpha or local; $LEADERBOARD_PHASE otherwise")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    try:
        Handler.phase = site.phase(args.phase)
        target = site.assemble(Handler.phase)
    except site.SiteError as e:
        parser.exit(2, f"serve: {e}\n")
    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        url = f"http://127.0.0.1:{server.server_port}/"
        print(f"{Handler.phase} site from {target}: {url}", flush=True)
        if not args.no_browser:
            webbrowser.open(url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
