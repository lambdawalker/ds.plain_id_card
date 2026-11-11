import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
POLL_INTERVAL = 0.25
REQUEST_TIMEOUT = 2.0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Start the template server and render the configured dataset."
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Local server port (default: 8000)")
    parser.add_argument("--workers", type=int, default=1, help="Uvicorn worker count (default: 1)")
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for the template server to become ready (default: 60)",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0")
    return args


def ensure_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        try:
            listener.bind((DEFAULT_HOST, port))
        except OSError as error:
            raise RuntimeError(f"Port {port} is already in use on {DEFAULT_HOST}.") from error


def fetch_summary(base_url, timeout):
    with urllib.request.urlopen(f"{base_url}/summary", timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def is_valid_summary(summary):
    return isinstance(summary, dict) and isinstance(summary.get("templates"), list)


def wait_for_server(process, base_url, timeout, request_summary=fetch_summary, sleep=time.sleep, clock=time.monotonic):
    deadline = clock() + timeout
    while True:
        if process.poll() is not None:
            raise RuntimeError(
                f"Template server exited with status {process.returncode} before becoming ready."
            )

        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError(f"Template server did not become ready within {timeout:g} seconds.")

        try:
            summary = request_summary(base_url, min(REQUEST_TIMEOUT, remaining))
            if is_valid_summary(summary):
                return
        except Exception:
            pass

        if process.poll() is not None:
            raise RuntimeError(
                f"Template server exited with status {process.returncode} before becoming ready."
            )

        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError(f"Template server did not become ready within {timeout:g} seconds.")
        sleep(min(POLL_INTERVAL, remaining))


def process_group_options():
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def stop_process_tree(process):
    if os.name == "nt":
        if process.poll() is None:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run_launcher(args, popen=subprocess.Popen, request_summary=fetch_summary, sleep=time.sleep,
                 clock=time.monotonic, stop_process=stop_process_tree):
    ensure_port_available(args.port)
    base_url = f"http://{DEFAULT_HOST}:{args.port}"
    server = None
    renderer = None
    process_options = process_group_options()
    try:
        server = popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "template_app:app",
                "--host",
                DEFAULT_HOST,
                "--port",
                str(args.port),
                "--workers",
                str(args.workers),
            ],
            cwd=str(PROJECT_ROOT),
            **process_options,
        )
        wait_for_server(server, base_url, args.timeout, request_summary, sleep, clock)

        renderer_environment = os.environ.copy()
        renderer_environment["DS_PLAIN_ID_BASE_URL"] = base_url
        renderer = popen(
            [sys.executable, "render.py"],
            cwd=str(PROJECT_ROOT),
            env=renderer_environment,
            **process_options,
        )

        while True:
            renderer_status = renderer.poll()
            if renderer_status is not None:
                return renderer_status
            if server.poll() is not None:
                raise RuntimeError(
                    f"Template server exited with status {server.returncode} while rendering."
                )
            sleep(POLL_INTERVAL)
    finally:
        if renderer is not None:
            stop_process(renderer)
        if server is not None:
            stop_process(server)


def main(argv=None):
    args = parse_args(argv)
    try:
        return run_launcher(args)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except (RuntimeError, TimeoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
