import socket
import unittest
from types import SimpleNamespace
from urllib.error import URLError
from unittest.mock import patch

import start


class FakeProcess:
    def __init__(self, statuses=(None,)):
        self.statuses = iter(statuses)
        self.last_status = None
        self.returncode = None
        self.pid = 12345

    def poll(self):
        try:
            self.last_status = next(self.statuses)
        except StopIteration:
            pass
        self.returncode = self.last_status
        return self.last_status


class StartTests(unittest.TestCase):
    def args(self, port=8000, timeout=1.0, workers=1):
        return SimpleNamespace(port=port, timeout=timeout, workers=workers)

    def launch(self, *args, **kwargs):
        with patch("start.ensure_port_available"):
            return start.run_launcher(*args, **kwargs)

    def test_wait_retries_network_and_invalid_responses_until_ready(self):
        process = FakeProcess()
        attempts = iter([URLError("not ready"), {}, {"templates": []}])
        calls = []

        def request_summary(url, timeout):
            calls.append((url, timeout))
            response = next(attempts)
            if isinstance(response, Exception):
                raise response
            return response

        start.wait_for_server(
            process,
            "http://127.0.0.1:8000",
            1.0,
            request_summary=request_summary,
            sleep=lambda _: None,
            clock=lambda: 0.0,
        )

        self.assertEqual(len(calls), 3)
        self.assertTrue(all(call[0] == "http://127.0.0.1:8000" for call in calls))

    def test_readiness_timeout(self):
        process = FakeProcess()
        times = iter([0.0, 0.1, 0.6])
        with self.assertRaisesRegex(TimeoutError, "did not become ready"):
            start.wait_for_server(
                process,
                "http://127.0.0.1:8000",
                0.5,
                request_summary=lambda *_: {},
                sleep=lambda _: None,
                clock=lambda: next(times),
            )

    def test_port_already_in_use_is_rejected(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind((start.DEFAULT_HOST, 0))
            port = listener.getsockname()[1]
            with self.assertRaisesRegex(RuntimeError, "already in use"):
                start.ensure_port_available(port)

    def test_launcher_waits_before_starting_renderer_and_cleans_up(self):
        server = FakeProcess()
        renderer = FakeProcess((0,))
        events = []
        processes = []

        def popen(command, **kwargs):
            events.append("server" if "uvicorn" in command else "renderer")
            process = server if "uvicorn" in command else renderer
            processes.append((command, kwargs, process))
            return process

        def request_summary(url, timeout):
            events.append("ready-check")
            return {"templates": []} if events.count("ready-check") == 2 else {}

        stopped = []
        result = self.launch(
            self.args(port=8010, workers=2),
            popen=popen,
            request_summary=request_summary,
            sleep=lambda _: None,
            clock=lambda: 0.0,
            stop_process=stopped.append,
        )

        self.assertEqual(result, 0)
        self.assertEqual(events, ["server", "ready-check", "ready-check", "renderer"])
        self.assertEqual(stopped, [renderer, server])
        server_command = processes[0][0]
        renderer_command, renderer_options, _ = processes[1]
        self.assertEqual(renderer_command[-1], "render.py")
        self.assertEqual(server_command[server_command.index("--workers") + 1], "2")
        self.assertEqual(renderer_options["env"]["DS_PLAIN_ID_BASE_URL"], "http://127.0.0.1:8010")
        self.assertEqual(renderer_options["cwd"], str(start.PROJECT_ROOT))

    def test_server_exit_before_readiness_prevents_renderer_start(self):
        server = FakeProcess((4,))
        started = []

        with self.assertRaisesRegex(RuntimeError, "before becoming ready"):
            self.launch(
                self.args(),
                popen=lambda command, **kwargs: started.append(command) or server,
                request_summary=lambda *_: {"templates": []},
                stop_process=lambda process: None,
            )

        self.assertEqual(len(started), 1)

    def test_renderer_exit_status_is_propagated(self):
        server = FakeProcess()
        renderer = FakeProcess((7,))
        started = []

        result = self.launch(
            self.args(),
            popen=lambda command, **kwargs: started.append(command) or (server if "uvicorn" in command else renderer),
            request_summary=lambda *_: {"templates": []},
            sleep=lambda _: None,
            clock=lambda: 0.0,
            stop_process=lambda process: None,
        )

        self.assertEqual(result, 7)
        self.assertEqual(len(started), 2)

    def test_server_exit_while_rendering_is_reported_and_cleaned(self):
        server = FakeProcess((None, 5))
        renderer = FakeProcess((None,))
        stopped = []

        with self.assertRaisesRegex(RuntimeError, "while rendering"):
            self.launch(
                self.args(),
                popen=lambda command, **kwargs: server if "uvicorn" in command else renderer,
                request_summary=lambda *_: {"templates": []},
                sleep=lambda _: None,
                clock=lambda: 0.0,
                stop_process=stopped.append,
            )

        self.assertEqual(stopped, [renderer, server])

    def test_interruption_during_readiness_cleans_up_server(self):
        server = FakeProcess()
        stopped = []

        with self.assertRaises(KeyboardInterrupt):
            self.launch(
                self.args(),
                popen=lambda command, **kwargs: server,
                request_summary=lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()),
                stop_process=stopped.append,
            )

        self.assertEqual(stopped, [server])


if __name__ == "__main__":
    unittest.main()
