from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, Event, get_ident
from unittest import TestCase
from unittest.mock import patch

from backend import exporter


class ParallelExportTest(TestCase):
    def test_pause_drains_parallel_renders_and_resume_reuses_published_instances(self):
        with TemporaryDirectory() as directory, patch.dict("os.environ", {"CARD_EXPORT_WORKERS": "2"}):
            folder = Path(directory)
            service = exporter.Exporter(None, None, None, folder, lambda: folder)
            session = {"completed": 0, "count": 4, "layouts": [{"stem": "front"}]}
            stop = Event()
            started = Barrier(2, timeout=5)
            second_published = Event()
            checkpoints = []
            rendered = []

            @contextmanager
            def renderer(_origin):
                yield get_ident()

            def render(owner, _session, index, stage):
                self.assertEqual(owner, get_ident())
                rendered.append(index)
                if index < 2 and not stop.is_set():
                    started.wait()
                if index == 0:
                    self.assertTrue(second_published.wait(5))
                for filename in exporter.expected_files(session["layouts"]):
                    (stage / filename).write_text("ready", encoding="utf-8")

            # The second result is published first. Pausing then must drain the first render
            # and must not report progress past the gap.
            original_rename = Path.rename

            def rename(path, target):
                result = original_rename(path, target)
                if target.name == "000002":
                    self.assertEqual(checkpoints, [])
                    stop.set()
                    second_published.set()
                return result

            with patch.object(exporter, "renderer", renderer), patch.object(exporter, "render_instance", render), \
                 patch.object(service, "update_session", side_effect=lambda key, **changes: checkpoints.append(changes["completed"])):
                with patch.object(Path, "rename", rename):
                    self.assertEqual(service.render_batch(1, "", session, folder, stop), 2)
                self.assertEqual(sorted(rendered), [0, 1])
                self.assertEqual(checkpoints[-1], 2)
                # Recover a later result already on disk without rendering it again.
                (folder / "000002").rename(folder / "000003")
                self.assertEqual(service.render_batch(1, "", session, folder, Event()), 4)
                self.assertEqual(sorted(rendered), [0, 1, 1, 3])
