import json
import multiprocessing.connection
import os.path
from typing import Dict, Any

import requests
from lambdawalker.pullman.pullman import BaseWorker, Orchestrator
from lambdawalker.template.render.CardRenderer import CardRenderer

base_url = "http://127.0.0.1:8000"


class MyWorker(BaseWorker):
    def __init__(self, pipe: multiprocessing.connection.Connection, worker_id: int, blackboard: Dict[str, Any]):
        super().__init__(pipe, worker_id, blackboard)
        self.card_renderer = CardRenderer(
            base_url=base_url,
            log=self.log,
            report_progress=self.report_progress,
        )

    def setup(self):
        self.log("Starting card renderer")
        self.card_renderer.start()

    def teardown(self):
        self.log("Closing card renderer")
        self.card_renderer.close()

    def work(self, payload):
        record_id = payload.get("record_id")
        photo_id = payload.get("photo_id")
        template = payload.get("template")

        self.log(f"Rendering {template}/{record_id}")

        if template is None or record_id is None:
            payload = json.dumps(payload)
            self.log(f"Failed to render card for record {record_id} with template {template}. Payload:\n{payload}")
            raise ValueError("Missing required fields")

        self.card_renderer.render_single_card(
            record_id,
            photo_id,
            template
        )

        self.log("Card rendered successfully.")

        self.report_progress(1)


def fetch_dataset_parameters(base_url):
    try:
        url = f"{base_url}/summary"
        respx = requests.get(url, timeout=10)
        respx.raise_for_status()
        return respx.json()
    except Exception as e:
        raise Exception(f"Failed to fetch dataset size: {e}")


def main():
    record_id = 0
    summary = fetch_dataset_parameters(base_url)

    tasks = []

    for template in summary["templates"]:
        for i in range(template["samples"]):
            task_id = os.path.join(template["name"], str(record_id)).replace("/", "-").replace("\\", "-")

            task = {
                "id": task_id,
                "payload": {
                    "record_id": record_id,
                    "photo_id": record_id % 10000,
                    "template": template["name"],
                }
            }
            record_id += 1
            tasks.append(task)

    print(f"Running {len(tasks)} tasks in parallel. ")

    orch = Orchestrator(
        tasks=tasks,
        worker_class=MyWorker,
        worker_scale=.6,
        session_id="render",
        force=True
    )

    orch.run()


if __name__ == "__main__":
    main()
