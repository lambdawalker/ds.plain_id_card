from types import SimpleNamespace

import requests
import yaml
from lambdawaker.template.render_parallel import run_dispatcher


def fetch_dataset_size(base_url) -> int:
    try:
        url = f"{base_url}/ds/lambdaWalker/ds.photo_id/body/len"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        # Handle quoted strings from some API responses
        return int(resp.text.strip().strip('"').strip("'"))
    except Exception as e:
        raise Exception(f"Failed to fetch dataset size: {e}")


def read_config():
    with open("render.yaml", 'r') as f:
        return SimpleNamespace(
            **yaml.safe_load(f)
        )


def main():
    config = read_config()
    limit = fetch_dataset_size(config.base_url)
    run_dispatcher(limit, config)


if __name__ == "__main__":
    main()
