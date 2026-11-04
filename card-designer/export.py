"""Screenshot a card.html downloaded from the designer."""
import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("card", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.goto(args.card.resolve().as_uri())
        card = page.frame_locator("iframe").locator("#view-port")
        card.screenshot(path=str(args.output), animations="disabled")
        browser.close()


if __name__ == "__main__":
    main()
