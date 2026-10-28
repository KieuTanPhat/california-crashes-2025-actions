"""Capture public completed run pages on a GitHub runner, without credentials."""
import json
from pathlib import Path
from playwright.sync_api import sync_playwright

statuses = [json.loads(Path(f'.runtime/proof/{key}-status.json').read_text()) for key in ('first', 'second')]
captured = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
    page = browser.new_page(viewport={'width': 1500, 'height': 1100})
    for key, status in zip(('first', 'second'), statuses):
        response = page.goto(status['url'], wait_until='domcontentloaded', timeout=60000)
        if not response or response.status != 200:
            raise RuntimeError('Public GitHub workflow page is not available for screenshot')
        page.get_by_text('California 2025', exact=False).first.wait_for(timeout=30000)
        page.wait_for_timeout(2000)
        target = f'github-{key}-full-success.png'
        page.screenshot(path='artifacts/proof/' + target, full_page=True)
        captured.append({'path': target, 'url': status['url'], 'workflow_conclusion_from_api': status['conclusion']})
    browser.close()
Path('artifacts/proof/github-screenshots.json').write_text(json.dumps(captured, indent=2))
