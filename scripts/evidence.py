"""Generate an evidence report from actual stage outputs and capture live service UIs."""
import csv
import html
import json
import os
import time
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from playwright.sync_api import sync_playwright

OUT = Path('artifacts')
EVIDENCE = OUT / 'evidence'
EVIDENCE.mkdir(parents=True, exist_ok=True)
TITLES = ['Chuẩn bị HDFS', 'Upload dữ liệu', 'Đọc bằng Spark', 'Làm sạch dữ liệu',
          'Spark SQL', 'RFM điều chỉnh & K-Means', 'Lưu kết quả HDFS', 'Kafka ingest', 'Airflow DAG & kiểm chứng']

def load(path, default=None):
    p = Path(path)
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else default

def table(rows, limit=30):
    if not rows:
        return '<p>Chưa có kết quả.</p>'
    keys = list(rows[0])
    cells = lambda row: ''.join('<td>' + html.escape(str(row.get(k, ''))) + '</td>' for k in keys)
    return '<div class="scroll"><table><tr>' + ''.join('<th>' + html.escape(k) + '</th>' for k in keys) + '</tr>' + ''.join('<tr>' + cells(r) + '</tr>' for r in rows[:limit]) + '</table></div>'

def chart(rows, key, name, title, horizontal=False):
    if not rows:
        return
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11})
    fig, ax = plt.subplots(figsize=(11, 4.8), layout='constrained')
    labels = [str(r[key]) for r in rows]
    values = [r['crashes'] for r in rows]
    if horizontal:
        ax.barh(labels[::-1], values[::-1], color='#305bd6')
        ax.set_xlabel('Recorded crashes')
    else:
        ax.bar(labels, values, color='#305bd6')
        ax.set_ylabel('Recorded crashes')
    ax.set_title(title, loc='left', pad=14, fontweight='bold')
    ax.spines[['top', 'right']].set_visible(False)
    ax.grid(axis='x' if horizontal else 'y', alpha=.15)
    ax.set_axisbelow(True)
    fig.savefig(OUT / 'results' / name, dpi=160)
    plt.close(fig)

monthly = load(OUT / 'results/monthly.json', [])
chart(monthly, 'month', 'monthly.png', 'California 2025 · Recorded crashes by month')
chart(load(OUT / 'results/hourly.json', []), 'hour', 'hourly.png', 'California 2025 · Recorded crashes by local hour')
chart(load(OUT / 'results/collision_types.json', [])[:8], 'collision_type', 'collision-types.png', 'California 2025 · Collision types', horizontal=True)
rfm = load(OUT / 'results/rfm_clusters.json', [])
clustered = [r for r in rfm if r['cluster'] is not None]
if clustered:
    fig, ax = plt.subplots(figsize=(10, 5.5), layout='constrained')
    points = ax.scatter([r['f'] for r in clustered], [r['m_observed'] for r in clustered], c=[r['cluster'] for r in clustered], cmap='tab10', alpha=.8, s=38)
    ax.set_xscale('log')
    ax.set_yscale('symlog', linthresh=1)
    ax.set_xlabel('F · Number of recorded crashes (log scale)')
    ax.set_ylabel('M · Observed casualties in complete records')
    ax.set_title('Geographic groups · Adjusted RFM and Spark ML K-Means', loc='left')
    fig.colorbar(points, ax=ax, label='Arbitrary cluster ID')
    fig.savefig(OUT / 'results/clusters.png', dpi=160)
    plt.close(fig)

manifest = load(OUT / 'manifest/run.json', {})
input_meta = load(OUT / 'manifest/input.json', {})
airflow_run = load(OUT / 'manifest/airflow-run.json', {})
airflow_tasks = load(OUT / 'manifest/airflow-tasks.json', [])
mode = os.environ.get('LAB_MODE', 'unknown')
run_url = f'https://github.com/{os.environ.get("GITHUB_REPOSITORY", "")}/actions/runs/{os.environ.get("GITHUB_RUN_ID", "")}'
sections = []
for i, title in enumerate(TITLES, 1):
    data = load(EVIDENCE / f'step_{i:02d}.json', {'status': 'not_completed'})
    body = '<pre>' + html.escape(json.dumps(data, indent=2, ensure_ascii=False)) + '</pre>'
    if i == 3 and data.get('result'):
        body = table(data['result'].get('sample', [])) + body
    if i == 5:
        body = '<img src="../results/monthly.png"><img src="../results/hourly.png">' + table(monthly) + body
    if i == 6:
        body = '<img src="../results/clusters.png">' + table(load(OUT / 'results/cluster_summary.json', [])) + body
    if i == 9:
        body = table([{'task': t['task_id'], 'state': t['state'], 'duration_seconds': t.get('duration')} for t in airflow_tasks]) + body
    status = data['status']
    sections.append(f'<section id="step-{i}"><div class="eyebrow">BƯỚC {i:02d} / 09 · {html.escape(status.upper())}</div><h2>{html.escape(title)}</h2>{body}</section>')

css = '''body{margin:0;background:#f1f4f9;color:#142440;font-family:Arial,sans-serif;line-height:1.5}main{max-width:1180px;margin:36px auto;padding:0 24px}header,section{background:white;border:1px solid #dce3ef;border-radius:12px;padding:30px;margin-bottom:24px}h1{font-size:34px;margin:8px 0}h2{font-size:27px;margin:8px 0 22px}.eyebrow{font-size:12px;font-weight:bold;letter-spacing:1px;color:#305bd6}pre{background:#f5f7fb;padding:20px;white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px;max-height:650px;overflow:hidden}table{border-collapse:collapse;font-size:12px;width:100%}td,th{padding:9px;border-bottom:1px solid #dce3ef;text-align:left}th{background:#edf2ff}.scroll{overflow:auto}img{max-width:100%;margin:12px 0}.note{background:#fff7df;padding:15px;border-left:4px solid #e3ac26}a{color:#305bd6}footer{padding:12px;color:#53637f}'''
row_label = f"{input_meta['selected_rows']:,}" if 'selected_rows' in input_meta else 'unavailable'
intro = f'''<header><div class="eyebrow">CALIFORNIA CRASHES · 2025 · GITHUB ACTIONS</div><h1>Minh chứng thực thi pipeline 9 bước</h1><p>Chế độ: <b>{html.escape(mode)}</b> · Bản ghi batch: <b>{row_label}</b> · Airflow: <b>{html.escape(airflow_run.get('state', 'not completed'))}</b></p><p><a href="{html.escape(run_url)}">Mở lần chạy GitHub Actions</a></p><p class="note">Báo cáo này trình bày kết quả được tạo trong lần chạy thật. Ảnh giao diện HDFS, Spark History và Airflow được lưu riêng. M của RFM là số thương vong quan sát được, không phải tiền. Kafka phát lại dữ liệu lịch sử trong phạm vi hữu hạn.</p><p>Commit: {html.escape(manifest.get('commit','not yet verified'))}<br>SHA256 nguồn: {html.escape(input_meta.get('original_sha256','unavailable'))}</p></header>'''
page_html = '<!doctype html><html lang="vi"><meta charset="utf-8"><title>California 2025 · Pipeline evidence</title><style>' + css + '</style><main>' + intro + ''.join(sections) + '<footer>Thiếu dữ liệu thương vong được giữ NULL. Số vụ không phải tỷ lệ rủi ro theo lưu lượng xe. Xem manifest và phương pháp trong repository.</footer></main></html>'
(EVIDENCE / 'report.html').write_text(page_html, encoding='utf-8')

captures = []
failures = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
    context = browser.new_context(viewport={'width': 1440, 'height': 1050}, device_scale_factor=1)
    page = context.new_page()
    page.goto((EVIDENCE / 'report.html').resolve().as_uri(), wait_until='load')
    for i in range(1, 10):
        path = EVIDENCE / f'{i:02d}-stage-results.png'
        page.locator(f'#step-{i}').screenshot(path=str(path))
        captures.append({'path': path.name, 'kind': 'Rendered actual stage output', 'step': i})
    for name, url, expected in [
        ('hdfs-overview.png', 'http://127.0.0.1:9870/dfshealth.html#tab-overview', 'Configured Capacity'),
        ('hdfs-files.png', 'http://127.0.0.1:9870/explorer.html#/california2025', 'raw'),
        ('spark-history.png', 'http://127.0.0.1:18080', 'CA2025')]:
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            page.get_by_text(expected, exact=False).first.wait_for(timeout=30000)
            page.screenshot(path=str(EVIDENCE / name), full_page=True)
            captures.append({'path': name, 'kind': 'Native live service UI', 'url': url})
        except Exception as exc:
            failures.append({'path': name, 'error': str(exc)})
    try:
        page.goto('http://127.0.0.1:8080/login/', wait_until='domcontentloaded')
        page.locator('#username').fill('report')
        page.locator('#password').fill(Path('.runtime/ui-password').read_text())
        page.locator('input[type=submit],button[type=submit]').first.click()
        page.wait_for_url(lambda url: '/login' not in url, timeout=30000)
        for view in ('grid', 'graph'):
            if view == 'graph':
                page.set_viewport_size({'width': 1440, 'height': 1600})
            url = f'http://127.0.0.1:8080/dags/california_crashes_2025/{view}'
            page.goto(url, wait_until='domcontentloaded')
            page.get_by_text('california_crashes_2025', exact=False).first.wait_for(timeout=30000)
            page.wait_for_timeout(2500)
            if view == 'graph':
                page.locator('.react-flow__node').first.wait_for(timeout=30000)
                # Prefer a readable vertical graph when the native UI offers it.
                for select in page.locator('select').all():
                    labels = select.locator('option').all_text_contents()
                    vertical = next((label for label in labels if label.strip().startswith('Top') and 'Bottom' in label), None)
                    if vertical:
                        select.select_option(label=vertical)
                        page.wait_for_timeout(1000)
                        break
                fit = page.locator('.react-flow__controls-fitview:visible')
                if fit.count():
                    fit.first.click()
                expected_tasks = [t['task_id'] for t in airflow_tasks]
                if expected_tasks:
                    page.wait_for_function('''names => {
                      const nodes = [...document.querySelectorAll('.react-flow__node')]
                        .filter(el => el.getClientRects().length > 0);
                      return names.every(name => nodes.some(el => el.textContent.includes(name)));
                    }''', arg=expected_tasks, timeout=30000)
            page.screenshot(path=str(EVIDENCE / f'airflow-{view}.png'), full_page=True)
            captures.append({'path': f'airflow-{view}.png', 'kind': 'Native live Airflow UI', 'url': url})
    except Exception as exc:
        page.screenshot(path=str(EVIDENCE / 'airflow-capture-debug.png'), full_page=True)
        (EVIDENCE / 'airflow-visible-nodes.json').write_text(json.dumps(page.locator('.react-flow__node:visible').all_text_contents(), indent=2))
        failures.append({'path': 'airflow', 'error': str(exc)})
    browser.close()

(EVIDENCE / 'screenshot-index.json').write_text(json.dumps({'captures': captures, 'failures': failures}, indent=2))
summary = f'''# California 2025 · {mode}\n\nAirflow DagRun: **{airflow_run.get('state','not completed')}**. Tasks: **{sum(t['state']=='success' for t in airflow_tasks)}/9**.\n\nBatch input: **{input_meta.get('selected_rows','unavailable')}** records. Screenshots: **{len(captures)}**.\n\nDownload the artifact and open `evidence/report.html`. Native UIs: HDFS, Spark History, Airflow Grid and Graph. RFM M = observed casualty magnitude, not money. Kafka = bounded historical replay.\n'''
(OUT / 'results/summary.md').write_text(summary, encoding='utf-8')
print(json.dumps({'screenshots': len(captures), 'capture_failures': failures}))
if failures:
    raise SystemExit('One or more native screenshot captures failed; evidence is incomplete')
