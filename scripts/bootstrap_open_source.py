"""Create safe local runtime files for a fresh open-source checkout."""
from pathlib import Path
import json, secrets, shutil

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "private"
DATA.mkdir(exist_ok=True)
(DATA / "auths").mkdir(exist_ok=True)

def copy_example(name, example_name):
    target = DATA / name
    example = DATA / example_name
    if not target.exists() and example.exists():
        shutil.copyfile(example, target)

copy_example('upstreams.json', 'upstreams.example.json')
copy_example('control-settings.json', 'control-settings.example.json')
copy_example('official-models.json', 'official-models.example.json')
if not (DATA / 'secrets.json').exists():
    (DATA / 'secrets.json').write_text(json.dumps({
        'client': 'sk-local-' + secrets.token_urlsafe(24),
        'management': 'mgmt-' + secrets.token_urlsafe(32),
    }, indent=2), encoding='utf-8')
print('已创建本地运行配置，请在 private/upstreams.json 中添加供应商。')
