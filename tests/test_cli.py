import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

pytestmark = [pytest.mark.realfs, pytest.mark.skipif(sys.platform not in ('darwin', 'linux'), reason='CLI storage requires Linux/macOS')]
ROOT = Path(__file__).resolve().parents[1]


def command(path, *args):
    env = dict(os.environ)
    env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
    return subprocess.run([sys.executable, '-m', 'lsmkv', '--db', str(path), *args],
                          capture_output=True, text=True, env=env, timeout=15)


def test_cli_hex_bytes_empty_values_and_maintenance(tmp_path):
    path = tmp_path / 'db'
    for args in [('put', '', ''), ('put', '00ff', 'ff000a'), ('flush',), ('compact',)]:
        result = command(path, *args)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)['ok'] is True
    result = command(path, 'get', '')
    assert result.returncode == 0 and json.loads(result.stdout) == {'found': True, 'value_hex': ''}
    result = command(path, 'get', '00ff')
    assert result.returncode == 0 and json.loads(result.stdout)['value_hex'] == 'ff000a'
    assert command(path, 'delete', '00ff').returncode == 0
    result = command(path, 'get', '00ff')
    assert result.returncode == 1 and json.loads(result.stdout) == {'found': False, 'value_hex': None}
    result = command(path, 'info')
    info = json.loads(result.stdout)
    assert result.returncode == 0 and info['next_seq'] == 4
    assert info['sync_primitive'] == ('fcntl.F_FULLFSYNC' if sys.platform == 'darwin' else 'os.fsync')
    assert info['directory_sync_primitive'] == info['parent_sync_primitive'] == 'os.fsync'


@pytest.mark.parametrize('hex_key', ['0', '0x01', 'gg', 'aa bb'])
def test_invalid_hex_is_rejected_before_initialization(tmp_path, hex_key):
    path = tmp_path / 'db'
    result = command(path, 'put', hex_key, '00')
    assert result.returncode == 2 and not path.exists()


def test_cli_storage_error_is_explicit_and_releases_resources(tmp_path):
    from lsmkv import Engine
    from lsmkv.realfs import RealFS
    path = tmp_path / 'db'
    db = Engine.open(RealFS(path))
    result = command(path, 'put', '01', '02')
    assert result.returncode == 2 and 'error:' in result.stderr
    db.close()
    assert command(path, 'put', '01', '02').returncode == 0
