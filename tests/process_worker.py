"""JSON-line test worker. Never use its stepping protocol as a storage API."""
import json
import sys
import traceback

from lsmkv import Engine
from lsmkv.harness import FS_CALLS
from lsmkv.realfs import RealFS


def send(message):
    print(json.dumps(message, sort_keys=True), flush=True)


def receive():
    line = sys.stdin.readline()
    if not line:
        raise RuntimeError('parent disconnected')
    return json.loads(line)


class SteppedFS:
    def __init__(self, fs, enabled):
        self.fs, self.enabled = fs, enabled
        self.scope, self.calls = 'open', 0

    def checkpoint(self, call, method, phase):
        if not self.enabled:
            return
        event = {'event': 'fs', 'id': self.scope, 'call': call, 'method': method, 'phase': phase}
        send(event)
        reply = receive()
        if reply != {'event': 'continue', 'id': self.scope, 'call': call, 'phase': phase}:
            raise RuntimeError('invalid checkpoint handshake')

    def __getattr__(self, name):
        if name not in FS_CALLS:
            return getattr(self.fs, name)
        def invoke(*args, **kwargs):
            call = self.calls; self.calls += 1
            self.checkpoint(call, name, 'before')
            result = getattr(self.fs, name)(*args, **kwargs)
            self.checkpoint(call, name, 'after')
            return result
        return invoke


def main():
    io = SteppedFS(RealFS(sys.argv[1]), '--step-open' in sys.argv[2:])
    send({'event': 'adapter', 'file_sync': io.sync_primitive,
          'directory_sync': io.directory_sync_primitive, 'parent_sync': io.parent_sync_primitive})
    db = Engine.open(io)
    send({'event': 'ready', 'file_sync': io.sync_primitive,
          'directory_sync': io.directory_sync_primitive, 'parent_sync': io.parent_sync_primitive})
    while True:
        command = receive()
        if command['kind'] == 'stop':
            io.enabled = False; db.close(); send({'event': 'stopped'}); return
        op_id = command['id']
        send({'event': 'started', 'id': op_id})
        if receive() != {'event': 'run', 'id': op_id}:
            raise RuntimeError('invalid operation-start handshake')
        io.scope, io.calls = op_id, 0
        io.enabled = not command.get('free_running', False)
        kind = command['kind']
        value = None
        if kind == 'put': db.put(bytes.fromhex(command['key']), bytes.fromhex(command['value']))
        elif kind == 'delete': db.delete(bytes.fromhex(command['key']))
        elif kind == 'get':
            data = db.get(bytes.fromhex(command['key']))
            value = None if data is None else data.hex()
        elif kind == 'flush': db.flush()
        elif kind == 'compact': db.compact()
        else: raise ValueError('unknown worker operation')
        result = {'id': op_id, 'value': value, 'table_count': len(db._tables)}
        send({'event': 'completed', **result})
        if receive() != {'event': 'ack', 'id': op_id}:
            raise RuntimeError('invalid acknowledgment handshake')
        send({'event': 'acknowledged', **result})


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        send({'event': 'error', 'error': repr(error)})
        traceback.print_exc(file=sys.stderr)
        raise SystemExit(2)
