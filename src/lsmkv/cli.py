"""Hex-byte CLI for a single serial database directory."""
import argparse
import json
import re
import sys

from .engine import Engine
from .manifest import decode as decode_manifest
from .realfs import RealFS


def hex_bytes(text):
    if re.fullmatch(r'(?:[0-9a-fA-F]{2})*', text) is None:
        raise argparse.ArgumentTypeError('expected even-length hexadecimal bytes (no 0x prefix or spaces)')
    return bytes.fromhex(text)


def parser():
    cli = argparse.ArgumentParser(prog='lsmkv', description='WAL-backed LSM key-value storage; keys and values use hexadecimal bytes.')
    cli.add_argument('--db', required=True, help='database directory; its parent must already exist')
    cli.add_argument('--memtable-bytes', type=int, default=4 * 1024 * 1024)
    cli.add_argument('--wal-bytes', type=int, default=16 * 1024 * 1024)
    commands = cli.add_subparsers(dest='command', required=True)
    put = commands.add_parser('put'); put.add_argument('key', type=hex_bytes); put.add_argument('value', type=hex_bytes)
    for name in ('get', 'delete'):
        command = commands.add_parser(name); command.add_argument('key', type=hex_bytes)
    for name in ('flush', 'compact', 'info'):
        commands.add_parser(name)
    return cli


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.memtable_bytes <= 0 or args.wal_bytes <= 0:
            raise ValueError('flush thresholds must be positive')
        with RealFS(args.db) as fs:
            db = Engine.open(fs, memtable_bytes=args.memtable_bytes, wal_bytes=args.wal_bytes)
            try:
                status = 0
                if args.command == 'put': db.put(args.key, args.value); result = {'ok': True}
                elif args.command == 'delete': db.delete(args.key); result = {'ok': True}
                elif args.command == 'get':
                    value = db.get(args.key)
                    result = {'found': value is not None, 'value_hex': None if value is None else value.hex()}
                    status = 1 if value is None else 0
                elif args.command == 'flush': db.flush(); result = {'ok': True}
                elif args.command == 'compact': db.compact(); result = {'ok': True}
                else:
                    result = {'sync_primitive': fs.sync_primitive,
                              'directory_sync_primitive': fs.directory_sync_primitive,
                              'parent_sync_primitive': fs.parent_sync_primitive,
                              'next_seq': db.next_seq, 'manifest': decode_manifest(fs.read('MANIFEST'))}
            finally:
                db.close()
        print(json.dumps(result, sort_keys=True))
        return status
    except (OSError, ValueError, RuntimeError) as error:
        print('error: ' + str(error), file=sys.stderr)
        return 2
