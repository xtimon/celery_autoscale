#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import os
import multiprocessing
import getpass
import json


def put_file(args, stats_file):
    os.system("scp -P {0} {2} {1}@{3}:{4}".format(
        args.port, args.login, stats_file, args.hostname, args.file
    ))


def get_stats():
    stats = dict()
    stats['cpu_count'] = multiprocessing.cpu_count()
    stats['cpu_load_min'] = os.getloadavg()[0]
    meminfo = dict((i.split()[0].rstrip(':'),int(i.split()[1])) for i in open('/proc/meminfo').readlines())
    stats['mem_total_kib'] = meminfo['MemTotal']
    stats['mem_free_kib'] = meminfo['MemFree']
    stats['swap_total_kib'] = meminfo['SwapTotal']
    stats['swap_free_kib'] = meminfo['SwapFree']
    stats['mem_cached'] = meminfo['Cached']
    return stats


def main():
    p = ArgumentParser()
    p.add_argument('hostname', metavar='hostname',
                   help='hostname or ip address')
    p.add_argument('-i', '--identity_file', action='store', default='{0}/.ssh/id_rsa'.format(os.getenv("HOME")),
                   help='identity file, default: ~/.ssh/id_rsa')
    p.add_argument('-l', '--login', action='store', default=getpass.getuser(),
                   help='login name, default: {0}'.format(getpass.getuser()))
    p.add_argument('-p', '--port', action='store', type=int, default=22,
                   help='port number')
    p.add_argument('-f', '--file', action='store',
                   help='remote file path')
    args = p.parse_args()
    stats_file = '/tmp/dgfsgdsgsgsgh'
    json.dump(get_stats(), open(stats_file, 'w'))
    put_file(args, stats_file)


if __name__ == "__main__":
    main()
