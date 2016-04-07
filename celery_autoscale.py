#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from djcelery import celery
import os
import datetime
import sys
import configparser
import rabbitpy
import multiprocessing
import json


def timed_print(S):
    print("{0}: {1}".format(datetime.datetime.now(), S))


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


def get_node_proc_count(celery_node):
    return celery.control.inspect().stats()[celery_node]['pool']['writes']['inqueues']['total']


def get_queue_length(queue):
    amqp_connection_string = celery.broker_connection().as_uri()
    amqp_password = celery.broker_connection().info().__getitem__('password')
    amqp_connection_string = amqp_connection_string.replace('**', amqp_password)
    with rabbitpy.Connection(amqp_connection_string) as conn:
            with conn.channel() as channel:
               queue_length = len(rabbitpy.Queue(channel, queue))
    return queue_length


def print_all_stats(bg_stats, db_stats):
    timed_print("bg_stats: {0}".format(bg_stats))
    timed_print("db_stats: {0}".format(db_stats))


def shrink_pool(scaling_step, celery_node):
    celery.control.pool_shrink(scaling_step, [celery_node])
    timed_print("-{0} (processes count {1})".format(
        scaling_step, get_node_proc_count(celery_node)
    ))


def grow_pool(scaling_step, celery_node):
    celery.control.pool_grow(scaling_step, [celery_node])
    timed_print("+{0} (processes count {1})".format(
        scaling_step, get_node_proc_count(celery_node)
    ))


def check_la(stats):
    interval = 0.7
    if stats['cpu_load_min'] > stats['cpu_count'] - interval and stats['cpu_load_min'] < stats['cpu_count'] - interval:
        return 0
    elif stats['cpu_load_min'] < stats['cpu_count']:
        return 1
    else:
        return -1


def check_mem(stats, min_cache):
    interval = 5
    free = float(stats['mem_free_kib'] + stats['mem_cached']) / stats['mem_total_kib'] * 100
    if free > min_cache - interval and free < min_cache + 5:
        return 0
    elif free > min_cache:
        return 1
    else:
        return -1


def check_swap(stats):
    limit = 50
    free = float(stats['swap_free_kib']) / stats['swap_total_kib'] * 100
    if free > limit:
        return 1
    else:
        return -1


def autoscale(config):
    sys.stdout = open(config['scale_log'], 'a')
    sys.stderr = open(config['scale_log'], 'a')
    bg_stats = get_stats()
    db_stats = json.load(open(config['db_stats_file']))
    print(bg_stats)
    print(db_stats)
    sum_queue_length = 0
    for queue in config['celery_queues'].split(','):
        sum_queue_length += get_queue_length(queue)
    if sum_queue_length > config['scaling_step']:
        to_do_list = [check_la(bg_stats), check_mem(bg_stats, config['minimal_cache']), check_swap(bg_stats),
                      check_la(db_stats), check_mem(db_stats, config['minimal_cache']), check_swap(db_stats)]
        if -1 in to_do_list:
            shrink_pool(config['scaling_step'], config['celery_node'])
        elif all(x == 1 for x in to_do_list):
            grow_pool(config['scaling_step'], config['celery_node'])
        else:
            if config['debug']:
                timed_print("nothing to do (processes count {0})".format(get_node_proc_count(config['celery_node'])))
        if config['debug']:
            print_all_stats(bg_stats, db_stats)
    else:
        if get_node_proc_count(config['celery_node']) > config['min_processes']:
            shrink_pool(int(config['scaling_step']), config['celery_node'])
            if config['debug']:
                print_all_stats(bg_stats, db_stats)
        elif config['debug']:
            timed_print("nothing to do (processes count {0})".format(get_node_proc_count(config['celery_node'])))
            print_all_stats(bg_stats, db_stats)


def main():
    p = ArgumentParser()
    p.add_argument('--config', '-c', action='store', default='/etc/celery_autoscale.conf',
                   help='configuration file path (default /etc/celery_autoscale.conf)')
    args = p.parse_args()
    if os.path.isfile(args.config):
        config = configparser.ConfigParser()
        config.read_file(open(args.config))
        for section in config.sections():
            s = dict()
            s['celery_node'] = str(config.get(section, 'celery_node'))
            s['celery_queues'] = str(config.get(section, 'celery_queues'))
            s['db_stats_file'] = str(config.get(section, 'db_stats_file'))
            s['min_processes'] = int(config.get(section, 'min_processes'))
            s['max_processes'] = int(config.get(section, 'max_processes'))
            s['scale_log'] = str(config.get(section, 'scale_log'))
            s['minimal_cache'] = float(config.get(section, 'minimal_cache_size_percent'))
            s['scaling_step'] = int(config.get(section, 'scaling_step'))
            s['debug'] = bool(config.get(section, 'debug'))
            autoscale(s)
    else:
        print("Configuration file can not be found")
        exit(1)


if __name__ == "__main__":
    main()
