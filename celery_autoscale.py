#!/usr/bin/env python
from argparse import ArgumentParser
from os import path
import datetime
import sys
import configparser
import supervisor.xmlrpc
import xmlrpclib
import rabbitpy


def timeprint(S):
    print("{0}: {1}".format(datetime.datetime.now(), S))


def autoscale(g):
    sys.stdout = open(g['scale_log'], 'a')
    sys.stderr = open(g['scale_log'], 'a')
    sc = xmlrpclib.ServerProxy('http://127.0.0.1',
                              transport=supervisor.xmlrpc.SupervisorTransport(
                                  None, None, g['supervisor_socket']))
    with rabbitpy.Connection(g['celery_broker_url']) as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, g['monit_queue'])
            ql = len(queue)
    la = float(open(g['la_file']).read())


    def balance_load(operation):
        i = 0
        for ap in g['autoscale_processes']:
            if i > 0:
                break
            if operation == 'min':
                if sc.supervisor.getProcessInfo(ap) == 20:
                    i += 1
                    sc.supervisor.stopProcess(ap)
                    timeprint("{0} process stoped".format(ap))
            if operation == 'max':
                if sc.supervisor.getProcessInfo(ap) == 0:
                    i += 1
                    sc.supervisor.startProcess(ap)
                    timeprint("{0} process started".format(ap))
        if i == 0:
            if operation == 'min':
                timeprint("All autoscale processes stopped, in queue 0 task")
            if operation == 'max':
                timeprint("All autoscale processes started, in queue {0} task".format(ql))


    if sc.system.listMethods['statecode'] == 1:
        if sc.supervisor.getProcessInfo(g['base_process']) == 20:
            if ql > 1:
                if la > g['la_workvalue'] - 0.7 and la < g['la_workvalue'] + 0.7:
                    timeprint("Optimal load, do nothing")
                    exit(0)
                else:
                    if la > g['la_workvalue']:
                        balance_load('min')
                    else:
                        balance_load('max')
            else:
                balance_load('min')
        else:
            timeprint("Base process in not running")
            exit()
    else:
        timeprint("Supervisor is not running")
        exit()


def main():
    p = ArgumentParser()
    p.add_argument('--config', '-c', action='store', default='/etc/celery_autoscale.conf',
                   help='configuration file path (default /etc/celery_autoscale.conf)')
    args = p.parse_args()
    if path.isfile(args.config):
        config = configparser.ConfigParser()
        config.read_file(open(args.config))
        for section in config.sections():
            s = dict()
            s['supervisor_socket'] = config.get(section, 'supervisor_socket')
            s['base_process'] = config.get(section, 'base_process')
            s['autoscale_processes'] = config.get(section, 'autoscale_processes')
            s['la_file'] = config.get(section, 'la_file')
            s['la_workvalue'] = float(config.get(section, 'la_workvalue'))
            s['celery_broker_url'] = config.get(section, 'celery_broker_url')
            s['monit_queue'] = config.get(section, 'monit_queue')
            s['scale_log'] = config.get(section, 'scale_log')
            autoscale(s)
    else:
        print("Configuration file can not be found")
        exit(1)


if __name__ == "__main__":
    main()
