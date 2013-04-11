#!/usr/bin/env python

import datetime
import os
import signal
import sys
import time
import json

from subprocess import Popen, check_output

class Instance(object):
    def __init__(self, uuid, name, state, networks=''):
        self.uuid = uuid
        self.name = name
        self.state = state
        self.networks = networks

    def __repr__(self):
        return 'Instance(%r, %r, %r, %r)' % \
            (self.uuid, self.name, self.state, self.networks)

class Timer(object):
    def __init__(self, name):
        self.start()
        self.name = name

    def start(self):
        self.start_time = time.time()

    def elapsed(self):
        return time.time() - self.start_time

def nova_list():
    out = check_output(['nova', 'list'])
    instances = []
    for line in out.split('\n'):
        line = line.strip()
        if line == '' or '-+-' in line or 'Networks' in line:
            continue
        comps = [x.strip() for x in line.split('|') if x.strip() != '']
        instances.append(Instance(*comps))
    return instances

IMAGE_NAME = 'cirros-0.3.1-x86_64-uec'
IMAGE = '6543052d-0ba5-40ca-a445-6afa34d3ba38'
FLAVOR_NAME = 'm1.tiny'
FLAVOR = '1'
DEV_NULL = open('/dev/null', 'w+')

def nova_boot(name):
    return Popen(['nova', 'boot', '--image', IMAGE, '--flavor', FLAVOR, name],
                 stdout=DEV_NULL)

def nova_delete(name):
    return Popen(['nova', 'delete', name], stdout=DEV_NULL)

if len(nova_list()) != 0:
    raise Exception('There are already instances running.')

if len(sys.argv) != 2:
    raise Exception('No N passed in.')

N = int(sys.argv[1])

def log(timer, *args):
    record = {'ts': str(datetime.datetime.now()),
              'pid': os.getpid(),
              'elapsed': timer.elapsed(),
              'name': timer.name,
              'N': N,
              'args': args,
             }
    print json.dumps(record), ','

atop = Popen(['sudo', 'atop', '-w', 'N=%s,pid=%d.atop' % (N, os.getpid()), '2',])
try:
    boot_timer = Timer('boot')
    boot_processes = []
    for i in range(N):
        boot_processes.append(nova_boot('boot-%d-of-%d' % ((i + 1), N)))
    for p in boot_processes:
        assert p.wait() == 0

    last_active = -1
    while True:
        instances = nova_list()
        active = 0
        for i in instances:
            assert i.state != 'ERROR'
            if i.state == 'ACTIVE':
                active += 1
        if last_active < active:
            log(boot_timer, {'active': active})
        if active == N:
            break
        last_active = active
        time.sleep(2)

    instances = nova_list()
    delete_timer = Timer('delete')
    delete_processes = []
    for i in instances:
        delete_processes.append(nova_delete(i.uuid))
    for p in delete_processes:
        assert p.wait() == 0

    last_existing = len(instances)
    while True:
        instances = nova_list()
        for i in instances:
            assert i.state != 'ERROR'
        if len(instances) < last_existing:
            log(delete_timer, {'deleted': last_existing - len(instances)})
        if len(instances) == 0:
            break
        last_existing = len(instances)
        time.sleep(2)
finally:
    atop.send_signal(signal.SIGINT)
