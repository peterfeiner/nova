#!/usr/bin/env python

import datetime
import os
import signal
import sys
import time
import json
import argparse

from numpy import mean, median
from subprocess import Popen, check_output
from threading import Thread, Lock

def timestamp():
    return str(datetime.datetime.now())

class Instance(object):
    def __init__(self, uuid, name, status, task_state, power_state, networks=''):
        self.uuid = uuid
        self.name = name
        self.status = status
        self.task_state = task_state
        self.power_state = power_state
        self.networks = {}
        for network in networks.split(','):
            name, sep, addr = network.partition('=')
            if sep:
                self.networks[name] = addr

    def any_ip(self):
        return self.networks.values()[0]

    def __repr__(self):
        return 'Instance(%r, %r, %r, %r, %r, %r)' % \
            (self.uuid, self.name, self.status, self.task_state,
             self.power_state, self.networks)

class Timer(object):
    def __init__(self, name):
        self.start()
        self.name = name

    def start(self):
        self.start_time = time.time()

    def elapsed(self):
        return time.time() - self.start_time

DEV_NULL = open('/dev/null', 'w+')

def nova_list():
    out = check_output(['nova', 'list'])
    instances = []
    for line in out.split('\n'):
        line = line.strip()
        if line == '' or '-+-' in line or 'Networks' in line:
            continue
        comps = [x.strip() for x in line.split('|') if x.strip() != '']
        instance = Instance(*comps)
        instances.append(instance)
    return instances

def nova_boot(name, image, flavor, key_name=None):
    args = ['nova', 'boot',
            '--image', image,
            '--flavor', flavor]
    if key_name != None:
        args.extend(['--key-name', key_name])
    args.append(name)
    return Popen(args, stdout=DEV_NULL)

def nova_live_image_start(name, image):
    return Popen(['nova', 'live-image-start', '--live-image', image, name],
                 stdout=DEV_NULL)

def nova_delete(name):
    return Popen(['nova', 'delete', name], stdout=DEV_NULL)

class Nova(object):

    def __init__(self):
        self.__list_thread = Thread(target=self.__list_main)
        self.__list_thread.daemon = True
        self.__list_thread.start()
        self.__list = []

    def __list_main(self):
        while True:
            self.__list = nova_list()
            time.sleep(0.5)

    def list(self):
        return self.__list

    def boot(self, image, flavor, key_name=None):
        return nova_boot(image, flavor, key_name=key_name)

    def live_image_start(self, name, image):
        return nova_live_image_start(name, image)

    def delete(self, name):
        return nova_delete(name)

class PopenLoopThread(Thread):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.delay = kwargs.pop('delay', None)
        Thread.__init__(self)

    def run(self):
        while True:
            p = Popen(*self.args, **self.kwargs)
            if p.wait() == 0:
                break
            if self.delay != None:
                time.sleep(self.delay)

class Atop(object):
    def __init__(self, title, interval=2):
        self.title = title
        self.process = None
        self.interval = interval

    def start(self):
        assert self.process == None
        self.process = Popen(['sudo', 'atop',
                              '-w', '%s@%s' % (self.title,
                                               timestamp().replace(' ', '-')),
                              str(self.interval)])

    def stop(self):
        self.process.send_signal(signal.SIGINT)
        self.process.wait()

class NullAtop(object):
    def start(self):
        pass

    def stop(self):
        pass

class Log(object):

    def __init__(self, timer, id=None):
        self.timer = timer
        self.id = id

    @classmethod
    def header(cls, id=False):
        line = []
        if id:
            line.append('ID')
        line.extend('TIME PHASE PH+T ARGS'.split())
        cls.__emit(line)

    __emit_lock = Lock()

    @classmethod
    def __emit(cls, line):
        with cls.__emit_lock:
            print '\t '.join(map(str, line))
            sys.stdout.flush()

    def __call__(self, timer, *args):
        line = []
        if self.id != None:
            line.append(self.id)
        line.append(round(self.timer.elapsed(), 2))
        line.append(timer.name)
        line.append(round(timer.elapsed(), 2))
        line.append(' '.join(map(str, args)))
        self.__emit(line)

class Phase(object):
    def __init__(self, name, start, end):
        self.name = name
        self.start = start
        self.end = end
        self.duration = self.end - self.start
        assert self.duration >= 0

class Experiment(object):
    def __init__(self, args, atop=None, nova=None, log=None):
        self.args = args
        self.timer = Timer('total')

        if nova == None:
            nova = Nova()
        self.nova = nova

        if atop == None:
            atop = Atop('%s-%s' % (self.args.op, self.args.n))
        self.atop = atop

        if log == None:
            log = Log(self.timer)
            log.header()
        self.log = log

        self.__phase_start = {}
        self.phases = []

    def start_phase(self, name):
        assert name not in self.__phase_start
        self.__phase_start[name] = self.timer.elapsed()

    def end_phase(self, name):
        self.phases.append(Phase(name,
                                 start=self.__phase_start.pop(name),
                                 end=self.timer.elapsed()))

    def wait_for_processes(self, processes, status, timer):
        for i in range(len(processes)):
            if processes[i].poll() == None:
                self.log(timer, {status: i})
            assert processes[i].wait() == 0
        self.log(timer, {status: len(processes)})

    def wait_for_threads(self, threads, status, timer):
        for i in range(len(threads)):
            if threads[i].is_alive():
                self.log(timer, {status: i})
            threads[i].join()
        self.log(timer, {status: len(threads)})

    def get_instances(self):
        return filter(lambda i: i.name.startswith(self.args.name_prefix),
                      self.nova.list())

    def start_atop(self):
        path = '%s-%d@%s.atop' % (self.args.op,
                                  self.args.n,
                                  timestamp().replace(' ', '-'))
        return Popen(['sudo', 'atop', '-w', path, '2'])

    def run(self):
        if len(self.get_instances()) != 0:
            raise Exception('There are already instances running.')

        self.atop.start()
        self.start_phase('total')
        try:
            self.timer.start()
            self.__create()
            if self.args.check_ping:
                self.__check_ping()
            if self.args.check_nmap:
                self.__check_nmap()
            if self.args.check_ssh:
                self.__check_ssh()
            self.__delete()
        finally:
            self.atop.stop()
        self.end_phase('total')
        assert len(self.__phase_start) == 0

    def instance_name(self, i):
        assert i >= 1
        assert i <= self.args.n
        return '%s-%d-of-%d' % (self.args.name_prefix, i, self.args.n)

    def __boot_op(self, name):
        return self.nova.boot(name,
                              image=self.args.image,
                              flavor=self.args.flavor,
                              key_name=self.args.key_name)

    def __launch_op(self, name):
        return self.nova.live_image_start(name, self.args.image)

    def __create(self):
        self.start_phase(self.args.op)
        if self.args.op == 'boot':
            op_func = self.__boot_op
        else:
            op_func = self.__launch_op

        timer = Timer(self.args.op)
        processes = []
        for i in range(self.args.n):
            processes.append(op_func(self.instance_name(i + 1)))
        self.wait_for_processes(processes, 'api ok', timer)

        last_active = -1
        while True:
            instances = self.get_instances()
            active = 0
            for i in instances:
                assert i.status != 'ERROR'
                if i.status == 'ACTIVE':
                    active += 1
            if last_active < active:
                self.log(timer, {'active': active})
            if active == self.args.n:
                break
            last_active = active
            time.sleep(1)
        self.end_phase(self.args.op)

    def __delete(self):
        instances = self.get_instances()
        self.start_phase('delete')

        timer = Timer('delete')
        processes = []
        for i in instances:
            processes.append(self.nova.delete(i.uuid))
        self.wait_for_processes(processes, 'api ok', timer)

        last_existing = len(instances)
        while True:
            instances = self.get_instances()
            for i in instances:
                assert i.status != 'ERROR'
            if len(instances) < last_existing:
                self.log(timer, {'deleted': last_existing - len(instances)})
            if len(instances) == 0:
                break
            last_existing = len(instances)
            time.sleep(1)
        self.end_phase('delete')

    def __check_nmap(self):
        self.start_phase('nmap')
        self.end_phase('nmap')

    def __check_ping(self):
        self.start_phase('ping')
        instances = self.get_instances()
        timer = Timer('ping')
        threads = []
        for i in instances:
            thread = PopenLoopThread(['ping', '-c', '1', instance.any_ip()],
                                     stdout=DEV_NULL)
            thread.start()
            threads.append(thread)
        self.wait_for_threads(threads, 'replied', timer)
        self.end_phase('ping')

    def __check_ssh(self):
        self.start_phase('ssh')
        instances = self.get_instances()
        timer = Timer('ssh')
        threads = []
        for i in instances:
            thread = PopenLoopThread(['ssh',
                                      '-l', self.args.check_ssh_user,
                                      '-o', 'UserKnownHostsFile=/dev/null',
                                      '-o', 'StrictHostKeyChecking=no',
                                      '-o', 'PasswordAuthentication=no',
                                      i.any_ip(),
                                      self.args.check_ssh_command],
                                     stdout=DEV_NULL,
                                     stderr=DEV_NULL,
                                     delay=1)
            thread.start()
            threads.append(thread)
        self.wait_for_threads(threads, 'ssh %s' % self.args.check_ssh_command,
                              timer)
        self.end_phase('ssh')

    def report(self):
        print '\t '.join(['PHASE', 'START', 'END', 'DURATION'])
        for phase in self.phases:
            print '\t '.join([phase.name,
                              str(round(phase.start, 2)),
                              str(round(phase.end, 2)),
                              str(round(phase.duration, 2))])

class ParallelExperiment(object):
    def __init__(self, args):
        self.args = args
        self.atop = Atop('%s-%s' % (self.args.op, self.args.n))
        self.nova = Nova()
        self.timer = Timer('total')
        self.experiments = []

    def run(self):
        Log.header(id=True)
        threads = []
        for i in range(self.args.n):
            id = i + 1

            args = clone_args(self.args)
            args.n = 1
            args.name_prefix = '%s-%s' % (self.args.name_prefix, id)
            args.parallel = False

            log = Log(timer=self.timer, id=id)
            
            experiment = Experiment(args, NullAtop(), self.nova, log)
            self.experiments.append(experiment)
            thread = Thread(target=experiment.run)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def report(self):
        byname = {}

        for e in self.experiments:
            for p in e.phases:
                byname.setdefault(p.name, [])
                byname[p.name].append(p)

        
        print '\t '.join(['',
                          'START', '', '',
                          'END', '', '',
                          'DURATION', '', ''])
        print '\t '.join(['PHASE'] + ['MEAN', 'MEDIAN', 'MAX'] * 3)

        for name, phases in byname.iteritems():
            starts = [phase.start for phase in phases]
            ends = [phase.end for phase in phases]
            durations = [phase.duration for phase in phases]

            print '\t '.join([name] +
                map(lambda x: str(round(x, 2)), 
                    [mean(starts), median(starts), max(starts),
                     mean(ends), median(ends), max(ends),
                     mean(durations), median(durations), max(durations)]))

def parse_argv(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('op', choices=['boot', 'launch'])
    parser.add_argument('n', type=int, default=1, nargs='?')
    parser.add_argument('--image',
                        default='precise-server-cloudimg-amd64-disk1.img')
    parser.add_argument('--flavor', default='m1.tiny')
    parser.add_argument('--key-name', default=None)
    parser.add_argument('--name-prefix', default='prof')
    parser.add_argument('--check-ssh', action='store_true')
    parser.add_argument('--check-ssh-user', default='ubuntu')
    parser.add_argument('--check-ssh-command', default='true')
    parser.add_argument('--check-ping', action='store_true')
    parser.add_argument('--check-nmap', type=int, default=None)
    parser.add_argument('--parallel', action='store_true')
    return parser.parse_args(argv[1:])

def clone_args(args):
    clone = args.__class__()
    for key, value in vars(args).iteritems():
        setattr(clone, key, value)
    return clone

def main(argv):
    args = parse_argv(argv)
    if args.parallel:
        cls = ParallelExperiment
    else:
        cls = Experiment
    experiment = cls(args)
    experiment.run()
    print
    experiment.report()

if __name__ == '__main__':
    main(sys.argv)
