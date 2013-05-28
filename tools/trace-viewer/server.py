#!/usr/bin/env python
# Copyright (c) 2012 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import optparse
import os
import sys
import time
import glob
try:
    import ujson as json
except ImportError:
    import json

import SimpleHTTPServer
import BaseHTTPServer

DEFAULT_PORT = 8003

class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
  def do_GET(self):
    if self.path == '/traces':
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        traces = []
        for path in glob.glob('traces/libvirt/*.trace') + glob.glob('traces/*.trace'):
            try:
                events = json.loads('%s null]' % open(path).read())[0:-1]
            except ValueError:
                print 'error loading', path
                continue
            if len(events) == 0:
                continue
            traces.append({'path': path,
                           'end': events[-1]['ts'],
                           'start': events[0]['ts'],
                           'req': os.path.splitext(os.path.basename(path))[0],
                           'args': events[0].get('args', {}),
                          })
        json.dump(traces, self.wfile)
    elif self.path.startswith('/traces/') and self.path.endswith('?n=1'):
        events = json.loads('%s null]' % open(self.path[1:-4]).read())[0:-1]
        start = events[0]['ts']
        for event in events:
            event['ts'] -= start
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        json.dump(events, self.wfile)
    else:
        return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

class Server(BaseHTTPServer.HTTPServer):
  def __init__(self, *args, **kwargs):
    BaseHTTPServer.HTTPServer.__init__(self, *args, **kwargs)
    self.next_deps_check = -1

def Main(args):
  parser = optparse.OptionParser()
  parser.add_option('--port',
                    action='store',
                    type='int',
                    default=DEFAULT_PORT,
                    help='Port to serve from')
  options, args = parser.parse_args()
  server = Server(('', options.port), Handler)
  sys.stderr.write("Now running on http://localhost:%i\n" % options.port)
  server.serve_forever()

if __name__ == '__main__':
  sys.exit(Main(sys.argv[1:]))
