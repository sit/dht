#!/usr/bin/env python

import asyncore
import re
import string
import sys

import vischat

def usage ():
    sys.stderr.write ("Usage: %s lsd-trace.log usenetdht.err vishost port")
    sys.exit (1)

if len (sys.argv) != 5:
    usage ()

lsdlog = sys.argv[1]
usenetdht = sys.argv[2]
vishost = sys.argv[3]
visport = int (sys.argv[4])

interesting_keys = []
msgkeyre = re.compile ("msgkey ([0-9a-f]+)$")
def monitor_usenet (vc, fh):
    lines = fh.readlines ()
    if len(lines) == 0:
        fh.seek (0, 2)
        return
    for line in lines:
        m = msgkeyre.search (line)
        if m:
            k = m.group (1)
            print "usenet: watching %s" % k
            interesting_keys.append (k)

def monitor_lsd (vc, fh):
    lines = fh.readlines ()
    if len(lines) == 0:
        fh.seek (0, 2)
        return
    for line in lines:
        for k in interesting_keys:
            if k in line:
                print line,
                break


nodes = []
def listcb (lines):
    nodes = map (lambda x: x.split ()[0], lines)
    def v (a, b):
        if len(a) == len(b): return cmp(a,b)
        else: return cmp(len(a),len(b))
    nodes.sort (v)
    
vc = vischat.vischat (vishost, visport)
def connected (v):
    print "Connection to vis established."
    v.list (listcb)
    v.reset ()

lfh = open (lsdlog)
ufh = open (usenetdht)
# Go to the end of the file
lfh.seek (0, 2)
ufh.seek (0, 2)

vc.start_connect (connected)
while 1:
    monitor_usenet (vc, ufh)
    monitor_lsd (vc, lfh)

    # Process some events for a while
    asyncore.poll (1)
