#!/usr/bin/env python

import datetime
import getopt
import os
import string
import sys

import plapp

# Change this if anything important about the output changes.
VERSION = 1

progname = os.path.basename (sys.argv[0])
now  = datetime.datetime.now ()

# We're lucky if we're running on amsterdam
root   = "/mnt/te2/strib/pl_app"
# Default to a little bit of time, but not too recent since data
# may be unreliable in the recent past.
finish = now + datetime.timedelta (hours=-4)
start  = now + datetime.timedelta (days=-1)

hoststate = {}

def usage (special=""):
    sys.stderr.write (progname + 
	    "[-d approot] [-s daysago] [-f daysago] [-o outdir]\n" +
	    special + "\n")

def dump_header (time):
    return "# plstate version %d\n# plapp version %d\n# time %d\n" \
           % (VERSION, plapp.VERSION, time)

def dump (dir, time, state):
    # XXX could be more careful about if dir is not really a dir.
    if not os.path.exists (dir):
	os.mkdir (dir)
    fn = os.path.join (dir, "%s.txt" % time)
    fh = open (fn, "w")
    fh.write (dump_header (time))
    k = state.keys ()
    k.sort ()
    for h in k:
        fh.write ("%s\t%d\n" % (h, state[h]))
    fh.close ()

def check_dump (dir, time):
    fn = os.path.join (dir, "%s.txt" % time)
    try:
        fh = open (fn)
    except:
        return 0
    expected = dump_header (time)
    actual   = fh.read (len (expected))
    fh.close ()
    return actual == expected

### main program ###
try:
    opts, cmdv = getopt.getopt (sys.argv[1:], "d:e:fo:s:")
except getopt.GetoptError:
    usage ()
    sys.exit (1)

force = 0
outdir = ''

for o, a in opts:
    if o == '-d':
	root = a
    elif o == '-e':
	finish = now + datetime.timedelta (days=-int(a))
    elif o == '-f':
        force = 1
    elif o == '-o':
        outdir = a
    elif o == '-s':
	start = now + datetime.timedelta (days=-int(a))

oneday = datetime.timedelta (days=1)
t = start.date ()
while t <= finish.date ():
    # Find the appropriate directory of pl_app; organized by month/date
    dir = "%d-%02d/%d-%02d-%02d" % (t.year, t.month, t.year, t.month, t.day)
    dumpdir = os.path.join (outdir, "%d-%02d-%02d" % (t.year, t.month, t.day))
    sys.stderr.write ("Processing %s...\n" % dir)
    try:
	apps = os.listdir (os.path.join (root, dir))
    except OSError, e:
	sys.stderr.write ("Oops? %s\n" % e)
	t += oneday
	continue
    apps.sort ()
    for app in apps:
	if app == '.' or app == '..': continue
	fn = os.path.join (root, dir, app)
	p  = plapp.plapp (fn)
	if p.datetime > finish:
	    break
        if not force and check_dump (dumpdir, p.time):
            continue
	sys.stderr.write ("%s\n" % p)
	for h in p.hosts:
	    v = p.get (h, h)
	    if v[0] != '*':
		hoststate[h] = 1
	    else:
		# It appears that the host is down.  Maybe it
		# just couldn't report in in time, so let's see if
		# anyone else could talk to it
		goodness = 0
		for g in p.hosts:
		    gv = p.get (g, h)
		    if gv[0] != '*': goodness += 1
		    if goodness > 1: 
			break
		if goodness < 2:
		    hoststate[h] = 0
        dump (dumpdir, p.time, hoststate)
    t += oneday
