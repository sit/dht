#!/usr/bin/env python

import datetime
import getopt
import os
import string
import sys

import plapp

progname = os.path.basename (sys.argv[0])
now  = datetime.datetime.now ()

# We're lucky if we're running on amsterdam
root   = "/mnt/te2/strib/pl_app"
# Default to a little bit of time, but not too recent since data
# may be unreliable in the recent past.
finish = now + datetime.timedelta (hours=-4)
start  = now + datetime.timedelta (days=-1)

hoststate = {}
hosts  = None

def usage (special=""):
    sys.stderr.write (progname + 
	    "[-d root] [-h hosts] [-s daysago] [-f daysago]\n" +
	    special + "\n")

def readhosts (fn):
    hl = []
    fh = open (fn)
    hl = fh.readlines ()
    hl = map (string.strip, hl)
    return hl

try:
    opts, cmdv = getopt.getopt (sys.argv[1:], "d:h:s:f:")
except getopt.GetoptError:
    usage ()
    sys.exit (1)

for o, a in opts:
    if o == '-d':
	root = a
    elif o == '-f':
	finish = now + datetime.timedelta (days=-int(a))
    elif o == '-h':
	hosts = readhosts (a)
    elif o == '-s':
	start = now + datetime.timedelta (days=-int(a))

os.chdir (root)
oneday = datetime.timedelta (days=1)
t = start.date ()
while t <= finish.date ():
    # Find the appropriate directory of pl_app; organized by month/date
    dir = "%d-%02d/%d-%02d-%02d" % (t.year, t.month, t.year, t.month, t.day)
    sys.stderr.write ("Processing %s...\n" % dir)
    try:
	apps = os.listdir (dir)
    except OSError, e:
	sys.stderr.write ("Oops? %s\n" % e)
	t += oneday
	continue
    apps.sort ()
    for app in apps:
	if app == '.' or app == '..': continue
	fn = os.path.join (dir, app)
	p  = plapp.plapp (fn)
	if p.datetime > finish:
	    break
	sys.stderr.write ("%s\n" % p)
	if hosts:
	    dohosts = hosts
	else:
	    dohosts = p.hosts
	for h in dohosts:
	    v = p.get (h, h)
	    if v[0] != '*':
		if hoststate.get (h, 0) != 1: print p.time, "join", h
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
		    if hoststate.get (h, 0) != 0: print p.time, "fail", h
		    hoststate[h] = 0
    t += oneday
