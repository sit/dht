#!/usr/bin/env python
events = ["fail", "join"]

import getopt
import gzip
import os
import string
import sys

progname = os.path.basename (sys.argv[0])

liveness = {}
hosts  = None

def load_hoststate (fn):
    if fn[-3:] == '.gz':
        fh = gzip.open (fn)
    else:
        fh = open (fn)
    line = fh.readline ()
    while line[0] == '#':
        words = line.split ()
        if words[1].lower ()[:4] == 'time':
	    words.reverse ()
	    for w in words:
                try:
                    fhtime = int (w)
                except:
                    fhtime = 0
                if fhtime > 0: break
	line = fh.readline ()
        # Should be a blank line after header section of comments
    return (fh, fhtime)

def usage (special=""):
    sys.stderr.write (progname + 
      " [-d root] [-h hosts] [-f inputfile] [-i idfield] [-l livenessfield]\n" +
      special + "\n")

def readhosts (fn):
    hl = []
    fh = open (fn)
    hl = fh.readlines ()
    hl = map (string.strip, hl)
    return hl

try:
    opts, cmdv = getopt.getopt (sys.argv[1:], "d:f:h:i:l:")
except getopt.GetoptError:
    usage ()
    sys.exit (1)

# Defaults
statefh = sys.stdin
idfield = 0
udfield = 1
root = None

for o, a in opts:
    if o == '-d':
	root = a
    elif o == '-f':
        statefh = open (a)
    elif o == '-h':
	hosts = readhosts (a)
    elif o == '-i':
        idfield = int (a)
    elif o == '-l':
        udfield = int (a)

if idfield == udfield:
    usage ("idfield and livenessfield must be different!")
    sys.exit (1)

if root:
    os.chdir (root)

events = ["fail", "join"]
for fn in statefh:
    fn = fn.strip ()
    if fn[0] == '#':
        continue
    sys.stderr.write ("Processing %s...\n" % fn)
    fh, fhtime = load_hoststate (fn)

    for line in fh:
	line = line.strip ()
	if len(line) == 0 or line[0] == '#': continue
        fields = line.strip ().split ()
        h  = fields[idfield]
        if hosts and h not in hosts: continue
        up = int (fields[udfield])
        
        if liveness.get (h, 0) != up:
            liveness[h] = up
            print fhtime, events[up], h
