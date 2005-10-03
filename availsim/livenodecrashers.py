#!/usr/bin/env python

import fileinput
import sys

livenodes = {}
count = 0
for line in fileinput.input ():
    (t, e, n) = line.strip ().split (None, 2)
    if e == 'join':
	count += 1
	livenodes[n] = 1
    elif count > 0 and (e == 'fail' or e == 'crash'):
	if e == 'crash':
	    print >>sys.stderr, t, count
	if livenodes.get(n, 0):
	    count -= 1
	    livenodes[n] = 0
    print t, count
