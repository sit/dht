import datetime
import gzip
import operator
import string
import time

"""
This file contains some tools for parsing the all pairs ping data collected
by Jeremy Stribling <strib@mit.edu>. 

For details about the formatting of the APP data files, see:
    http://www.pdos.lcs.mit.edu/~strib/pl_app/
"""

class plapp:
    def __init__ (my, fn):
	if fn[-3:] == '.gz':
	    my.fh = gzip.open (fn)
	else:
	    my.fh = open (fn)
	tstr = my.fh.readline ().strip ()
	my.time = int (time.mktime (time.strptime (tstr, "%H:%M:%S %m/%d/%Y")))
	my.datetime = datetime.datetime.fromtimestamp (my.time)
	my.pings = int (my.fh.readline ())
	my.hosts = string.split (my.fh.readline ().strip ())
	my.hostdx = {}
	for dx in xrange (0, len (my.hosts)):
	    my.hostdx[my.hosts[dx]] = dx

	my.hosts_read = -1
	my.matrix = {}

    def __str__ (my):
	return "%s #%d" % (my.datetime, len (my.hosts))

    def _readline (my):
	line = my.fh.readline ().strip ()
	my.hosts_read += 1
	host = my.hosts[my.hosts_read] # the node for which this line has data
	parts = line.split ()
	if line[:24] == "*** no data received for":
	    assert parts[5] == host, "Mismatched host line (%s vs %s)!" % (parts[5], host)
	    my.matrix[host] = None
	else:
	    assert len (parts) == len (my.hosts), "Incorrect number of entries on line %d (%d vs expected %d, for host %s)" % (my.hosts_read + 3, len (parts), len (my.hosts), host)
	    dummy = {}
	    map (operator.setitem, [dummy] * len (my.hosts), my.hosts, parts)
	    my.matrix[host] = dummy

    def get (my, x, y):
	if x not in my.hostdx or y not in my.hostdx:
	    # no idea who you're talking about
	    return '***no_data***'
	xdx = my.hostdx[x]
	while my.hosts_read < xdx:
	    my._readline ()
	xread = my.matrix[x]
	if xread == None: # x node failed to report
	    return "***no_data***"
	return my.matrix[x][y]

if __name__ == '__main__':
    import sys
    if len (sys.argv) > 1:
	x = plapp (sys.argv[1])
    else:
	x = plapp ("/mnt/te2/strib/pl_app/current.app")
    print x
    print x.get ("128.100.241.68", "128.100.241.68")
    print x.get ("140.109.17.180", "140.109.17.180")
