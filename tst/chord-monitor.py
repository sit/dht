#!/usr/bin/env python

#
# This is a simple threaded application that listens for connections from
# phone-home.py scripts running on remote nodes.  It expects to read a
# greeting of the form:
#   HELO hostname.lcs.mit.edu 5
# indicating a connection from hostname reporting on 5 vnodes
#
import os, sys, string, time
from SocketServer import *
from threading import *

# Hack?? Cause threads to exit when main thread exits.
class DaemonThreadingMixIn:
    """Mix-in class to handle each request in a new daemon thread."""

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.

        In addition, exception handling is done here.

        """
        try:
            self.finish_request(request, client_address)
            self.close_request(request)
        except:
            self.handle_error(request, client_address)
            self.close_request(request)

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        import threading
        t = threading.Thread(target = self.process_request_thread,
                             args = (request, client_address))
        t.setDaemon (1)
        t.start()

# Inter-thread state and communication
connections = {}
conlock     = Lock ()
def add_connection (handler):
    conlock.acquire ()
    assert handler.client_address not in connections
    print 'adding', handler.client_address
    connections[handler.client_address] = handler
    conlock.release ()

def del_connection (handler):
    conlock.acquire ()
    assert handler.client_address in connections
    del connections[handler.client_address]
    conlock.release ()

exitflag = Event ()

# Periodic checker
def processor ():
    from time import sleep
    while 1:
        exitflag.wait (30)
        if exitflag.isSet ():
            sys.exit ()
            
        args = []
        totvnodes = 0
        conlock.acquire ()
        for n in connections:
            h = connections[n]
            if h.ready:
                args.append (h.fname ())
                totvnodes = totvnodes + h.vnodes
        conlock.release ()
        if totvnodes == 0:
            continue
	print "Processing %d vnodes from %d files" % (totvnodes, len(args))
        args.insert (0, "%d" % totvnodes)
        args.insert (0, './stable.pl')
        rv = os.spawnv (os.P_WAIT, './stable.pl', args)
        if rv == 0:
            print "...UNSTABLE..."
        elif rv == 1:
            print "...MISSING NODES???"
        elif rv == 17:
            print "!!!STABLE!!!"
        else:
            print "=== rv:", rv
    

class ProtocolError (Exception):
    def __init__ (self, val_):
        self.val = val_
    def __str__ (self):
        return self.val

class ChordMonitorServer (DaemonThreadingMixIn, TCPServer): pass
    
class ChordMonitorHandler (StreamRequestHandler):
    def fname (self):
        return "%s/%s" % (self.server.workdir, self.hostname)
    def ready (self):
        return self.ready
        
    def setup (self):
        StreamRequestHandler.setup (self)

        print "Accepting connection from", self.client_address
        greeting = self.rfile.readline ()
        args = string.split (greeting)
        if len (args) != 3:
            raise ProtocolError ('Invalid number of HELO arguments: %d' % len (args))
        if args[0] != 'HELO':
            raise ProtocolError ('Unexpected protocol message %s' % args[0])
        
        try:
            self.vnodes = int (args[2])
        except ValueError:
            raise ProtocolError ('Bad number of vnodes %s' % args[2])
        
        self.hostname = args[1]
        
        print "Session established from", self.hostname, "with",
        print self.vnodes, "vnodes."

        add_connection (self)

    def handle (self):
        line = self.rfile.readline ()
        in_stats = 0
        while line != '':
            c = string.split (line, None)
            if not in_stats:
                if c[0] == 'STATS':
                    self.ready = 0
                    in_stats = 1
                    self.fh = open (self.fname (), 'w') # truncate
                    print "New stats from", self.hostname
                    self.fh.write ("CHORD NODE STATS %s\n" % c[1]) # for stable.pl
            else:
                if c[0] == '========':
                    self.fh.write ("myID is %s\n" % c[1])
                self.fh.write(line)
                if c[0] == 'ENDSTATS':
                    self.fh.close ()
                    in_stats = 0
                    self.ready = 1
            line = self.rfile.readline ()

    def finish (self):
        StreamRequestHandler.finish (self)
        del_connection (self)
        print "Closing connection to", self.hostname
        os.rename (self.fname (), "%s.%d" % (self.fname (), int(time.time ())))


def main ():
    if len (sys.argv) < 3:
        sys.stderr.write ("Usage: chord-monitor.py workdir port\n")
        sys.exit (1)

    os.umask (022)
    workdir = sys.argv[1]
    port = int (sys.argv[2])
    t = Thread (target = processor)
    t.setDaemon (1)
    t.start ()
    
    try:
        srv = ChordMonitorServer (('', port), ChordMonitorHandler)
        srv.workdir = workdir # "subclassing"
        
        print 'ChordMonitorServer starting on port %d...' % port
        srv.serve_forever ()
    except KeyboardInterrupt:
        print '^C received, shutting down server'
        exitflag.set ()
        sys.exit ()

if __name__ == '__main__':
    main()
