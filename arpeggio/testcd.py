#!/usr/bin/env python

"""A simple client to test and frobnicate cd."""

import sys, os, socket
sys.path.append("../devel/")  # XXX Need RPC library

import RPC, asyncore
import chord_types, cd_prot

class OneShotServer(asyncore.dispatcher):

    """A one-shot server that sits on a UNIX socket.

    This listens on a UNIX domain socket.  Once a connection comes in,
    the passive socket is shut down and another dispatcher is
    constructed around the incoming connection."""

    def __init__(self, cb):
        asyncore.dispatcher.__init__(self)
        self.__sock = os.tempnam(None, "arpcc")
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.bind(self.__sock)
        self.listen(1)
        self.__cb = cb

    def get_sock_name(self):
        """Return the name of the listening UNIX socket created for
        this server."""

        return self.__sock

    def handle_accept(self):
        """Accept an incoming connection, destroy the listening
        socket, and call the registered callback."""

        (sock, addr) = self.accept()
        os.unlink(self.get_sock_name())
        self.close()
        self.__cb(sock, addr)

    def writable(self):
        """Indicate that this socket should never be included in the
        write set.

        This overrides the default writable, which always returns
        true, leading to useless write notifications."""

        return False

class AConnectedClient(RPC.AClient):

    """Modification of RPC.AClient that supports wrapping an existing
    connection.

    The existing connection can be any socket type (including UNIX
    domain sockets).  The addr argument should reflect the address of
    the remote endpoint of the socket.

    Because the socket already exists, many of the methods usually
    used to set up an RPC.AClient are unnecessary (and will raise
    exceptions if attempted)"""
    
    def __init__(self, module, prog, vers, sock, addr):
        RPC.AClient.__init__(self, module, prog, vers, None, None)
        self.set_socket(sock)
        self.socket.setblocking(0)
        self.connected = 1
        self.addr = addr
        # The following would usually be done by AClient.start_connect
        self.set_terminator(None)

    def create_socket(self, family, type):
        """Connected clients cannot create sockets."""
        raise RuntimeError, "Already have a socket for connected client"

    def connect(self, address):
        """Connected clients cannot connect to another address."""
        raise RuntimeError, "Already have a socket for connected client"

    def start_connect(self, cb = None):
        """Connected clients  cannot start connecting to anything."""
        raise RuntimeError, "start_connect unnecessary with connected client"


client = None
options = None

def CD(pnum, arg, cb = None):
    """Call an RPC in cd (works with both sync and async clients)

    This is a simple wrapper around the client RPC call to make it a
    bit more, er, callable."""
    
    res = client(pnum, arg, cb)
    if res is not None and cb is not None:
        cb(res)

def do_lookup(vnodeid, id):
    """Performs a lookup of id from vnode vnodeid.

    Currently this simply displays the results of the lookup when the
    lookup call completes.  Not useful in general, but this function
    includes some bits of code for decoding the returned information
    that may prove useful later."""

    def itos(val):
        """Converts an integer into the equivalent four-character
        string.

        Unfortunately, the Python sockets API is mixed with respect to
        which functions expect integers and which functions expect
        four-character strings (why do _any_ of them expect strings?).
        This function exists to make peace between the two worlds."""
        return (chr((val>>24)&0xFF) + chr((val>>16)&0xFF) +
                chr((val>>8)&0xFF) + chr(val&0xFF))

    arg = cd_prot.cd_lookup_arg()

    arg.vnode = chord_types.bigint(vnodeid)
    arg.key = chord_types.bigint(id)

    def lookupcb(res):
        if res.stat == chord_types.CHORD_OK:
            print "Found", arg.key, "at:"
            for wire in res.resok.route:
                print "%s:%d (%d)" % \
                      (socket.inet_ntoa(itos(wire.machine_order_ipv4_addr)),
                       wire.machine_order_port_vnnum>>16,
                       wire.machine_order_port_vnnum&0xFFFF)
        else:
            print "Lookup failed:", res.stat
    print "Looking up", arg.key, "via", arg.vnode
    CD(cd_prot.CD_LOOKUP, arg, lookupcb)

def start_vnode():
    """Start a new vnode and do a demonstration lookup."""
    arg = cd_prot.cd_newvnode_arg()

    arg.routing_mode = cd_prot.MODE_CHORD

    def newvnodecb(res):
        if res.stat == chord_types.CHORD_OK:
            print "Vnode created:", res.resok.vnode
            do_lookup(res.resok.vnode, res.resok.vnode + 1)
        else:
            print "Vnode creation failed:", res.stat
    print "Starting vnode"
    CD(cd_prot.CD_NEWVNODE, arg, newvnodecb)

def start_chord():
    """Start a Chord instance and launch a single vnode when done.

    Since cd is a very thin wrapper around the Chord API, it is
    necessary to initialize the chord object with the appropriate
    arguments.  Once the Chord object has been created, this calls
    start_vnode."""
    
    arg = cd_prot.cd_newchord_arg()

    if options.bootstrap:
        arg.wellknownhost = ""
        arg.wellknownport = options.port
    else:
        arg.wellknownhost = socket.gethostbyname(options.join[0])
        arg.wellknownport = options.join[1]
    arg.myname = ""
    arg.myport = options.port
    arg.maxcache = 64  # XXX Uh

    def newchordcb(res):
        if res.stat == chord_types.CHORD_NOTINRANGE:
            print "Chord object already exists, continuing anyways"
        else:
            print "Chord object created"
        start_vnode()
    print "Instantiating Chord"
    CD(cd_prot.CD_NEWCHORD, arg, newchordcb)

def start_client():
    """Launch cd and connection with it.

    The approach this takes is somewhat backwards.  Despite the fact
    that cd acts as an RPC server, because it needs to be launched by
    cc and have a one-to-one connection with it, cc acts as a one-shot
    server that accepts the immediate connection that cd makes to it
    over the provided UNIX domain socket when cd is started.
    
    It really does make sense if you think of the transport
    server/client and the RPC server/client as separate things.
    Really.  (If X can abuse the terms server and client, so can we!)
    """

    def connectcb(sock, addr):
        global client
        print "Initating RPC with cd"
        client = AConnectedClient(cd_prot, cd_prot.CD_PROGRAM, 1, sock, addr)
        start_chord()
    print "Starting one-shot server for cd"
    cdsock = OneShotServer(connectcb).get_sock_name()

    print "Starting cd on", cdsock
    # XXX Need a way to pass other arguments to cd
    os.spawnl(os.P_NOWAIT, "./cd", "cd", "-C", cdsock)

def start():
    """Parse command-line arguments and start the client.

    The parsed command line arguments are dumped into the options
    global.  If an optional option is not specified, its value will be
    filled with a reasonable default.

    options.port -- The port to run Chord on (always valid)
    options.bootstrap -- True iff this is a bootstrap node
    options.join -- (host, port) of another cc node to join with or None
       if this is a bootstrap node"""
    
    global options
    import optparse

    # Build the parser
    parser = optparse.OptionParser()
    parser.add_option("-b", action="store_true", dest="bootstrap",
                      default=False,
                      help="indicates this is a bootstrap node (exclusive"
                      " with -j)")
    def join_arg(option, opt, value, parser):
        """Parse host:port value of join option"""
        parts = value.split(":")
        if len(parts) != 2:
            raise optparse.OptionValueError, \
                  "option %s: must specify host:port" % opt
        host, port = parts
        port = int(port)
        parser.values.join = (host, port)
    parser.add_option("-j", type="string", action="callback",
                      callback=join_arg, dest="join", metavar="HOST:PORT",
                      help="specifies existing Chord node to join with")
    parser.add_option("-p", type="int", dest="port",
                      help="indicates local port to listen on for Chord"
                      " connections (defaults to joined node's port)")

    # Parse and validate argments
    (options, args) = parser.parse_args()
    if args:
        parser.error("too many arguments specified")
    if not ((options.join is None) ^ (not options.bootstrap)):
        parser.error("must specify either -j or -b")
    if options.port is None:
        if options.join is None:
            parser.error("-p is required when -j is not specified")
        options.port = options.join[1]

    start_client()

if __name__ == "__main__":
    start()
    asyncore.loop()
    print "Left asyncore loop, exiting"

