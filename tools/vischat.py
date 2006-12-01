import asynchat
import socket
import errno

class vischat (asynchat.async_chat):
    def __init__ (self, host, port):
        self.host = host
        self.port = port
        self.outstanding = []
        self.lines = []
        self.buffer = ""
        asynchat.async_chat.__init__ (self)

    def handle_connect (self):
        err = self.getsockopt (socket.SOL_SOCKET, socket.SO_ERROR)
        if err == errno.ECONNREFUSED:
            self.connect_cb (None)
        else:
            self.connect_cb (self)

    def start_connect (self, cb):
        self.create_socket (socket.AF_INET, socket.SOCK_STREAM)
        self.connect ((self.host, self.port))
        self.set_terminator ("\n")
        self.connect_cb = cb

    def collect_incoming_data (self, data):
        self.buffer += data
        
    def found_terminator (self):
        # Assumes that vis handles all request in order.
        # print "### %s" % self.buffer
        if self.buffer[0] == '.':
            z = self.outstanding.pop (0)
            if z:
                z (self.lines)
            self.lines = []
        else:
            self.lines.append (self.buffer)
        self.buffer = ""

    # Each command here is a front-end to a real message that could get
    # sent to vis, and a callback that should notify 
    def list (self, cb):
        self.push ("list\n")
        self.outstanding.append (cb)
    def arc (self, a, b):
        self.push ("arc %s %s\n" % (a, b))
        self.outstanding.append (None)
    def arrow (self, a, b):
        self.push ("arrow %s %s\n" % (a, b))
        self.outstanding.append (None)
    def reset (self):
        self.push ("reset\n")
        self.outstanding.append (None)
    def highlight (self, a):
        self.push ("highlight %s\n" % a)
        self.outstanding.append (None)
    def select (self, a):
        self.push ("select %s\n" % a)
        self.outstanding.append (None)
