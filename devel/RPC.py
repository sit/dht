import asynchat
import errno
import socket
import struct
import RPCProto
import string
import sys
from xdrlib import Packer, Unpacker
from traceback import print_exc
from SocketServer import ThreadingTCPServer

# FIXME: get rid of these...
VERSION           = RPCProto.RPC_VERSION
CALL              = RPCProto.CALL
REPLY             = RPCProto.REPLY
AUTH_NONE         = RPCProto.AUTH_NONE
AUTH_SYS          = RPCProto.AUTH_SYS
AUTH_SHORT        = RPCProto.AUTH_SHORT
MSG_ACCEPTED      = RPCProto.MSG_ACCEPTED
MSG_DENIED        = RPCProto.MSG_DENIED
RPC_MISMATCH      = RPCProto.RPC_MISMATCH   # RPC version number != 2
AUTH_ERROR        = RPCProto.AUTH_ERROR     # remote can't authenticate caller
SUCCESS           = RPCProto.SUCCESS        # RPC executed successfully
PROG_UNAVAIL      = RPCProto.PROG_UNAVAIL   # remote hasn't exported program
PROG_MISMATCH     = RPCProto.PROG_MISMATCH  # remote can't support version #
PROC_UNAVAIL      = RPCProto.PROC_UNAVAIL   # program can't support procedure
GARBAGE_ARGS      = RPCProto.GARBAGE_ARGS   # procedure can't decode params
SYSTEM_ERR        = RPCProto.SYSTEM_ERR     # errors like memory allocation
AUTH_OK           = RPCProto.AUTH_OK           # success
AUTH_BADCRED      = RPCProto.AUTH_BADCRED      # bad credential (seal broken)
AUTH_REJECTEDCRED = RPCProto.AUTH_REJECTEDCRED # client must begin new session
AUTH_BADVERF      = RPCProto.AUTH_BADVERF      # bad verifier (seal broken)
AUTH_REJECTEDVERF = RPCProto.AUTH_REJECTEDVERF # verifier expired or replayed
AUTH_TOOWEAK      = RPCProto.AUTH_TOOWEAK      # rejected for security reasons
AUTH_INVALIDRESP  = RPCProto.AUTH_INVALIDRESP  # bogus response verifier
AUTH_FAILED       = RPCProto.AUTH_FAILED       # reason unknown

PROC_NULL = 0
NULL_AUTH = RPCProto.opaque_auth()
NULL_AUTH.flavor = RPCProto.AUTH_NONE
NULL_AUTH.body = ''


def parse_frag_len(data):
    if len(data) < 4:
        raise EOFError, "no fraglen"
    fraglen = struct.unpack('>L', data[:4])[0]
    lastfrag = fraglen & 0x80000000
    fraglen = fraglen & 0x7fffffff
    return (fraglen, lastfrag)

def writefrags(message, write):
    """Fragments message and writes the fragments using write.

    This procedure consumes message, so caller should
    save a copy if needed.
    """
    # TODO: use StringIO
    while message:
        frag = message[0:0x7fffffff]
        fraglen = len(frag)
        message = message[fraglen:]
        if not message:
            fraglen = fraglen | 0x80000000
        fraglen = struct.pack('>L', fraglen)
        write(fraglen)
        write(frag)

def readfrags(read):
    """Reads fragments using read and returns the assembled message.

    Raises EOFError if unable to read the whole message.
    """
    # TODO: use StringIO
    message = ''
    while 1:
        fraglen = read(4)
        (fraglen, lastfrag) = parse_frag_len(fraglen)
        frag = read(fraglen)
        if len(frag) < fraglen:
            raise EOFError, "frag too short"
        message += frag
        if lastfrag:
            return message
    raise AssertionError, "should not get here"

class UnpackException(Exception):
    pass

class ReplyException(Exception):
    """An exception that carries a reply packet"""
    def __init__(self, message, reply):
        Exception.__init__(self)
        self.message = message
        self.reply = reply
    def __str__(self):
        return self.message

def pack_reply(xid, *args):
    """Packs an RPC reply from a variable-length arg list (args):
    MSG_ACCEPTED, verf, (SUCCESS | PROG_MISMATCH, low, hi | PROG_UNAVAIL
                         | PROC_UNAVAIL | GARBAGE_ARGS | SYSTEM_ERR)
    MSG_DENIED, (RPC_MISMATCH, hi, low | AUTH_ERROR, auth_stat)

    verf is an auth of the form (flavor, value)
    Returns an xdrlib.Packer that the caller can use to add data,
    such as the results of a SUCCESSful call.
    """
    arg = list(args) # need a mutable list for pop()
    msg = RPCProto.rpc_msg()
    msg.xid = xid
    msg.body = RPCProto.body_t()
    msg.body.mtype = RPCProto.REPLY
    msg.body.rbody = reply = RPCProto.reply_body()
    reply.stat = reply_stat = arg.pop(0)
    if reply_stat == MSG_ACCEPTED:
        reply.areply = RPCProto.accepted_reply()
        reply.areply.verf = verf = arg.pop(0)
        reply.areply.reply_data = RPCProto.reply_data_t()
        reply.areply.reply_data.stat = accept_stat = arg.pop(0)
        if accept_stat == PROG_MISMATCH:
            reply.areply.reply_data.mismatch_info = RPCProto.mismatch_info_t()
            reply.areply.reply_data.mismatch_info.low  = arg.pop(0)
            reply.areply.reply_data.mismatch_info.high = arg.pop(0)
        elif (accept_stat == SUCCESS):
            reply.areply.reply_data.results = '' # FIXME?
        elif (accept_stat == PROG_UNAVAIL or
              accept_stat == PROC_UNAVAIL or
              accept_stat == GARBAGE_ARGS or
              accept_stat == SYSTEM_ERR):
            pass
        else:
            raise ValueError("unknown accept_stat: %u" % accept_stat)
    elif reply_stat == MSG_DENIED:
        reply.rreply = RPCProto.rejected_reply()
        reply.rreply.stat = reject_stat = arg.pop(0)
        if reject_stat == RPC_MISMATCH:
            reply.rreply.mismatch_info.low = RPCProto.mismatch_info_t()
            reply.rreply.mismatch_info.low = arg.pop(0)
            reply.rreply.mismatch_info.high = arg.pop(0)
        elif reject_stat == AUTH_ERROR:
            reply.rreply.astat = arg.pop(0)
        else:
            raise ValueError("unknown reject_stat: %u" % reject_stat)
    else:
        raise ValueError("unknown reply_stat: %u" % reply_stat)
    p = Packer()
    RPCProto.pack_rpc_msg(p, msg)
    return p

def check(expected, actual, name, replyf=None):
    """If expected is not None, checks whether expected equals actual,
    and if not, raises an exception (see code for themessage).
    If replyf is None, the exception is an UnpackException.
    Otherwise, reply must be a function that takes no arguments
    and returns an RPC reply (a string), and the exception is a
    ReplyException containing the output of the function.
    """
    if expected is not None and expected != actual:
        if replyf:
            raise ReplyException("Expected %s %s, but got %s"
                                  % (name, expected, actual), replyf())
        else:
            raise UnpackException("Expected %s %s, but got %s"
                                  % (name, expected, actual))

def unpack_reply(response, myxid=None, myreply_stat=MSG_ACCEPTED,
                 myverf=NULL_AUTH, myaccept_stat=SUCCESS,
                 myreject_stat=None, myauth_stat=None):
    """Unpacks an RPC reply and returns a variable-length arg list
    of the same form as the argument to pack_reply, but for SUCCESS also
    returns an xdrlib.Unpacker as the final element of the list
    that the caller can use to unpack the results of the call.

    If values are given for any myXXX arguments, checks that those
    values match the unpacked XXX values.  Default myXXX values assume
    success with no authentication.
    
    Raises UnpackException on any errors or mismatches.
    """
    u = Unpacker(response)
    msg = RPCProto.unpack_rpc_msg(u)
    check(myxid, msg.xid, "xid")
    if msg.body.mtype == RPCProto.CALL:
        raise UnpackException("Expected reply, but got call")
    reply = msg.body.rbody
    check(myreply_stat, reply.stat, "reply_stat")
    retval = [msg.xid, reply.stat]
    if reply.stat == RPCProto.MSG_ACCEPTED:
        check(myverf, reply.areply.verf, "verf")
        retval.append(reply.areply.verf)
        accept_stat = reply.areply.reply_data.stat
        check(myaccept_stat, accept_stat, "accept_stat")
        retval.append(accept_stat)
        if accept_stat == RPCProto.SUCCESS:
            retval.append(u)
        elif accept_stat == RPCProto.PROG_MISMATCH:
            retval.append(reply.areply.reply_data.mismatch_info.low)
            retval.append(reply.areply.reply_data.mismatch_info.high)
        elif (accept_stat == RPCProto.PROG_UNAVAIL or
              accept_stat == RPCProto.PROC_UNAVAIL or
              accept_stat == RPCProto.GARBAGE_ARGS or
              accept_stat == RPCProto.SYSTEM_ERR):
            pass
        else:
            raise UnpackException("unknown accept_stat: %u" % accept_stat)
    elif reply.stat == RPCProto.MSG_DENIED:
        reject_stat = reply.rreply.stat
        check(myreject_stat, reject_stat, "reject_stat")
        retval.append(reject_stat)
        if reject_stat == RPCProto.RPC_MISMATCH:
            retval.append(reply.rreply.mismatch_info.low)
            retval.append(reply.rreply.mismatch_info.high)
        elif reject_stat == RPCProto.AUTH_ERROR:
            check(myauth_stat, reply.rreply.astat, "auth_stat")
            retval.append(reply.rreply.astat)
        else:
            raise UnpackException("unknown reject_stat: %u" % reject_stat)
    else:
        raise UnpackException("unknown reply_stat: %u" % reply.stat)
    return retval
    
def pack_call(xid, prog, vers, proc,
              cred=NULL_AUTH, verf=NULL_AUTH):
    """Packs an RPC call message; returns an xdrlib.Packer that
    the caller can use to add more data, e.g., the call arguments.
    """
    msg = RPCProto.rpc_msg()
    msg.xid = xid
    msg.body = RPCProto.body_t()
    msg.body.mtype = RPCProto.CALL
    msg.body.cbody = RPCProto.call_body()
    msg.body.cbody.rpcvers = RPCProto.RPC_VERSION
    msg.body.cbody.prog = prog
    msg.body.cbody.vers = vers
    msg.body.cbody.proc = proc
    msg.body.cbody.cred = cred
    msg.body.cbody.verf = verf
    p = Packer()
    RPCProto.pack_rpc_msg(p, msg)
    return p

def unpack_call(request, myprog=None, myvers=None,
                mycred=NULL_AUTH, myverf=NULL_AUTH):
    """Unpacks an RPC call message from request.

    Returns (xid, prog, vers, proc, cred, verf, u) if okay,
    where u is an xdrlib.Unpacker.
    otherwise raises either UnpackException or ReplyException.
    If myXXX is not None, checks that XXX == myXXX.
    Assumes AUTH_NONE for cred and verf; override with mycred and myverf.
    """
    if len(request) < 24:
        raise UnpackException("Packet too short (%d bytes)" % len(request))
    u = Unpacker(request)
    msg = RPCProto.unpack_rpc_msg(u)
    if msg.body.mtype == RPCProto.REPLY:
        raise UnpackException("Expected call, but got reply")
    call = msg.body.cbody
    check(RPCProto.RPC_VERSION, call.rpcvers, "RPC version",
          lambda: pack_reply(msg.xid,
                             RPCProto.MSG_DENIED,
                             RPCProto.RPC_MISMATCH,
                             RPCProto.RPC_VERSION,
                             RPCProto.RPC_VERSION).get_buffer())
    check(myprog, call.prog, "program",
          lambda: pack_reply(msg.xid,
                             RPCProto.MSG_ACCEPTED,
                             NULL_AUTH,
                             RPCProto.PROG_UNAVAIL).get_buffer())
    check(myvers, call.vers, "version",
          lambda: pack_reply(msg.xid,
                             RPCProto.MSG_ACCEPTED,
                             NULL_AUTH,
                             RPCProto.PROG_MISMATCH,
                             myvers,
                             myvers).get_buffer())
    check(mycred, call.cred, "cred",
          lambda: pack_reply(msg.xid,
                             RPCProto.MSG_DENIED,
                             RPCProto.AUTH_ERROR,
                             RPCProto.AUTH_BADCRED).get_buffer())
    check(myverf, call.verf, "verf",
          lambda: pack_reply(msg.xid,
                             RPCProto.MSG_DENIED,
                             RPCProto.AUTH_ERROR,
                             RPCProto.AUTH_BADVERF).get_buffer())
    return (msg.xid, call.prog, call.vers,
            call.proc, call.cred, call.verf, u)

class ReuseTCPServer(ThreadingTCPServer):
    def __init__(self, addr, handler):
        self.allow_reuse_address = 1
        ThreadingTCPServer.__init__(self, addr, handler)

class Server:
    def __init__(self, module, PROG, VERS, port, handlers, name=None):
        """If name is not None, Server prints debug messages
        prefixed by name."""
        assert module is not None
        assert 'programs' in dir(module)
        assert PROG in module.programs
        assert VERS in module.programs[PROG]
        for proc in handlers:
            assert proc in module.programs[PROG][VERS]
        import SocketServer
        class StreamHandler(SocketServer.StreamRequestHandler):
            def dispatch(self, request):
                xid, prog, vers, proc, cred, verf, u = unpack_call(
                    request, myprog=PROG, myvers=VERS)
                if proc in handlers:
                    if name:
                        print name + ": Got proc", proc
                    arg = module.programs[PROG][VERS][proc].unpack_arg(u)
                    res = handlers[proc](xid, cred, verf, arg)
                    p = pack_reply(xid, MSG_ACCEPTED, NULL_AUTH, SUCCESS)
                    module.programs[PROG][VERS][proc].pack_res(p, res)
                    return p.get_buffer()
                else:
                    # no such procedure
                    if name:
                        print name + ": Got unknown proc", proc
                    return pack_reply(xid, MSG_ACCEPTED, NULL_AUTH,
                                      PROC_UNAVAIL).get_buffer()
            def handle(self):
                rfile = self.request.makefile('rb', -1)
                wfile = self.request.makefile('wb', -1)
                if name:
                    print name + ": Got connection from", self.client_address
                while 1:
                    try:
                        request = readfrags(rfile.read)
                        reply = self.dispatch(request)
                        writefrags(reply, wfile.write)
                        wfile.flush()
                    except EOFError:
                        return
                    except UnpackException, e:
                        if name:
                            print name + ":", e
                        return
                    except ReplyException, e:
                        if name:
                            print name + ":", e
                        writefrags(e.reply, wfile.write)
                        wfile.flush()
                    except:
                        if name:
                            print name + ": Unexpected error:"
                            print_exc()
                        return # avoid killing the server
        self.handler = StreamHandler
        self.port = port
    def run(self):
        server = ReuseTCPServer(('', self.port), self.handler)
        server.serve_forever() # support UDP?
        
class XidGenerator:
    # FIXME: should this use locks?
    def __init__(self):
        import random, sys
        self.xid = random.randint(0, sys.maxint/2)
    # FIXME: should this randomize xids?
    def next(self):
        self.xid += 1
        return self.xid

class ClientBase:
    def __init__(self, module, PROG, VERS, host, port):
        assert module is not None
        assert 'programs' in dir(module)
        assert PROG in module.programs
        assert VERS in module.programs[PROG]
        self.PROG = PROG
        self.VERS = VERS
        self.module = module
        self.xidgen = XidGenerator()
        self.host = host
        self.port = port

    def start_connect(self, host, port, cb = None):
         raise AssertionError, "This method must be overridden."
     
    def __call__(self, pnum, arg, cb = None):
        """Call proc number pnum with arg.
           If answer is immediately available, it will be returned.
           Otherwise, None is returned, and cb will be called later."""
        raise AssertionError, "This method must be overridden."

class SClient(ClientBase):
    def start_connect(self, cb = None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.rfile = self.sock.makefile('rb', -1)
        self.wfile = self.sock.makefile('wb', -1)
        return self.sock
        # TODO: use exec to define methods on this object for each proc
        # in programs[PROG][VERS].  This requires that each generated
        # Procedure() object include the proc name as a string.
        # Probably a good idea, and for the Program and Version objects,
        # too.  Also, rename all these to RPCProcedure, RPCProgram,
        # RPCVersion.
    def write_request(self, request):
        writefrags(request, self.wfile.write)
        self.wfile.flush()
    def read_reply(self):
        return readfrags(self.rfile.read)
    def __call__(self, pnum, arg, cb = None):
        proc = self.module.programs[self.PROG][self.VERS][pnum]
        p = pack_call(self.xidgen.next(), self.PROG, self.VERS, pnum)
        proc.pack_arg(p, arg)
        request = p.get_buffer()
        self.write_request(request)
        reply = self.read_reply()
        u = unpack_reply(reply)[-1]
        return proc.unpack_res(u)

class strbuf:
    """Unlike stringio, always append to the end of string, read from front."""
    def __init__(self): self.s = ''
    def write(self, s): self.s += s
    def read(self, n):
        # Slicing past the end of string returns '', working out well.
        v = self.s[:n]
        self.s = self.s[n:]
        return v

class AClient(ClientBase,asynchat.async_chat):
    def __init__(self, module, PROG, VERS, host, port):
        # A table of callbacks to be called, keyed by xid.
        self.xidcbmap = {}
        self.inbuffer = ''
        self.fragments = []
        self.bytesleft = 0 # until end of current fragment.
        asynchat.async_chat.__init__(self)
        ClientBase.__init__(self, module, PROG, VERS, host, port)

    def handle_connect(self):
        err = self.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err == errno.ECONNREFUSED:
            self.connect_cb(None)
        else:
            self.connect_cb(self)
    
    def start_connect(self, cb = None):
        if cb is None:
            raise TypeError, "Must pass cb to async client"
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect_cb = cb
        self.connect((self.host, self.port))
        self.set_terminator(None)
        return None

    def handle_reply(self):
        reply = ''.join(self.fragments)
        self.fragments = []
        u = unpack_reply(reply)
        # print "Reply for xid %x" % u[0]
        try:
            (cb, proc) = self.xidcbmap[u[0]]
            del self.xidcbmap[u[0]]
        except KeyError:
            sys.stderr.write("Reply for unknown xid %x received: %s" %
                             (u[0], str(u[1:])))
            return

        if not cb:
            return
        # XXX should really return some useful info to cb if error case
        #     either if denied, or if some weird bug like PROG_UNAVAIL.
        if u[1] == RPCProto.MSG_ACCEPTED:
            res = proc.unpack_res(u[-1])
            cb(res)
        else:
            cb(None)

    def collect_incoming_data(self, data):
        if len(self.inbuffer) > 0:
            data = self.inbuffer + data
        (fraglen, lastfrag) = parse_frag_len(data)
        # print "a", fraglen, lastfrag, len(data)
        while 4 + fraglen <= len(data):
            frag = data[4:4+fraglen]
            self.fragments.append(frag)
            if lastfrag:
                self.handle_reply()
            data = data[4+fraglen:]
            if len(data) > 0:
                (fraglen, lastfrag) = parse_frag_len(data)
                # print "b", fraglen, lastfrag, len(data)
            # else:
                # print "c"

        self.inbuffer = data
            
    def found_terminator(self):
        raise AssertionError, "We don't use terminators."

    def __call__(self, pnum, arg, cb = None):
        proc = self.module.programs[self.PROG][self.VERS][pnum]
        xid = self.xidgen.next()
        # print "Call for xid %x" % xid
        p = pack_call(xid, self.PROG, self.VERS, pnum)
        proc.pack_arg(p, arg)
        request = p.get_buffer()
        val = strbuf()
        writefrags(request, val.write)
        self.push(val.s)
        self.xidcbmap[xid] = (cb, proc)
        return None
