/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include "chord.h"
#include "chord_util.h"

#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>
#include "comm.h"
#include "route.h"
#include <transport_prot.h>

#include <modlogger.h>
#define trace modlogger ("chord")

#include <configurator.h>

static struct chord_config_init {
  chord_config_init ();
} cci;

chord_config_init::chord_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int
#define set_str Configurator::only ().set_str
  ok = ok && set_int ("chord.nsucc", 16);
  ok = ok && set_int ("chord.npred", 1);
  ok = ok && set_int ("chord.ncoords", 3);

  ok = ok && set_str ("chord.rpc_mode", "stp");

  /** don't use the lookup_mode's lookup_closestsucc; use
   *  the greedy metric instead.  Probably desirable if toes are
   *  enabled. */
  ok = ok && set_int ("chord.greedy_lookup", 0);
  /** try to terminate retrieves early if the hop has
   *  returned a "sufficient" number of successors.  */
  ok = ok && set_int ("chord.find_succlist_shaving", 1);

  /**
   * Determine how to find the "closest known predecessor to x".
   * Legal values currently include:
   *  fingerlike: only use fingerlike (e.g. finger table)
   *  fingersandsuccs: look at all fingers + successors
   *  loctable: look at any cached location
   *  proximity: look at the nodes discovered by toes
   *             (falling back to successors)
   */
  ok = ok && set_str ("chord.lookup_mode", "fingerlike");
  assert (ok);
#undef set_int
#undef set_str
}

int logbase;  // base = 2 ^ logbase

chord::chord (str _wellknownhost, int _wellknownport, 
	      str _myname, int port, int max_cache,
	      int l_mode, int _logbase) :
  myname (_myname),
  lookup_mode (l_mode),
  nrcv (NULL),
  rpcm (NULL),
  active (NULL)
{
  logbase = _logbase;

  str rpcstr;
  bool ok = Configurator::only ().get_str ("chord.rpc_mode", rpcstr);
  assert (ok);

  chord_rpc_style = CHORD_RPC_STP;
  if (rpcstr == "tcp" || rpcstr == "TCP")
    chord_rpc_style = CHORD_RPC_SFST;
  else if (rpcstr == "udp" || rpcstr == "UDP")
    chord_rpc_style = CHORD_RPC_SFSU;

  myport = startchord (port);
  warnx << "chord: running on " << myname << ":" << myport << "\n";

  nrcv = New refcounted<u_int32_t>;
  *nrcv = 0;
  if (chord_rpc_style == CHORD_RPC_SFSU)
    rpcm = New refcounted<rpc_manager> (nrcv);
  else if ((chord_rpc_style == CHORD_RPC_SFST) || (chord_rpc_style == CHORD_RPC_SFSBT))
    rpcm = New refcounted<tcp_manager> (nrcv);
  else if (chord_rpc_style == CHORD_RPC_STP)
    rpcm = New refcounted<stp_manager> (nrcv);
  else
    fatal << "bad chord_rpc_style value: " << chord_rpc_style << "\n";

  locations = New refcounted<locationtable> (max_cache);

  chord_node wkn;
  wkn.r.hostname = _wellknownhost;
  wkn.r.port = _wellknownport ? _wellknownport : myport;
  wkn.x = make_chordID (wkn.r.hostname,
				   wkn.r.port);
  wkn.vnode_num = 0;
  wkn.coords.setsize (NCOORDS);
  // Make up some random initial information for this other node.
  for (int i = 0; i < NCOORDS; i++)
    wkn.coords[i] = (int) uniform_random_f (1000000.0);

  if (myname != _wellknownhost || myport != _wellknownport) {
    wellknown_node = locations->insert (wkn);
    if (!wellknown_node) {
      warn << "Well known host failed to verify! Bailing.\n";
      exit (0);
    }
  }

  nvnode = 0;
  srandom ((unsigned int) (getusec() & 0xFFFFFFFF));
}

void
chord::tcpclient_cb (int srvfd)
{
  int fd = accept (srvfd, NULL, NULL);
  if (fd < 0)
    warn << "chord: accept failed " << strerror (errno) << "\n";
  else {
    ptr<axprt> x = axprt_stream::alloc (fd, 230000);

    ptr<asrv> s = asrv::alloc (x, transport_program_1);
    s->setcb (wrap (mkref(this), &chord::dispatch, s));
  }
}


int
chord::startchord (int myp, int type)
{
  
  int srvfd = inetsocket (type, myp);
  if (srvfd < 0)
    fatal ("binding %s port %d: %m\n",
	   (type == SOCK_DGRAM ? "UDP" : "TCP"), myp);
  
  if (myp == 0) {
    struct sockaddr_in addr;
    socklen_t len = sizeof (addr);
    bzero (&addr, sizeof (addr));
    if (getsockname (srvfd, (sockaddr *) &addr, &len) < 0) 
      fatal ("getsockname failed %m\n");
    myp = ntohs (addr.sin_port);
  }

  
  if (type == SOCK_DGRAM) {
    x_dgram = axprt_dgram::alloc (srvfd, sizeof(sockaddr), 230000);
    ptr<asrv> s = asrv::alloc (x_dgram, transport_program_1);
    s->setcb (wrap (mkref(this), &chord::dispatch, s));
  }
  else {
    int ret = listen (srvfd, 1000);
    assert (ret == 0);
    fdcb (srvfd, selread, wrap (this, &chord::tcpclient_cb, srvfd));
  }
  
  return myp;
}


int
chord::startchord (int myp)
{
  // see also locationtable constructor.
  if ((chord_rpc_style == CHORD_RPC_SFST) || (chord_rpc_style == CHORD_RPC_SFSBT))  {
    // Ensure the DGRAM and STREAM sockets are on same port #,
    // since it is included in the Chord ID's hash.
    myp = startchord (myp, SOCK_STREAM);
  }

  return startchord (myp, SOCK_DGRAM);
}



ptr<vnode>
chord::newvnode (cbjoin_t cb, ptr<fingerlike> fingers, ptr<route_factory> f)
{
  if (nvnode > max_vnodes)
    fatal << "Maximum number of vnodes (" << max_vnodes << ") reached.\n";
    
  chordID newID = make_chordID (myname, myport, nvnode);
  warnx << gettime () << ": creating new vnode: " << newID << "\n";

  vec<float> coords;
  warn << gettime () << " coords are: ";
  for (int i = 0; i < NCOORDS; i++) {
    coords.push_back (uniform_random_f (1000000.0));
    warnx << (int) coords[i] << " " ;
  }
  warnx << "\n";
  ptr<location> l = locations->insert (newID, myname, myport, nvnode, coords);
  if (wellknown_node == NULL)
    wellknown_node = l;
  locations->pin (newID);

  ptr<vnode> vnodep = vnode::produce_vnode (locations, rpcm, fingers, f,
					    mkref (this), newID, 
					    nvnode,
					    lookup_mode);
  f->setvnode (vnodep);
  
  if (!active) active = vnodep;
  nvnode++;
  vnodes.insert (newID, vnodep);
  
  if (newID != wellknown_node->id ()) {
    vnodep->join (wellknown_node, cb);
  } else {
    vnodep->stabilize ();
    (*cb) (vnodep, CHORD_OK);
  }
  return vnodep;
}

void
chord::stats_cb (const chordID &k, ptr<vnode> v) { 
  v->stats();
}

void
chord::stats ()
{
  warnx << "CHORD NODE STATS\n";
  warnx << "# vnodes: " << nvnode << "\n";
  vnodes.traverse (wrap (this, &chord::stats_cb));
  rpcm->stats ();
}

void
chord::print_cb (strbuf outbuf, const chordID &k, ptr<vnode> v) {
  v->print (outbuf);
}

void
chord::print (strbuf &outbuf) {
  vnodes.traverse (wrap (this, &chord::print_cb, outbuf));
}

void
chord::stop_cb (const chordID &k, ptr<vnode> v) {
  v->stop ();
}

void
chord::stop () {
  vnodes.traverse (wrap (this, &chord::stop_cb));
}

const rpc_program *
chord::get_program (int progno)
{
  for (unsigned int i = 0; i < handledProgs.size (); i++)
    if (progno == (int)handledProgs[i]->progno)
      return handledProgs[i];
  return NULL;
}


bool
chord::isHandled (int progno) {
  for (u_int i = 0; i < handledProgs.size (); i++)
    if (progno == (int)handledProgs[i]->progno) return true;
  return false;
}
void
chord::handleProgram (const rpc_program &prog) {
  warn << "chord::handleProgram: " << prog.progno << "\n";
  if (isHandled (prog.progno)) return;
  else {
    handledProgs.push_back (&prog);
  }
}


void
chord::dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    s->setcb (NULL);
    return;
  }
  (*nrcv)++;
  
  //autocache nodes
  dorpc_arg *arg = sbp->template getarg<dorpc_arg> ();

  /*
  if (arg && !locations->cached (arg->src_id)) {
    const sockaddr *sa = sbp->getsa ();
    if (sa) {
      net_address netaddr;
      netaddr.port = arg->src_port;
      str addrstr (inet_ntoa (((sockaddr_in *)sa)->sin_addr));
      netaddr.hostname = addrstr;
      warnx << gettime () << ": autocaching " << addrstr << ":" << netaddr.port << " in ::dispatch\n";
      vec<float> coords = convert_coords (arg);
      locations->insert (arg->src_id, netaddr.hostname, netaddr.port, coords);
    }
  }
  */

  switch (sbp->proc ()) {
  case TRANSPORTPROC_DORPC:
    {
      //v (the destination chordID) is at the top of the header
      chordID *v = sbp->template getarg<chordID> ();
      vnode *vnodep = vnodes[*v];
      if (!vnodep) {
	trace << "unknown vnode " << *v << " for procedure "
	      << sbp->proc ()
	      << " (" << arg->progno << "." << arg->procno << ").\n";
	sbp->replyref (rpcstat (DORPC_UNKNOWNNODE));
	return;
      }
      
      //find the program
      const rpc_program *prog = get_program (arg->progno);
      if (!prog) {
	warn << "bad program: " << arg->progno << "\n";
	sbp->replyref (rpcstat (DORPC_MARSHALLERR));
	return;
      }
      
      //unmarshall the args
      char *arg_base = (char *)(arg->args.base ());
      int arg_len = arg->args.size ();
      
      xdrmem x (arg_base, arg_len, XDR_DECODE);
      xdrproc_t proc = prog->tbl[arg->procno].xdr_arg;
      assert (proc);
      
      void *unmarshalled_args = prog->tbl[arg->procno].alloc_arg ();
      if (!proc (x.xdrp (), unmarshalled_args)) {
	warn << "dispatch: error unmarshalling arguments\n";
	sbp->replyref (rpcstat (DORPC_MARSHALLERR));
	return;
      }

      //call the handler
      user_args *ua = New user_args (sbp, unmarshalled_args, 
				     prog, arg->procno, arg->send_time);
      vnodep->fill_user_args (ua);
      if (!vnodep->progHandled (arg->progno)) {
	trace << "dispatch to vnode " << *v << " doesn't handle "
	      << arg->progno << "." << arg->procno << "\n";
	ua->replyref (chordstat (CHORD_NOHANDLER));
      } else {	      
	cbdispatch_t dispatch = vnodep->getHandler(arg->progno);
	(dispatch)(ua);
      }  
      
    }
    break;
  default:
    fatal << "Transport procedure " << sbp->proc () << " not handled\n";
  }
		
}



