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

#include <misc_utils.h>
#include <id_utils.h>
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
  ok = ok && set_int ("chord.max_vnodes", 1024);

  ok = ok && set_int ("chord.nsucc", 16);
  ok = ok && set_int ("chord.npred", 1);
  ok = ok && set_int ("chord.ncoords", 3);

  ok = ok && set_str ("chord.rpc_mode", "stp");

  ok = ok && set_int ("chord.lookup_timeout", 15);

  /** use the greedy metric instead.  Probably desirable if toes are
   *  enabled. */
  ok = ok && set_int ("chord.greedy_lookup", 0);
  /** try to terminate retrieves early if the hop has
   *  returned a "sufficient" number of successors.  */
  ok = ok && set_int ("chord.find_succlist_shaving", 1);

  ok = ok && set_int ("chord.checkdead_interval", 60);
  ok = ok && set_int ("chord.checkdead_max", 960);

  assert (ok);
#undef set_int
#undef set_str
}

chord::chord (str host, int port, 
	      vnode_producer_t p, int nvnodes,
              int max_cache) :
  max_vnodes (0),
  myname (host),
  myport (port),
  fd_dgram (-1),
  fd_stream (-1),
  x_dgram (NULL),
  nrcv (NULL),
  rpcm (NULL),
  locations (NULL)
{
  bool ok (false);

  ok = Configurator::only ().get_int ("chord.max_vnodes", max_vnodes);
  assert (ok);
  if (nvnodes > max_vnodes)
    fatal << "Requested more than allowed vnodes: " 
          << nvnodes << ">" << max_vnodes << "\n";
  if (nvnodes < 1) {
    warn << "Creating at least one vnode.\n";
    nvnodes = 1;
  }
  
  str rpcstr;
  ok = Configurator::only ().get_str ("chord.rpc_mode", rpcstr);
  assert (ok);
  chord_rpc_style = CHORD_RPC_STP;
  if (rpcstr == "tcp" || rpcstr == "TCP")
    chord_rpc_style = CHORD_RPC_SFST;
  else if (rpcstr == "udp" || rpcstr == "UDP")
    chord_rpc_style = CHORD_RPC_SFSU;

  nrcv = New refcounted<u_int32_t>;
  *nrcv = 0;
  switch (chord_rpc_style) {
  case CHORD_RPC_STP:
    rpcm = New refcounted<stp_manager> (nrcv);
    break;
  case CHORD_RPC_SFSU:
    rpcm = New refcounted<rpc_manager> (nrcv);
    break;
  case CHORD_RPC_SFST:
  case CHORD_RPC_SFSBT:
    rpcm = New refcounted<tcp_manager> (nrcv);
    break;
  default:
    fatal << "bad chord_rpc_style value: " << chord_rpc_style << "\n";
  }

  /* In case myport == 0, need to initialize to something in order
   * to create chordIDs during vnode initialization */
  myport = initxprt (myport, SOCK_DGRAM, &fd_dgram);
  myport = initxprt (myport, SOCK_STREAM, &fd_stream);

  warnx << "chord: running on " << myname << ":" << myport << "\n";

  locations = New refcounted<locationtable> (max_cache);

  srandom ((unsigned int) (getusec() & 0xFFFFFFFF));

  for (int i = 0; i < nvnodes; i++) 
  {
    chordID newID = make_chordID (myname, myport, i);
    warnx << gettime () << ": creating new vnode: " << newID << "\n";
    Coord coords;
    ptr<location> l = locations->insert (newID, myname, myport,
					 i, coords, 30, 0, 1, true);
    assert (l);
    locations->pin (newID);

    ptr<vnode> vnodep = (*p) (mkref (this), rpcm, l);
  
    vnodes.insert (newID, vnodep);
    vlist.push_back (vnodep);
  }
}

int
chord::initxprt (int myp, int type, int *fd)
{
  in_addr my_addr;
  inet_aton (myname.cstr (), &my_addr);
  *fd = inetsocket (type, myp, ntohl (my_addr.s_addr));
  if (*fd < 0)
    fatal ("binding %s addr %s port %d: %m\n",
	   (type == SOCK_DGRAM ? "UDP" : "TCP"), myname.cstr (), myp);
  
  if (myp == 0) {
    struct sockaddr_in addr;
    socklen_t len = sizeof (addr);
    bzero (&addr, sizeof (addr));
    if (getsockname (*fd, (sockaddr *) &addr, &len) < 0) 
      fatal ("getsockname failed %m\n");
    myp = ntohs (addr.sin_port);
  }
  return myp;
}

void
chord::startchord ()
{
  assert (fd_stream > 0 || fd_dgram > 0);
  if (fd_dgram > 0) {
    x_dgram = axprt_dgram::alloc (fd_dgram, sizeof(sockaddr), 230000);
    ptr<asrv> s = asrv::alloc (x_dgram, transport_program_1);
    s->setcb (wrap (mkref(this), &chord::dispatch, s));
  }

  if (fd_stream > 0) {
    int ret = listen (fd_stream, 1000);
    if (ret < 0)
      fatal ("listen (%d, 1000): %m\n", fd_stream);
    fdcb (fd_stream, selread, wrap (this, &chord::tcpclient_cb, fd_stream));
  }
}

void
chord::tcpclient_cb (int srvfd)
{
  int fd = accept (srvfd, NULL, NULL);
  if (fd < 0)
    warn << "chord: accept failed " << strerror (errno) << "\n";
  else {
    ptr<axprt> x = axprt_stream::alloc (fd, 260*1024);

    ptr<asrv> s = asrv::alloc (x, transport_program_1);
    s->setcb (wrap (mkref(this), &chord::dispatch, s));
  }
}

ptr<vnode>
chord::get_vnode (unsigned int i)
{
  if (i > vlist.size ()) 
    return NULL;
  return vlist[i];
}

size_t
chord::num_vnodes (void)
{
  return vlist.size ();
}

void
chord::join (str wellknownhost, int wellknownport, bool failok)
{
  chord_node wkn;
  bzero (&wkn, sizeof (wkn));
  wkn.r.hostname = wellknownhost;
  wkn.r.port = wellknownport ? wellknownport : myport;
  wkn.x = make_chordID (wkn.r.hostname, wkn.r.port);
  wkn.vnode_num = 0;
  //make up info about the age and knownup for this entry
  wkn.age = 60;
  wkn.knownup = 600;

  wkn.coords.setsize (Coord::NCOORD + Coord::USING_HT);
  // Make up some random initial information for this other node.
  for (unsigned int i = 0; i < Coord::NCOORD + Coord::USING_HT; i++)
    wkn.coords[i] = (int) 0.0;
  wkn.e = -1;

  ptr<location> wellknown_node = vlist[0]->my_location ();
  if (myname != wellknownhost || myport != wellknownport) {
    wellknown_node = locations->insert (wkn);
    if (!wellknown_node)
      fatal << "Well known host failed to verify! Bailing.\n";
  }

  if (vlist[0]->my_ID () == wellknown_node->id ()) {
    for (size_t i = 0; i < vlist.size (); i++) {
      vlist[i]->stabilize ();
    }
  } else {
    for (size_t i = 0; i < vlist.size (); i++) {
      vlist[i]->join (wellknown_node, wrap (this, &chord::join_cb, failok));
    }
  }
}

void
chord::join_cb (bool failok, ptr<vnode> v, chordstat s)
{
  if (s != CHORD_OK) {
    warnx << "chord::join failed " << s << "\n";
    if (!failok)
      fatal << "Exiting!\n";
  }
}

void
chord::stats ()
{
  warnx << "CHORD NODE STATS\n";
  warnx << "# vnodes: " << vlist.size () << "\n";
  for (size_t i = 0; i < vlist.size (); i++) 
    vlist[i]->stats ();
  rpcm->stats (warnx);
}

void
chord::print (strbuf &outbuf)
{
  for (size_t i = 0; i < vlist.size (); i++) 
    vlist[i]->print (outbuf);
}

void
chord::rpcmstats (const strbuf &ob)
{
  rpcm->stats (ob);
}

void
chord::stop () 
{
  for (size_t i = 0; i < vlist.size (); i++) 
    vlist[i]->stop ();
}

void
chord::stabilize ()
{
  for (size_t i = 0; i < vlist.size (); i++) 
    vlist[i]->stabilize ();
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
  warn << "chord::handleProgram: " << prog.name
       << " (" << prog.progno << ")\n";
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
  
  dorpc_arg *arg = sbp->Xtmpl getarg<dorpc_arg> ();

  switch (sbp->proc ()) {
  case TRANSPORTPROC_NULL:
    sbp->reply (NULL);
    break;
  case TRANSPORTPROC_DORPC:
    {
      chordID v = make_chordID (arg->dest);
      vnode *vnodep = vnodes[v];
      if (!vnodep) {
	trace << "unknown vnode " << v << " for procedure "
	      << sbp->proc ()
	      << " (" << arg->progno << "." << arg->procno << ").\n";
	sbp->replyref (rpcstat (DORPC_UNKNOWNNODE));
	return;
      }
      
      //find the program
      const rpc_program *prog = get_program (arg->progno);
      if (!prog) {
	sbp->replyref (rpcstat (DORPC_NOHANDLER));
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
	warn << "dispatch: error unmarshalling arguments: "
	     << arg->progno << "." << arg->procno 
	     << " from " << v <<"\n";
        xdr_delete (prog->tbl[arg->procno].xdr_arg, unmarshalled_args);
	sbp->replyref (rpcstat (DORPC_MARSHALLERR));
	return;
      }

      //call the handler
      user_args *ua = New user_args (sbp, unmarshalled_args, 
				     prog, arg->procno, arg->send_time);
      vnodep->fill_user_args (ua);
      if (!vnodep->progHandled (arg->progno)) {
	trace << "dispatch to vnode " << v << " doesn't handle "
	      << arg->progno << "." << arg->procno << "\n";
	ua->replyref (chordstat (CHORD_NOHANDLER));
      } else {	      
	cbdispatch_t dispatch = vnodep->getHandler(arg->progno);
	(dispatch)(ua);
      }  
      
    }
    break;
  default:
    warn << "Transport procedure " << sbp->proc () << " not handled\n";
  }
}
