/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */
#include <chord.h>
#include <chord_util.h>

chord::chord (str _wellknownhost, int _wellknownport, 
	      const chordID &_wellknownID,
	      int port, str myhost, int set_rpcdelay, int max_cache, 
	      int max_connections) :
  wellknownID (_wellknownID), active (NULL)
{
  myaddress.port = startchord (port);
  myaddress.hostname = myhost;
  wellknownhost.hostname = _wellknownhost;
  wellknownhost.port = _wellknownport;
  warnx << "chord: myport is " << myaddress.port << "\n";
  warnx << "chord: myname is " << myaddress.hostname << "\n";
  locations = New refcounted<locationtable> (mkref (this), set_rpcdelay, 
					     max_cache, max_connections);
  locations->insert (wellknownID, wellknownhost.hostname, wellknownhost.port);
  nvnode = 0;
  ngetsuccessor = 0;
  ngetpredecessor = 0;
  nfindclosestpred = 0;
  nnotify = 0;
  nalert = 0;
  ntestrange = 0;
  ngetfingers = 0;

}

int
chord::startchord (int myp)
{
  int srvfd = inetsocket (SOCK_DGRAM, myp);
  if (srvfd < 0)
    fatal ("binding UDP port %d: %m\n", myp);

  if (myp == 0) {
    struct sockaddr_in addr;
    socklen_t len = sizeof (addr);
    bzero (&addr, sizeof (addr));
    if (getsockname (srvfd, (sockaddr *) &addr, &len) < 0) 
      fatal ("getsockname failed %m\n");
    myp = ntohs (addr.sin_port);
  }

  ptr<axprt_dgram> x = axprt_dgram::alloc (srvfd);
  ptr<asrv> s = asrv::alloc (x, chord_program_1);
  s->setcb (wrap (mkref(this), &chord::dispatch, s, x));
  return myp;
}


chordID
chord::initID (int index)
{
  chordID ID;
#if 1
  ID = random_bigint (NBIT);
#else
  vec<in_addr> addrs;
  if (!myipaddrs (&addrs))
    fatal ("cannot find my IP address.\n");

  in_addr *addr = addrs.base ();
  while (addr < addrs.lim () && ntohl (addr->s_addr) == INADDR_LOOPBACK)
    addr++;
  if (addr >= addrs.lim ())
    fatal ("cannot find my IP address.\n");

  str ids = inet_ntoa (*addr);
  ids = ids << "." << index;
  warnx << "my address: " << ids << "\n";
  char id[sha1::hashsize];
  sha1_hash (id, ids, ids.len());
  mpz_set_rawmag_be (ID, id, sizeof (id));  // For big endian
  chordID b (1);
  b = b << NBIT;
  b = b - 1;c
  *ID = *ID & b;
#endif
  //  warnx << "myid: " << ID << "\n";
  return ID;
}


ptr<vnode>
chord::newvnode (cbjoin_t cb)
{
  chordID newID = initID (nvnode);
  locations->insert (newID, myaddress.hostname, myaddress.port);
  ptr<vnode> vnodep = New refcounted<vnode> (locations, mkref (this), newID);
  nvnode++;
  warn << "insert: " << newID << "\n";
  vnodes.insert (newID, vnodep);
  vnodep->join (cb);
  if (!active) active = vnodep;
  return vnodep;
}

ptr<vnode>
chord::newvnode (chordID &x, cbjoin_t cb)
{
  if (x != wellknownID) 
    locations->insert (x, myaddress.hostname, myaddress.port);
  ptr<vnode> vnodep = New refcounted<vnode> (locations, mkref (this), x);
  nvnode++;
  warn << "insert: " << x << "@" << myaddress.hostname << "(" 
       << myaddress.port << ")\n";
  vnodes.insert (x, vnodep);
  if (x != wellknownID) {
    vnodep->join (cb);
  } else {
    route r;
    vnodep->stabilize ();
    (*cb) (vnodep);
  }
  if (!active) active = vnodep;
  return vnodep;
}

void
chord::deletefingers_cb (chordID x, const chordID &k, ptr<vnode> v) {
  v->deletefingers (x);
}

void
chord::deletefingers (chordID x)
{
  warnx << "deletefingers: " << x << "\n";
  vnodes.traverse (wrap (this, &chord::deletefingers_cb, x));
}

int
chord::countrefs (chordID &x)
{
  int n = 0;
  for (qhash_slot <chordID, ref<vnode> > *s = vnodes.first(); s != NULL; 
       s = vnodes.next (s)) {
    n += s->value->countrefs (x);
  }
  return n;
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
  warnx << "# getsuccesor requests " << ngetsuccessor << "\n";
  warnx << "# getpredecessor requests " << ngetpredecessor << "\n";
  warnx << "# findclosestpred requests " << nfindclosestpred << "\n";
  warnx << "# notify requests " << nnotify << "\n";  
  warnx << "# alert requests " << nalert << "\n";  
  warnx << "# testrange requests " << ntestrange << "\n";  
  warnx << "# getfingers requests " << ngetfingers << "\n";
  vnodes.traverse (wrap (this, &chord::stats_cb));
  locations->stats ();
}

void
chord::print_cb (const chordID &k, ptr<vnode> v) {
  v->print ();
}

void
chord::print () {
  vnodes.traverse (wrap (this, &chord::print_cb));
}

void 
chord::register_handler (int progno, chordID dest, cbdispatch_t hand)
{
  vnode *vnodep = vnodes[dest];
  assert (vnodep);
  vnodep->addHandler (progno, hand);
}

void
chord::dispatch (ptr<asrv> s, ptr<axprt_dgram> x, svccb *sbp)
{
  if (!sbp) {
    s->setcb (NULL);
    return;
  }
  chord_vnode *v = sbp->template getarg<chord_vnode> ();
  vnode *vnodep = vnodes[v->n];
  if (!vnodep) {
    sbp->replyref (chordstat (CHORD_UNKNOWNNODE));
    return;
  }
  switch (sbp->proc ()) {
  case CHORDPROC_NULL: 
    {
      sbp->reply (NULL);
    }
    break;
  case CHORDPROC_GETSUCCESSOR:
    {
      warnt("CHORD: getsuccessor_request");
      ngetsuccessor++;
      vnodep->doget_successor (sbp);
    }
    break;
  case CHORDPROC_GETPREDECESSOR:
    {
      warnt("CHORD: getpredecessor_request");
      ngetpredecessor++;
      vnodep->doget_predecessor (sbp);
    }
    break;
  case CHORDPROC_FINDCLOSESTPRED:
    {
      chord_findarg *fa = sbp->template getarg<chord_findarg> ();
      warn << "(find_pred) looking for " << fa->v.n << "\n";
      warnt("CHORD: findclosestpred_request");
      nfindclosestpred++;
      vnodep->dofindclosestpred (sbp, fa);
    }
    break;
  case CHORDPROC_NOTIFY:
    {
      chord_nodearg *na = sbp->template getarg<chord_nodearg> ();
      warnt("CHORD: donotify");
      nnotify++;
      vnodep->donotify (sbp, na);
    }
    break;
  case CHORDPROC_ALERT:
    {
      chord_nodearg *na = sbp->template getarg<chord_nodearg> ();
      nalert++;
      vnodep->doalert (sbp, na);
    }
    break;
  case CHORDPROC_TESTRANGE_FINDCLOSESTPRED:
    {
      warnt("CHORD: testandfindrequest");
      chord_testandfindarg *fa = 
        sbp->template getarg<chord_testandfindarg> ();
      ntestrange++;
      vnodep->dotestrange_findclosestpred (sbp, fa);
    }
    break;
  case CHORDPROC_GETFINGERS: 
    {
      chord_vnode *v = sbp->template getarg<chord_vnode> ();
      vnode *vnodep = vnodes[v->n];
      assert (vnodep);
      warnt("CHORD: getfingers_request");
      ngetfingers++;
      vnodep->dogetfingers (sbp);
    }
    break;
  case CHORDPROC_HOSTRPC:
    {
      chord_RPC_arg *arg = sbp->template getarg<chord_RPC_arg> ();
      cbdispatch_t dispatch = vnodep->getHandler(arg->host_prog);
      if (dispatch) {
	long xid = locations->new_xid (sbp);
	(dispatch)(arg->host_proc, arg, xid);
      } else {
	chord_RPC_res res;
	res.set_status (CHORD_NOHANDLER);
	sbp->replyref (res);
      }
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


