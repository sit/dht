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
	      int port, int set_rpcdelay, int max_cache, 
	      int max_connections, int server_selection_mode) :
  wellknownID (_wellknownID), ss_mode (server_selection_mode % 10), 
  active (NULL)
{
  myport = startchord (port);
  wellknownhost.hostname = _wellknownhost;
  wellknownhost.port = _wellknownport;
  warnx << "chord: myport is " << myport << "\n";
  warnx << "chord: myname is " << my_addr () << "\n";
  locations = New refcounted<locationtable> (mkref (this), set_rpcdelay, 
					     max_cache, max_connections);
  locations->insert (wellknownID, wellknownhost.hostname, wellknownhost.port);
  nvnode = 0;
  srandom ((unsigned int) (getusec() & 0xFFFFFFFF));
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

  ptr<axprt_dgram> x = axprt_dgram::alloc (srvfd, sizeof(sockaddr), 230000);
  ptr<asrv> s = asrv::alloc (x, chord_program_1);
  s->setcb (wrap (mkref(this), &chord::dispatch, s, x));
  return myp;
}


ptr<vnode>
chord::newvnode (cbjoin_t cb)
{
  chordID newID = init_chordID (nvnode, myport);
  if (newID != wellknownID)
    locations->insert (newID, my_addr (), myport);
  ptr<vnode> vnodep = New refcounted<vnode> (locations, mkref (this), newID, 
					     nvnode, ss_mode);
  if (!active) active = vnodep;
  nvnode++;
  warn << "insert: " << newID << "\n";
  vnodes.insert (newID, vnodep);

  if (newID != wellknownID) {
    vnodep->join (cb);
  } else {
    vnodep->stabilize ();
    (*cb) (vnodep, CHORD_OK);
  }
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
chord::stop_cb (const chordID &k, ptr<vnode> v) {
  v->stop ();
}

void
chord::stop () {
  vnodes.traverse (wrap (this, &chord::stop_cb));
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
  nrcv++;
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
      vnodep->doget_successor (sbp);
    }
    break;
  case CHORDPROC_GETPREDECESSOR:
    {
      warnt("CHORD: getpredecessor_request");
      vnodep->doget_predecessor (sbp);
    }
    break;
  case CHORDPROC_FINDCLOSESTPRED:
    {
      chord_findarg *fa = sbp->template getarg<chord_findarg> ();
      warn << "(find_pred) looking for " << fa->v.n << "\n";
      warnt("CHORD: findclosestpred_request");
      vnodep->dofindclosestpred (sbp, fa);
    }
    break;
  case CHORDPROC_NOTIFY:
    {
      chord_nodearg *na = sbp->template getarg<chord_nodearg> ();
      warnt("CHORD: donotify");
      vnodep->donotify (sbp, na);
    }
    break;
  case CHORDPROC_ALERT:
    {
      chord_nodearg *na = sbp->template getarg<chord_nodearg> ();
      warnt("CHORD: alert");
      vnodep->doalert (sbp, na);
    }
    break;
  case CHORDPROC_TESTRANGE_FINDCLOSESTPRED:
    {
      warnt("CHORD: testandfindrequest");
      chord_testandfindarg *fa = 
        sbp->template getarg<chord_testandfindarg> ();
      vnodep->dotestrange_findclosestpred (sbp, fa);
    }
    break;
  case CHORDPROC_GETFINGERS: 
    {
      warnt("CHORD: getfingers_request");
      vnodep->dogetfingers (sbp);
    }
    break;
  case CHORDPROC_CHALLENGE:
    {
      warnt("CHORD: challenge");
      chord_challengearg *ca = 
        sbp->template getarg<chord_challengearg> ();
      vnodep->dochallenge (sbp, ca);
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


