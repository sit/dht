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

void
chord::doaccept (int fd)
{
  ptr<asrv> s;
  if (fd < 0)
    fatal ("EOF\n");
  tcp_nodelay (fd);
  ref<axprt_stream> x = axprt_stream::alloc (fd);
  s = asrv::alloc (x, chord_program_1,  wrap (mkref(this), &chord::dispatch));
  srv.push_back (s);
  //  dhs->accept (x);
}

void
chord::accept_standalone (int lfd)
{
  sockaddr_in sin;
  bzero (&sin, sizeof (sin));
  socklen_t sinlen = sizeof (sin);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sin), &sinlen);
  if (fd >= 0)
    doaccept (fd);
  else 
    warnx << "accept_standalone: accept failed\n";
}

int
chord::startchord (int myp)
{
  int p = myp;
  int srvfd = inetsocket (SOCK_STREAM, myp);
  if (srvfd < 0)
    fatal ("binding TCP port %d: %m\n", myp);
  if (myp == 0) {
    struct sockaddr_in la;
    socklen_t len;
    len = sizeof (la);
    if (getsockname (srvfd, (struct sockaddr *) &la, &len) < 0) {
      fatal ("getsockname failed\n");
    }
    p = ntohs (la.sin_port);
    warnx << "startp2pd: local port " << p << "\n";
  }
  listen (srvfd, 1000);
  fdcb (srvfd, selread, wrap (mkref (this), &chord::accept_standalone, srvfd));
  return p;
}

chord::chord (str _wellknownhost, int _wellknownport, 
	      const chordID &_wellknownID,
	      int port, str myhost, int set_rpcdelay) :
  wellknownID (_wellknownID)
{
  myaddress.port = startchord (port);
  myaddress.hostname = myhost;
  wellknownhost.hostname = _wellknownhost;
  wellknownhost.port = _wellknownport;
  warnx << "chord: myport is " << myaddress.port << "\n";
  warnx << "chord: myname is " << myaddress.hostname << "\n";
  locations = New refcounted<locationtable> (mkref (this), set_rpcdelay);
  locations->insert (wellknownID, wellknownhost.hostname, wellknownhost.port, 
		     wellknownID);
  nvnode = 0;
  ngetsuccessor = 0;
  ngetpredecessor = 0;
  nfindclosestpred = 0;
  nnotify = 0;
  nalert = 0;
  ntestrange = 0;
  ngetfingers = 0;
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

void
chord::newvnode ()
{
  chordID newID = initID (nvnode);
  locations->insert (newID, myaddress.hostname, myaddress.port, newID);
  ptr<vnode> vnodep = New refcounted<vnode> (locations, mkref (this), newID);
  nvnode++;
  vnodes.insert (vnodep);
  locations->checkrefcnt (0);
  vnodep->join ();
}

void
chord::newvnode (chordID &x)
{
  if (x != wellknownID) 
    locations->insert (x, myaddress.hostname, myaddress.port, x);
  ptr<vnode> vnodep = New refcounted<vnode> (locations, mkref (this), x);
  nvnode++;
  vnodes.insert (vnodep);
  locations->checkrefcnt (0);
  if (x != wellknownID) {
    vnodep->join ();
  } else {
    vnodep->stabilize (0);
  }
}

void
chord::deletefingers (chordID &x)
{
  warnx << "deletefingers: " << x << "\n";
  for (vnode *v = vnodes.first (); v != NULL; v = vnodes.next (v)) {
    v->deletefingers (x);
  }
}

int
chord::countrefs (chordID &x)
{
  int n = 0;
  for (vnode *v = vnodes.first (); v != NULL; v = vnodes.next (v)) {
    n += v->countrefs (x);
  }
  return n;
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
  for (vnode *v = vnodes.first (); v != NULL; v = vnodes.next (v)) {
    v->stats ();
  }
  locations->stats ();
}

void
chord::dispatch (svccb *sbp)
{
  if (!sbp) {
    warnx << "a connection was closed\n"; // XXX remove asrv from list
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
      chord_vnode *v = sbp->template getarg<chord_vnode> ();
      vnode *vnodep = vnodes[v->n];
      assert (vnodep);
      warnt("CHORD: getsuccessor_request");
      ngetsuccessor++;
      vnodep->doget_successor (sbp);
    }
    break;
  case CHORDPROC_GETPREDECESSOR:
    {
      chord_vnode *v = sbp->template getarg<chord_vnode> ();
      vnode *vnodep = vnodes[v->n];
      warnt("CHORD: getpredecessor_request");
      assert (vnodep);
      ngetpredecessor++;
      vnodep->doget_predecessor (sbp);
    }
    break;
  case CHORDPROC_FINDCLOSESTPRED:
    {
      chord_findarg *fa = sbp->template getarg<chord_findarg> ();
      vnode *vnodep = vnodes[fa->v.n];
      assert (vnodep);
      warnt("CHORD: findclosestpred_request");
      nfindclosestpred++;
      vnodep->dofindclosestpred (sbp, fa);
    }
    break;
  case CHORDPROC_NOTIFY:
    {
      chord_nodearg *na = sbp->template getarg<chord_nodearg> ();
      vnode *vnodep = vnodes[na->v.n];
      assert (vnodep);
      warnt("CHORD: donotify");
      nnotify++;
      vnodep->donotify (sbp, na);
    }
    break;
  case CHORDPROC_ALERT:
    {
      chord_nodearg *na = sbp->template getarg<chord_nodearg> ();
      vnode *vnodep = vnodes[na->v.n];
      assert (vnodep);
      nalert++;
      vnodep->doalert (sbp, na);
    }
    break;
  case CHORDPROC_TESTRANGE_FINDCLOSESTPRED:
    {
      warnt("CHORD: testandfindrequest");
      chord_testandfindarg *fa = 
	sbp->template getarg<chord_testandfindarg> ();
      vnode *vnodep = vnodes[fa->v.n];
      assert (vnodep);
      ntestrange++;
      vnodep->dotestandfind (sbp, fa);
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
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

