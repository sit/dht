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

#include <chord.h>
#include <chord_util.h>
#include <dhash.h>

chord::chord (str _wellknownhost, int _wellknownport, 
	      str _myname, int port, int max_cache, 
	      int server_selection_mode) :
  myname (_myname), 
  ss_mode (server_selection_mode % 10),
  active (NULL)
{
  myport = startchord (port);
  wellknownhost.hostname = _wellknownhost;
  wellknownhost.port = _wellknownport ? _wellknownport : myport;
  wellknownID  = make_chordID (wellknownhost.hostname, wellknownhost.port);

  warnx << "chord: running on " << myname << ":" << myport << "\n";
  locations = New refcounted<locationtable> (mkref (this), max_cache);
  locations->insert (wellknownID, wellknownhost.hostname, wellknownhost.port);
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
    ptr<asrv> s = asrv::alloc (x, chord_program_1);
    ptr<asrv> s2 = asrv::alloc (x, dhash_program_1);
    s->setcb (wrap (mkref(this), &chord::dispatch, s));
    s2->setcb (wrap (mkref(this), &chord::dispatch, s2));
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
    ptr<asrv> s = asrv::alloc (x_dgram, chord_program_1);
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
  if (getenv("DHASHTCP")) {
    // Ensure the DGRAM and STREAM sockets are on same port #,
    // since it is included in the Chord ID's hash.
    myp = startchord (myp, SOCK_STREAM);
  }

  return startchord (myp, SOCK_DGRAM);
}



ptr<vnode>
chord::newvnode (cbjoin_t cb)
{
  if (nvnode > max_vnodes)
    fatal << "Maximum number of vnodes (" << max_vnodes << ") reached.\n";
    
  chordID newID = init_chordID (nvnode, myname, myport);
  if (newID != wellknownID)
    locations->insert (newID, myname, myport);
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

bool
chord::isHandled (int progno) {
  for (unsigned int i = 0; i < handledProgs.size (); i++)
    if (progno == handledProgs[i]) return true;
  return false;
}
void
chord::handleProgram (const rpc_program &prog) {
  if (isHandled (prog.progno)) return;
  else {
    handledProgs.push_back (prog.progno);
    ptr<asrv> s = asrv::alloc (x_dgram, prog);
    s->setcb (wrap (mkref(this), &chord::dispatch, s));
  }
  
}
void
chord::dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    s->setcb (NULL);
    return;
  }

  nrcv++;
  chord_vnode *v = sbp->template getarg<chord_vnode> ();
  vnode *vnodep = vnodes[v->n];
  if (!vnodep) {
    warnx << "CHORD: unknown node in " << sbp->proc() 
	  << "request " << v->n << "\n";
    sbp->replyref (chordstat (CHORD_UNKNOWNNODE));
    return;
  }

  if (sbp->prog () == CHORD_PROGRAM) {
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
    case CHORDPROC_GETFINGERS_EXT: 
      {
	warnt("CHORD: getfingers_ext_request");
	vnodep->dogetfingers_ext (sbp);
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
    case CHORDPROC_GETTOES:
      {
	warnt("CHORD: get toes");
	vnodep->dogettoes (sbp);
      }
      break;
    default:
      sbp->reject (PROC_UNAVAIL);
      break;
    }  /* switch sbp->proc () */
  } else { /* not a CHORDPROG RPC */
    cbdispatch_t dispatch = vnodep->getHandler(sbp->prog ());
    if (dispatch) {
      (dispatch)(sbp);
    } else {
      chord_RPC_res res;
      res.set_status (CHORD_NOHANDLER);
      sbp->replyref (res);
    }
  }
  
}


