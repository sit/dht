/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 * Copyright (C) 2002 Frans Kaashoek (kaashoek@lcs.mit.edu),
 *                    Frank Dabek (fdabek@lcs.mit.edu) and
 *                    Emil Sit (sit@lcs.mit.edu).
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

#include <math.h>
#include "chord.h"

bool nochallenges;

#if 0
void
locationtable::printloc (locwrap *l)
{
  assert (l);
  warnx << "Locwrap " << l->n_;
  warnx << " type " << l->type_;
  if (l->type_ & LOC_REGULAR)
    warnx << " (A=" << l->loc_->alive
	  << ", C=" << l->loc_->challenged << ")\n";
}
#endif /* 0 */

static void
ignore_challengeresp (chordID x, bool b, chordstat s)
{
  warnx << "Ignoring " << ((b && s == CHORD_OK) ? "good" : "bad")
	<< " challenge response for " << x << "\n";
}
cbchallengeID_t cbchall_null (wrap (ignore_challengeresp));

location::location (const chordID &_n, const net_address &_r) 
  : n (_n), addr (_r), alive (true), challenged (false)
{
  rpcdelay = 0;
  nrpc = 0;
  a_lat = 0.0;
  a_var = 0.0;
  maxdelay = 0;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (_r.hostname.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (addr.port);
};

location::~location () {
  warnx << "~location: delete " << n << "\n";
}

void
locationtable::start_network ()
{
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";

  delaycb (1, 0, wrap (this, &locationtable::ratecb));

  reset_idle_timer ();
}

locationtable::locationtable (ptr<chord> _chordnode, int _max_cache)
  : chordnode (_chordnode),
    good (0),
    size_cachedlocs (0),
    max_cachedlocs (_max_cache), 
    rpcdelay (0),
    nrpc (0),
    a_lat (0.0),
    a_var (0.0),
    avg_lat (0.0),
    bf_lat (0.0),
    bf_var (0.0),
    nrpcfailed (0),
    nsent (0),
    npending (0),
    nnodessum (0),
    nnodes (0),
    nvnodes (0),
    seqno (0),
    cwind (1.0),
    ssthresh (6.0),
    left (0),
    cwind_cum (0.0),
    num_cwind_samples (0),
    num_qed (0),
    idle_timer (NULL)
{
  start_network ();
}

locationtable::locationtable (const locationtable &src)
{
  chordnode = src.chordnode;
#ifdef PNODE  
  myvnode = NULL;
#endif /* PNODE */  
  max_cachedlocs = src.max_cachedlocs;

  // State parameters will be zeroed in the copy
  good = 0;
  size_cachedlocs = 0; 
  nrpc = 0;
  nrpcfailed = 0;
  a_lat = 0.0;
  a_var = 0.0;
  avg_lat = 0.0;
  bf_lat = 0.0;
  bf_var = 0.0;
  nsent = 0;
  npending = 0;
  nchallenge = 0;
  size_cachedlocs = 0;
  nvnodes = 0;
  nnodes = 0;
  nnodessum = 0;
  seqno = 0;
  cwind = 1;
  ssthresh = 6;
  left = 0;
  cwind_cum = 0.0;
  num_cwind_samples = 0;
  num_qed = 0;

  idle_timer = NULL;

  // Deep copy the list of locations. Do not copy pins because those
  // reflect the needs of the individual vnodes; each vnode should
  // insert its own pins into the table. (Maybe this should change.)
  for (locwrap *l = src.locs.first (); l; l = src.locs.next (l)) {
    if ((l->type_ & LOC_REGULAR) == 0) continue; 
    if (!l->loc_->alive) continue;
    ref<location> loc = New refcounted<location> (l->loc_->n, l->loc_->addr);
    loc->challenged = l->loc_->challenged;
    realinsert (loc);
    if (loc->challenged) good++;
  }
  
  start_network ();
}

bool
locationtable::locwrap::good ()
{
  return ((type_ & LOC_REGULAR) &&
	  loc_ &&
	  loc_->alive &&
	  loc_->challenged);
}

void
locationtable::ratecb () {
#if 0
  warnx << "sent " << nsent << " RPCs in the last second\n";
  warnx << "received " << chordnode->nrcv << " RPCs in the last second\n";
  warnx << npending << " RPCs are outstanding\n";
#endif

  delaycb (1, 0, wrap (this, &locationtable::ratecb));
  nsent = 0;
  chordnode->nrcv = 0;
}

void
locationtable::replace_estimate (u_long o, u_long n)
{
  assert (nvnodes > 0);
  nnodessum = nnodessum - o + n;
  nnodes = nnodessum / nvnodes;
}

void
locationtable::insertgood (const chordID &n, sfs_hostname s, int p)
{
  assert (!locs[n]);

  net_address r;
  r.hostname = s; r.port = p;

  ref<location> loc = New refcounted<location> (n, r);
  loc->challenged = true; // force goodness
  realinsert (loc);
  good++;
  // warnx << "INSERT (good): " << n << "\n";
}

void
locationtable::insert (const chordID &n, sfs_hostname s, int p,
		       cbchallengeID_t cb)
{
  assert (!locs[n]);
  
  net_address r;
  r.hostname = s;
  r.port = p;
  cacheloc (n, r, cb);
}

void
locationtable::cacheloc (const chordID &x, net_address &r, cbchallengeID_t cb)
{
  // char *state;
  locwrap *lx = locs[x];
  if (lx == NULL) {
    // state = "new";
    ref<location> loc = New refcounted<location> (x, r);
    realinsert (loc);
    challenge (x, cb);
  } else if (lx->loc_->alive == false || lx->loc_->challenged == false) {
    // state = "pending";
    challenge (x, cb); // queue up for additional callback
  } else {
    // state = "old";
    if (cb != cbchall_null)
      cb (x, lx->loc_->challenged, CHORD_OK);
  }
#ifdef PNODE
  // if (myvnode)
  //   warnx << myvnode->myID << " ";
#endif /* PNODE */
  // warnx << "CACHELOC (" << state << "): " << x << " at port " << r.port << "\n";
}

void
locationtable::delete_cachedlocs (void)
{
  if (!size_cachedlocs)
    return;
  locwrap *lw = cachedlocs.first;
  locwrap *p = loclist.prev (lw);
  locwrap *n = loclist.next (lw);
  
#if 0
  if (size_cachedlocs == good) {
    // must evict someone good... 
    while ((p && p->type_ & LOC_PINSUCC) ||
	   (n && n->type_ & LOC_PINPRED))
    {
      lw = cachedlocs.next (lw);
      if (!lw)
	break;
      p = loclist.prev (lw);
      n = loclist.next (lw);
    }
    if (!lw) { 
      warnx << "locationtable::delete_cachedlocs: WOW! everyone is pinned!\n";
      // but this is okay.
      return;
    }
  } else
#endif /* 0 */
  {
    // pick some loser.
    while (lw->good () ||
	   (lw->loc_ && lw->loc_->outstanding_cbs.size ()) ||
	   (p && p->type_ & LOC_PINSUCC) ||
	   (n && n->type_ & LOC_PINPRED))
    {
      lw = cachedlocs.next (lw);
      if (!lw)
	break;
      p = loclist.prev (lw);
      n = loclist.next (lw);
    }
    if (!lw) {
      warnx << "locationtable::delete_cachedlocs: no bad nodes to evict.\n";
      return;
    }
  }
  assert (lw);

  if (lw->good ()) good--;
  locs.remove (lw);
  loclist.remove (lw->n_);
  cachedlocs.remove (lw);
  size_cachedlocs--;
  {
    locwrap *foo = locs.first ();
    size_t mygood = 0;
    while (foo) {
      if (foo->good ())
	mygood++;
      foo = locs.next (foo);
    }
    assert (good == mygood);
  }
  delete lw;
}

void
locationtable::pinpred (const chordID &x)
{
  locwrap *lw = locs[x];
  if (lw)
    lw->type_ |= LOC_PINPRED;
  else {
    lw = New locwrap (x, LOC_PINPRED);
    locs.insert (lw);
    loclist.insert (lw);
    // DO NOT insert into cachedlocs.
  }
}

void
locationtable::pinsucc (const chordID &x)
{
  locwrap *lw = locs[x];
  if (lw)
    lw->type_ |= LOC_PINSUCC;
  else {
    lw = New locwrap (x, LOC_PINSUCC);
    locs.insert (lw);
    loclist.insert (lw);
    // DO NOT insert into cachedlocs.
  }
}

void
locationtable::realinsert (ref<location> l)
{
  if (size_cachedlocs >= max_cachedlocs) {
    delete_cachedlocs ();
  }
  locwrap *lw = locs[l->n];
  if (lw) {
    if (lw->type_ & LOC_REGULAR) 
      warnx << "locationtable::realinsert: duplicate insertion.\n";
    lw->type_ |= LOC_REGULAR;
    lw->loc_ = l;
  } else {
    lw = New locwrap (l, LOC_REGULAR);
    locs.insert (lw);
    loclist.insert (lw);
  }
  cachedlocs.insert_tail (lw);
  size_cachedlocs++;
}

ptr<location>
locationtable::lookup (const chordID &n)
{
  locwrap *l = locs[n];
  if (!l)
    return NULL;
  if ((l->type_ & LOC_REGULAR) == 0) {
    warnx << "locationtable::lookup " << n << " is not REGULAR\n";
    return NULL;
  }

  cachedlocs.remove (l);
  cachedlocs.insert_tail (l);
  return l->loc_;
}

bool
locationtable::lookup_anyloc (const chordID &n, chordID *r)
{
  for (locwrap *l = locs.first (); l != NULL; l = locs.next (l)) {
    if (!l->good ()) continue;
    if (l->loc_->n != n) {
      *r = l->loc_->n;
      return true;
    }
  }
  return false;
}

chordID
locationtable::closestsuccloc (const chordID &x) {
  // Find the first actual successor as quickly as possible...
  locwrap *l = locs[x];
  if (l) {
    l = loclist.next (l);
    if (l == NULL)
      l = loclist.first ();
  } else {
    l = loclist.closestsucc (x);
  }
  // ...and now narrow it down to someone who's "good".
  while (l && !l->good ()) {
    l = loclist.next (l);
    if (l == NULL)
      l = loclist.first ();
  }
  chordID n = l->loc_->n;

#if 0
  // Brute force check to make sure we have the right answer.
  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
    if (!l->
    if ((l->type_ & LOC_REGULAR) == 0) continue;
    if (!l->loc_->alive) continue;
    if (!l->loc_->challenged) continue;
    if ((x == nbar) || between (x, nbar, l->loc_->n)) nbar = l->loc_->n;
  }
  if (n != nbar) {
    warnx << "Searching for " << x << "\n";
    loclist.traverse (wrap (this, &locationtable::printloc));
    panic << "locationtable::closestsuccloc " << nbar << " vs " << n << "\n";
  }
  // warnx << "closestsuccloc of " << x << " is " << n << "\n";
#endif /* 0 */  
  return n;
}

bool
locationtable::betterpred1 (chordID current, chordID target, chordID candidate)
{
  return between (current, target, candidate);
}

chordID
locationtable::closestpredloc (const chordID &x) 
{
  locwrap *l = locs[x];
  if (l) {
    l = loclist.prev (l);
    if (l == NULL)
      l = loclist.last ();
  } else {
    l = loclist.closestpred (x);
  }
  while (l && !l->good ()) {
    l = loclist.prev (l);
    if (l == NULL)
      l = loclist.last ();
  }
  
  chordID n = l->loc_->n;

#if 0
  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
    if ((l->type_ & LOC_REGULAR) == 0) continue;
    if (!l->loc_->alive) continue;
    if (!l->loc_->challenged) continue;
    if ((x == nbar) || betterpred1 (nbar, x, l->loc_->n)) nbar = l->loc_->n;
  }
  if (n != nbar) {
    warnx << "Searching for " << x << "\n";
    loclist.rtraverse (wrap (this, &locationtable::printloc));
    panic << "locationtable::closestpredloc " << nbar << " vs " << n << "\n";
  }
#endif /* 0 */  
  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return n;
}

void
locationtable::ping (const chordID &x, cbping_t cb) 
{
  ptr<chord_vnode> v = New refcounted<chord_vnode> ();
  v->n = x;
  doRPC (x, chord_program_1, CHORDPROC_NULL,
	 v, NULL,
	 wrap (this, &locationtable::ping_cb, cb));
}

void
locationtable::ping_cb (cbping_t cb, clnt_stat err) 
{
  if (err) {
    warn << "error pinging: " << err << "\n";   
    if (cb)
      (*cb) (CHORD_RPCFAILURE);
  } else {
    if (cb)
      (*cb) (CHORD_OK);
  }
}

void
locationtable::challenge (const chordID &x, cbchallengeID_t cb)
{
  ptr<location> l = lookup (x);
  if (nochallenges) {
    if (!l->challenged)
      good++;
    l->challenged = true;
    cb (x, true, chordstat (CHORD_OK));
    return;
  }
  if (!l->outstanding_cbs.empty ()) {
    // warnx << "challenge: adding cb to queue\n";
    l->outstanding_cbs.push_back (cb);
    return;
  }
  assert (l->outstanding_cbs.size () == 0);
  
  int c = random ();
  ptr<chord_challengearg> ca = New refcounted<chord_challengearg>;
  chord_challengeres *res = New chord_challengeres (CHORD_OK);
  nchallenge++;
  ca->v.n = x;
  ca->challenge = c;
  l->outstanding_cbs.push_back (cb);
  doRPC (x, chord_program_1, CHORDPROC_CHALLENGE, ca, res, 
	 wrap (mkref (this), &locationtable::challenge_cb, c, l, res));
}

void
locationtable::challenge_cb (int challenge, ptr<location> l,
			     chord_challengeres *res, clnt_stat err)
{
  bool chalok = false;
  chordstat status = res->status;
  if (err) {
    //    warnx << "challenge_cb: RPC failure " << err << "\n";
    status = CHORD_RPCFAILURE;
  } else if (res->status) {
    //    warnx << "challenge_cb: error " << res->status << "\n";
  } else if (challenge != res->resok->challenge) {
    //    warnx << "challenge_cb: challenge mismatch\n";
  } else {
    net_address r = l->addr;
    chalok = is_authenticID (l->n, r.hostname, r.port, res->resok->index);
  }

  if (chalok && !l->challenged) // if not alive, chalok is false
    good++;
  l->challenged = chalok;
  if (chalok && !l->alive)
    l->alive = true;
  
  cbchallengeID_t::ptr cb = NULL;;
  while (!l->outstanding_cbs.empty ()) {
    cb = l->outstanding_cbs.pop_front ();
    cb (l->n, chalok, status);
  }

  delete res;
}

void
locationtable::fill_getnodeext (chord_node_ext &data, const chordID &x)
{
  locwrap *lw = locs[x];
  if (!lw || (lw->type_ & LOC_REGULAR) == 0) {
    data.alive = false;
    return;
  }
  ptr<location> l = lw->loc_;
  
  data.x = x;
  data.r = l->addr;
  data.a_lat = (long) (l->a_lat * 100);
  data.a_var = (long) (l->a_var * 100);
  data.nrpc  = l->nrpc;
  data.alive = l->alive;
  
  return;
}

bool
locationtable::alive (const chordID &x)
{
  locwrap *l = locs[x];
  if (l && l->type_ & LOC_REGULAR)
    return l->loc_->alive;
  return false;
}

bool
locationtable::challenged (const chordID &x)
{
  locwrap *l = locs[x];
  if (l && l->type_ & LOC_REGULAR)
    return l->loc_->challenged;
  return false;
}

bool
locationtable::cached (const chordID &x)
{
  locwrap *l = locs[x];
  return (l && l->type_ & LOC_REGULAR);
}

net_address &
locationtable::getaddress (const chordID &n)
{
  locwrap *l = locs[n];
  assert (l);
  assert (l->type_ & LOC_REGULAR);
  return (l->loc_->addr);
}

float
locationtable::get_a_lat (const chordID &x)
{
  locwrap *l = locs[x];
  if (l && l->type_ & LOC_REGULAR)
    return l->loc_->a_lat;
  return 1e8; // xxx something big?
}
