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
#include "location.h"

// XXX should include succ_list.h but that's too much hassle.
#define NSUCC 10

bool nochallenges;
const int CHORD_RPC_STP (0);
const int CHORD_RPC_SFSU (1);
const int CHORD_RPC_SFST (2);
const int CHORD_RPC_DEFAULT (CHORD_RPC_STP);

int chord_rpc_style (getenv ("CHORD_RPC_STYLE") ?
		     atoi (getenv ("CHORD_RPC_STYLE")) :
		     0);

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

locationtable::locwrap *
locationtable::next (locwrap *lw)
{
  if (!lw)
    return NULL;
  lw = loclist.next (lw);
  if (lw == NULL)
    lw = loclist.first ();
  return lw;
}

locationtable::locwrap *
locationtable::prev (locwrap *lw)
{
  if (!lw)
    return NULL;
  lw = loclist.prev (lw);
  if (lw == NULL)
    lw = loclist.last ();
  return lw;
}

static void
ignore_challengeresp (chordID x, bool b, chordstat s)
{
  warnx << "Ignoring " << ((b && s == CHORD_OK) ? "good" : "bad")
	<< " challenge response for " << x << "\n";
}
cbchallengeID_t cbchall_null (wrap (ignore_challengeresp));

location::location (const chordID &_n, const net_address &_r) 
  : n (_n), addr (_r), alive (true), checkdeadcb (NULL), challenged (false)
{
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (_r.hostname.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (addr.port);
};

location::~location () {
  if (checkdeadcb) {
    timecb_remove (checkdeadcb);
    checkdeadcb = NULL;
  }
  warnx << "~location: delete " << n << "\n";
}

void
locationtable::initialize_rpcs ()
{
  if (chord_rpc_style == CHORD_RPC_SFSU)
    hosts = New refcounted<rpc_manager> (nrcv);
  else if (chord_rpc_style == CHORD_RPC_SFST)
    hosts = New refcounted<tcp_manager> (nrcv);
  else
    hosts = New refcounted<stp_manager> (nrcv);

  // see also chord::startchord
}

locationtable::locationtable (ptr<u_int32_t> _nrcv, int _max_cache)
  : nrcv (_nrcv),
    good (0),
    size_cachedlocs (0),
    max_cachedlocs (_max_cache), 
    nnodessum (0),
    nnodes (0),
    nvnodes (0),
    nchallenge (0)
{
  initialize_rpcs ();
}

locationtable::locationtable (const locationtable &src)
{
  nrcv = src.nrcv;
  max_cachedlocs = src.max_cachedlocs;

  initialize_rpcs ();

  // State parameters will be zeroed in the copy
  good = 0;
  size_cachedlocs = 0; 
  nvnodes = 0;
  nnodes = 0;
  nnodessum = 0;
  nchallenge = 0;

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
}

bool
locationtable::locwrap::good ()
{
  return ((type_ & LOC_REGULAR) &&
	  loc_ &&
	  loc_->alive &&
	  loc_->challenged);
}

size_t
locationtable::size ()
{
  return locs.size ();
}

size_t
locationtable::usablenodes ()
{
  return good;
}

u_long
locationtable::estimate_nodes ()
{
  return (good > nnodes) ? good : nnodes;
}

void
locationtable::replace_estimate (u_long o, u_long n)
{
  assert (nvnodes > 0);
  nnodessum = nnodessum - o + n;
  nnodes = nnodessum / nvnodes;
}

void
locationtable::resendRPC (long seqno)
{
  return hosts->rexmit (seqno);
}

long
locationtable::doRPC (const chordID &n, rpc_program progno, 
		      int procno, ptr<void> in, 
		      void *out, aclnt_cb cb, bool dead)
{
  ptr<location> l = lookup (n);
  if (!l) panic << "location (" << n << ") is null. forgot to call cacheloc\n?";
  l->nrpc++;
  if (!dead)
    return hosts->doRPC (l, progno, procno, in, out, 
			 wrap (this, &locationtable::doRPCcb, l, cb));
  else
    return hosts->doRPC_dead (l, progno, procno, in, out, 
			      wrap (this, &locationtable::doRPCcb, l, cb));

}

void
locationtable::doRPCcb (ptr<location> l, aclnt_cb realcb, clnt_stat err)
{
  assert (locs[l->n]);
  // This code assumes that the location is still in the locs[], etc
  // structures.  Otherwise, the good bookkeeping is probably wrong and
  // the checkdead stuff is extraneous.  However, it is unlikely (?)
  // that a node would get evicted while it has an RPC in flight.
  
  if (err) {
    if (l->alive) {
      // xxx should really kill all nodes that share the same ip/port.
      //     maybe should increase granularity of hostinfo cache for
      //     stp from per-host to per ip/port... but this is no worse
      //     than before.
      if (l->challenged)
	good--;
      l->alive = false;
      if (!l->checkdeadcb)
	l->checkdeadcb = delaycb (60, 0,
	  wrap(this, &locationtable::check_dead, l, 120));
    }
  } else {
    if (!l->alive) {
      l->alive = true;
      if (l->challenged)
	good++;
    }
  }

  (realcb) (err);
}

void
locationtable::insertgood (const chordID &n, sfs_hostname s, int p)
{
  locwrap *lw;
  if ((lw = locs[n])) {
    warnx << "re-INSERT (good): " << n << "\n";
    if (lw->type_ & LOC_REGULAR) {
      if (!lw->good ()) {
      lw->loc_->challenged = true;
      lw->loc_->alive = true;
      good++;
      return;
      }
      // if already good, nothing to do.
    }
    // if it was here as part of a pin, then the fall
    // through path will finish the insert (via realinsert).
  }

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
locationtable::cacheloc (const chordID &x, const net_address &r, cbchallengeID_t cb)
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
    //    warn << "cacheloc " << x << " challenging\n";
    challenge (x, cb); // queue up for additional callback
  } else {
    // state = "old";
    if (cb != cbchall_null)
      cb (x, lx->loc_->challenged, CHORD_OK);
  }
  // warnx << "CACHELOC (" << state << "): " << x << " at port " << r.port << "\n";
}

void
locationtable::delete_cachedlocs (void)
{
  if (!size_cachedlocs)
    return;

  // First, try to find a bad node
  locwrap *lw = cachedlocs.first;
  while (lw &&
	 (lw->good () || (lw->loc_ && lw->loc_->outstanding_cbs.size ()))) {
      lw = cachedlocs.next (lw);
      continue;
  }
  if (lw) {
    remove (lw);
    return;
  }
  // Else, find some unimportant cached node to evict.
  assert (!lw);
  warnx << "locationtable::delete_cachedlocs: no bad nodes to evict.\n";

  lw = cachedlocs.first;
  unsigned int n = NSUCC;
  locwrap *c = NULL;
  while (lw) {
    // Continue to skip over those that are being challenged or
    // are directly pinned
    if (lw->loc_->outstanding_cbs.size () ||
	(prev (lw) && prev (lw)->type_ & LOC_PINSUCC) ||
	(next (lw) && next (lw)->type_ & LOC_PINPRED)) {
      goto nextcandidate;
    }
    // Next, check to see if it is part of a predecessor or
    // successor list by scanning to try and find a PINSUCCLIST/PINPREDLIST
    // within NSUCC.
    n = NSUCC;
    c = lw;
    while (n > 0) {
      c = prev (c);
      if (c == lw) break; // loop, so this check is okay
      if (c->type_ & LOC_PINSUCCLIST)
	goto nextcandidate;
      n--;
    }
    n = NSUCC; c = lw;
    while (n > 0) {
      c = next (c);
      if (c == lw) break; 
      if (c->type_ & LOC_PINPREDLIST)
	goto nextcandidate;
      n--;
    }
    // If we're here, then this node has no reason to be saved.
    remove (lw);
    
  nextcandidate:
    lw = cachedlocs.next (lw);
  }
}

void
locationtable::pin (const chordID &x, loctype pt)
{
  locwrap *lw = locs[x];
  if (lw)
    lw->type_ |= pt;
  else {
    lw = New locwrap (x, pt);
    locs.insert (lw);
    loclist.insert (lw);
    // DO NOT insert into cachedlocs.
  }
}

void
locationtable::pinpred (const chordID &x)
{
  pin (x, LOC_PINPRED);
}

void
locationtable::pinpredlist (const chordID &x)
{
  pin (x, LOC_PINPREDLIST);
}

void
locationtable::pinsucc (const chordID &x)
{
  pin (x, LOC_PINSUCC);
}

void
locationtable::pinsucclist (const chordID &x)
{
  pin (x, LOC_PINSUCCLIST);
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

void
locationtable::get_node (const chordID &x, chord_node *n)
{
  ptr<location> loc = lookup (x);
  if (!loc) return;
  n->x = loc->n;
  n->r = loc->addr;
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
  // Recall that responsibility is right inclusive, n is responsible
  // for keys in (p, n].
  locwrap *l = locs[x];
  if (!l)
    l = loclist.closestsucc (x);

  // ...and now narrow it down to someone who's "good".
  while (l && !l->good ())
    l = next (l);

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
locationtable::closestpredloc (const chordID &x, vec<chordID> failed) 
{
  locwrap *l = locs[x];
  if (l) {
    l = prev (l);
  } else {
    l = loclist.closestpred (x);
  }
  while (l && (!l->good () || (l->loc_ && in_vector (failed, l->loc_->n))))
    l = prev (l);
  
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

chordID
locationtable::closestpredloc (const chordID &x) 
{
  locwrap *l = locs[x];
  if (l) {
    l = prev (l);
  } else {
    l = loclist.closestpred (x);
  }
  while (l && !l->good ())
    l = prev (l);
  
  chordID n = l->loc_->n;

  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return n;
}

void
locationtable::ping (const chordID &x, cbping_t cb) 
{
  ptr<chordID> v = New refcounted<chordID> (x);
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
  if (!l) {
    warn << "locationtable::challenge() l is NULL\n";
    cb (x, false, chordstat (CHORD_ERRNOENT));
  }

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
  ca->v = l->n;
  ca->challenge = c;
  l->outstanding_cbs.push_back (cb);
  doRPC (l->n, chord_program_1, CHORDPROC_CHALLENGE, ca, res, 
	 wrap (mkref (this), &locationtable::challenge_cb, c, l, res), true);
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
  data.a_lat = (long) (hosts->get_a_lat (l) * 100);
  data.a_var = (long) (hosts->get_a_var (l) * 100);
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

const net_address &
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
  assert (l);
  assert (l->type_ & LOC_REGULAR);
  
  return hosts->get_a_lat (l->loc_);
}

unsigned int
locationtable::get_nrpc (const chordID &x)
{
  locwrap *l = locs[x];
  assert (l);
  assert (l->type_ & LOC_REGULAR);
  
  return l->loc_->nrpc;
  
}

float 
locationtable::get_avg_lat ()
{
  return hosts->get_avg_lat();
}

float 
locationtable::get_avg_var ()
{
  return hosts->get_avg_var();
}


void
locationtable::stats ()
{
  hosts->stats ();
}

void
locationtable::check_dead (ptr<location> l, unsigned int newwait)
{
  warnx << "check_dead: " << l->n << "\n";
  l->checkdeadcb = NULL;

  // Only bother if we still have this node around.
  // This is purely defensive code; check_dead should only be
  // called from a timecb and that should be removed during eviction.
  // See locationtable::remove.
  if (!locs[l->n]) {
    warnx << "check_dead: " << l->n << ": already evicted.\n";
    return;
  }
  
  // make sure we actually go out on the network and check if the node
  // is alive. if we're allowed to challenge, do that as well.
  if (nochallenges)
    ping (l->n, wrap (this, &locationtable::check_dead_cb, l, newwait,
			    l->n, false));
  else
    challenge (l->n, wrap (this, &locationtable::check_dead_cb, l, newwait));
}

void
locationtable::check_dead_cb (ptr<location> l, unsigned int newwait,
			      chordID x, bool b, chordstat s)
{
  // the challenge_cb and doRPCcb has already fixed up the
  // the cases where the node is alive. We're just here to
  // reschedule a check in case the node was still unreachable.
  
  if (s == CHORD_OK)
    return;

  if (newwait > 3600) {
    warnx << "check_dead: " << l->n << " dead too long. Giving up.\n";
    remove (locs[l->n]);
  } else {
    warnx << "check_dead: " << l->n << " still dead; waiting " << newwait << "\n";
    l->checkdeadcb = delaycb (newwait, 0,
      wrap (this, &locationtable::check_dead, l, newwait*2));
  }
}

bool
locationtable::remove (locwrap *lw)
{
  if (!lw)
    return false;
  
  // XXX pray that there aren't any references to this location
  //     floating around and certainly no references to its chordID
  
  if (lw->good ()) good--;
  
  locs.remove (lw);
  loclist.remove (lw->n_);
  cachedlocs.remove (lw);
  size_cachedlocs--;

  if ((lw->type_ & LOC_REGULAR) && lw->loc_->checkdeadcb) {
    timecb_remove (lw->loc_->checkdeadcb);
    lw->loc_->checkdeadcb = NULL;
  }

  {
    // This code is here only for sanity checking.
    locwrap *foo = locs.first ();
    size_t mygood = 0;
    while (foo) {
      if (foo->good ())
	mygood++;
      foo = locs.next (foo);
    }
    if (good != mygood) {
      warn << "good(" << good << ") isn't == to mygood(" << mygood << ")\n";
      good = mygood;
    }
  }
  delete lw;
  return true;
}
  
