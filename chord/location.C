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
static void
printloc (location *l)
{
  assert (l);
  warnx << "Location " << l->n << " (A=" << l->alive << ", C="
	<< l->challenged << ")\n";
}
#endif /* 0 */

static void
ignore_challengeresp (chordID x, bool b, chordstat s)
{
  warnx << "Ignoring " << ((b && s == CHORD_OK) ? "good" : "bad")
	<< " challenge response for " << x << "\n";
}
cbchallengeID_t cbchall_null (wrap (ignore_challengeresp));

location::location (chordID &_n, net_address &_r) 
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

  // Deep copy the list of locations.
  for (location *l = src.locs.first (); l; l = src.locs.next (l)) {
    if (!l->alive) continue;
    location *loc = New location (l->n, l->addr);
    loc->challenged = l->challenged;
    locs.insert (loc);
    loclist.insert (loc);
    add_cachedlocs (loc);
    if (loc->challenged) good++;
  }
  
  start_network ();
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
locationtable::insertgood (chordID &n, sfs_hostname s, int p)
{
  assert (!locs[n]);

  net_address r;
  r.hostname = s; r.port = p;
  
  location *loc = New location (n, r);
  loc->challenged = true; // force goodness
  locs.insert (loc);
  loclist.insert (loc);
  add_cachedlocs (loc);
  good++;
  // warnx << "INSERT (good): " << n << "\n";
}

void
locationtable::insert (chordID &n, sfs_hostname s, int p, cbchallengeID_t cb)
{
  assert (!locs[n]);
  
  net_address r;
  r.hostname = s;
  r.port = p;
  cacheloc (n, r, cb);
}

bool
locationtable::lookup_anyloc (chordID &n, chordID *r)
{
  for (location *l = locs.first (); l != NULL; l = locs.next (l)) {
    if (!l->alive) continue;
    if (!l->challenged) continue;
    if (l->n != n) {
      *r = l->n;
      return true;
    }
  }
  return false;
}

chordID
locationtable::closestsuccloc (chordID x) {
  // Find the first actual successor as quickly as possible...
  location *l = locs[x];
  if (l) {
    l = loclist.next (l);
    if (l == NULL)
      l = loclist.first ();
  } else {
    l = loclist.closestsucc (x);
  }
  // ...and now narrow it down to someone who's "good".
  while (l && (!l->alive || !l->challenged)) {
    l = loclist.next (l);
    if (l == NULL)
      l = loclist.first ();
  }
  chordID n = l->n;

#if 0
  // Brute force check to make sure we have the right answer.
  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
    if (!l->alive) continue;
    if (!l->challenged) continue;
    if ((x == nbar) || between (x, nbar, l->n)) nbar = l->n;
  }
  if (n != nbar) {
    warnx << "Searching for " << x << "\n";
    loclist.traverse (wrap (printloc));
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
locationtable::closestpredloc (chordID x) 
{
  location *l = locs[x];
  if (l) {
    l = loclist.prev (l);
    if (l == NULL)
      l = loclist.last ();
  } else {
    l = loclist.closestpred (x);
  }
  while (l && (!l->alive || !l->challenged)) {
    l = loclist.prev (l);
    if (l == NULL)
      l = loclist.last ();
  }
  
  chordID n = l->n;

#if 0
  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
    if (!l->alive) continue;
    if (!l->challenged) continue;
    if ((x == nbar) || betterpred1 (nbar, x, l->n)) nbar = l->n;
  }
  if (n != nbar) {
    warnx << "Searching for " << x << "\n";
    loclist.rtraverse (wrap (printloc));
    panic << "locationtable::closestpredloc " << nbar << " vs " << n << "\n";
  }
#endif /* 0 */  
  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return n;
}

void
locationtable::cacheloc (chordID &x, net_address &r, cbchallengeID_t cb)
{
  // char *state;
  if (locs[x] == NULL) {
    // state = "new";    
    location *loc = New location (x, r);
    locs.insert (loc);
    loclist.insert (loc);
    add_cachedlocs (loc);
    challenge (x, cb);
  } else if (locs[x]->alive == false || locs[x]->challenged == false) {
    // state = "pending";
    challenge (x, cb); // queue up for additional callback
  } else {
    // state = "old";
    if (cb != cbchall_null)
      cb (x, locs[x]->challenged, CHORD_OK);
  }
#ifdef PNODE
  // if (myvnode)
  //   warnx << myvnode->myID << " ";
#endif /* PNODE */
  // warnx << "CACHELOC (" << state << "): " << x << " at port " << r.port << "\n";
}

#if 0
void
locationtable::updateloc (chordID &x, net_address &r, cbchallengeID_t cb)
{
  if (locs[x] == NULL) {
    // warnx << "UPDATELOC: " << x << " at port " << r.port << "\n";
    location *loc = New location (x, r);
    // loc->refcnt++;
    locs.insert (loc);
    loclist.insert (loc);
    challenge (x, cb);
  } else {
    // increfcnt (x);
    if (locs[x]->addr.hostname != r.hostname ||
	locs[x]->addr.port     != r.port) {
      warnx << "locationtable::updateloc: address changed!!!\n";
      locs[x]->addr = r;
      locs[x]->challenged = false;
      challenge (x, cb);
    } else if (cb != cbchall_null) {
      // warnx << "UPDATELOC (old): " << x << " at port " << r.port << "\n";
      cb (x, locs[x]->challenged, CHORD_OK);
    }
  }
}
#endif /* 0 */

#if 0
void
locationtable::decrefcnt (chordID &n)
{
  location *l = locs[n];
  if (!l)
    panic << "locationtable::decrefcnt: no location for " << n << "\n";
  decrefcnt (l);
}

void
locationtable::decrefcnt (location *l)
{
  l->refcnt--;
  assert (l->refcnt >= 0);
  if (l->refcnt == 0) add_cachedlocs (l);
}

void
locationtable::increfcnt (chordID &n)
{
  location *l = locs[n];
  if (!l)
    panic << "locationtable::increfcnt: no location for " << n << "\n";
  l->refcnt++;
  if (l->refcnt == 1) {
    remove_cachedlocs (l);
  }
}
#endif /* 0 */

void
locationtable::touch_cachedlocs (location *l)
{
  //  if (l->refcnt > 0) return;
  //  assert (l->refcnt == 0);
  cachedlocs.remove (l);
  cachedlocs.insert_tail (l);
}

void
locationtable::add_cachedlocs (location *l)
{
  if (size_cachedlocs >= max_cachedlocs) {
    delete_cachedlocs ();
  }
  cachedlocs.insert_tail (l);
  size_cachedlocs++;
}

void
locationtable::delete_cachedlocs (void)
{
  location *l = cachedlocs.first;
  assert (l);
  //  assert (l->refcnt == 0);
  // warnx << "DELETE: " << l->n << "\n";
  locs.remove (l);
  loclist.remove (l->n);
  if (l->alive && l->challenged) good--;
  cachedlocs.remove (l);
  size_cachedlocs--;
  delete l;
}

void
locationtable::remove_cachedlocs (location *l)
{
  //  assert (l->refcnt > 0);
  cachedlocs.remove (l);
  size_cachedlocs--;
}

void
locationtable::ping (chordID id, cbping_t cb) 
{
  ptr<chord_vnode> v = New refcounted<chord_vnode> ();
  v->n = id;
  doRPC (id, chord_program_1, CHORDPROC_NULL,
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
};

bool
locationtable::alive (chordID &x)
{
  location *l = locs[x];
  if (l == NULL)
    return false;
  return l->alive;
}

void
locationtable::challenge (chordID &x, cbchallengeID_t cb)
{
  if (nochallenges) {
    locs[x]->challenged = true;
    good++;
    cb (x, true, chordstat (CHORD_OK));
    return;
  }
  if (!locs[x]->outstanding_cbs.empty ()) {
    // warnx << "challenge: adding cb to queue\n";
    locs[x]->outstanding_cbs.push_back (cb);
    return;
  }
  assert (locs[x]->outstanding_cbs.size () == 0);
  
  int c = random ();
  ptr<chord_challengearg> ca = New refcounted<chord_challengearg>;
  chord_challengeres *res = New chord_challengeres (CHORD_OK);
  nchallenge++;
  ca->v.n = x;
  ca->challenge = c;
  locs[x]->outstanding_cbs.push_back (cb);
  doRPC (x, chord_program_1, CHORDPROC_CHALLENGE, ca, res, 
	 wrap (mkref (this), &locationtable::challenge_cb, c, x, res));
}

void
locationtable::challenge_cb (int challenge, chordID x,
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
    net_address r = getaddress (x);
    chalok = is_authenticID (x, r.hostname, r.port, res->resok->index);
  }

  location *l = locs[x];
  assert (l);

  if (chalok && !l->challenged) // if not alive, chalok is false
    good++;
  l->challenged = chalok;
  if (chalok && !l->alive)
    l->alive = true;
  
  cbchallengeID_t::ptr cb = NULL;;
  while (!l->outstanding_cbs.empty ()) {
    cb = l->outstanding_cbs.pop_front ();
    cb (x, chalok, status);
  }

  delete res;
}

void
locationtable::fill_getnodeext (chord_node_ext &data, chordID &x)
{
  location *l = locs[x];
  if (!l) {
    data.alive = false;
    return;
  }
  data.x = x;
  data.r = l->addr;
  data.a_lat = (long) (l->a_lat * 100);
  data.a_var = (long) (l->a_var * 100);
  data.nrpc  = l->nrpc;
  data.alive = l->alive;
  
  return;
}

bool
locationtable::challenged (chordID &x)
{
  location *l = locs[x];
  if (l)
    return l->challenged;
  else
    return false;
}

bool
locationtable::cached (chordID &x)
{
  location *l = locs[x];
  return (l != NULL);
}

net_address &
locationtable::getaddress (chordID &n)
{
  location *l = locs[n];
  assert (l);
  return (l->addr);
}

float
locationtable::get_a_lat (chordID &x)
{
  location *l = locs[x];
  if (!l)
    return 1e8; // xxx something big?
  return l->a_lat;
}
