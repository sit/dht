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

#include <math.h>
#include "chord.h"

location::location (chordID &_n, net_address &_r) 
  : n (_n), addr (_r) 
{
  refcnt = 0;
  rpcdelay = 0;
  nrpc = 0;
  a_lat = 0.0;
  maxdelay = 0;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (_r.hostname.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (addr.port);
};

location::~location () {
  warnx << "~location: delete " << n << "\n";
}

locationtable::locationtable (ptr<chord> _chordnode, int set_rpcdelay, 
			      int _max_cache, int _max_connections)
  : chordnode (_chordnode), max_cachedlocs (_max_cache), 
     rpcdelay (set_rpcdelay)
{
  nrpc = 0;
  nrpcfailed = 0;
  rpcdelay = 0;
  a_lat = 0.0;
  nsent = 0;
  npending = 0;
  size_cachedlocs = 0;
  nvnodes = 0;
  nnodes = 0;
  nnodessum = 0;
  last_xid = 0;
  cwind = 1;
  left = 0;
  idle_timer = NULL;

  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";

  delaycb (1, 0, wrap (this, &locationtable::ratecb));

  reset_idle_timer ();
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
locationtable::insert (chordID &n, sfs_hostname s, int p)
{
  net_address r;
  r.hostname = s;
  r.port = p;
  location *l = New location (n, r);
  assert (!locs[n]);
  locs.insert (l);
  add_cachedlocs (l);
}

location *
locationtable::getlocation (chordID &x)
{
  location *l = locs[x];
  return l;
}

void
locationtable::changenode(node *n, chordID &x, net_address &r)
{
  updateloc (x, r);
  if (n->alive) deleteloc (n->n);
  n->n = x;
  n->alive = true;
}

net_address &
locationtable::getaddress (chordID &n)
{
  location *l = locs[n];
  assert (l);
  return (l->addr);
}

bool
locationtable::lookup_anyloc (chordID &n, chordID *r)
{
  for (location *l = locs.first (); l != NULL; l = locs.next (l)) {
    if (l->n != n) {
      *r = l->n;
      return true;
    }
  }
  return false;
}

chordID
locationtable::query_location_table (chordID x) {
  location *l = locs.first ();
  chordID min = bigint(1) << 160;
  chordID ret = -1;
  while (l) {
    chordID d = diff(l->n, x);
    if (d < min) { min = d; ret = l->n; }
    l = locs.next (l);
  }
  return ret;
}

chordID
locationtable::closestsuccloc (chordID x) {
  chordID n = x;
  for (location *l = locs.first (); l; l = locs.next (l)) {
    if (l->refcnt == 0) continue;
    if ((x == n) || between (x, n, l->n)) n = l->n;
  }
  // warnx << "closestsuccloc of " << x << " is " << n << "\n";
  return n;
}

bool
locationtable::betterpred1 (chordID current, chordID target, chordID candidate)
{
  return between (current, target, candidate);
}

// assumes some of form of the triangle equality!
char
locationtable::betterpred2 (chordID myID, chordID current, chordID target, 
			    chordID newpred)
{ 
  #define HIST 0
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  char r = 0;
  if (between (myID, target, newpred)) { // is newpred a possible pred?

    location *cur_loc = getlocation (current);
    location *proposed_loc = getlocation (newpred);

    if ((current == myID) && (newpred != myID)) {
      r = 1;
    } else if ((cur_loc->nrpc == 0) || (proposed_loc->nrpc == 0)) {
      if (between (current, target, newpred)) r = 2;
    } else {
      int nbit;
      if (nnodes <= 1) nbit = 0;
      //else nbit = log2 (nnodes / log2 (nnodes));
      else {
	float n = nnodes;
	float log2_nnodes = logf(n)/logf(2.0);
	float div = n / log2_nnodes;
	nbit = (int)ceilf ( logf(div)/logf(2.0));
      };

      u_long target_bits = topbits (nbit, target);
      u_long current_bits = topbits (nbit, current);
      u_long prop_bits = topbits (nbit, newpred);

      u_long cur_diff = (target_bits > current_bits) ? 
	target_bits - current_bits :
	current_bits - target_bits;
      u_long prop_diff = (target_bits > prop_bits) ? 
	target_bits - prop_bits : 
	prop_bits - target_bits;
      if (n1bits (cur_diff) > n1bits (prop_diff)) { 
	r = 3;
      } else if (n1bits (cur_diff) == n1bits (prop_diff)) {
	float cur_delay = cur_loc->rpcdelay / cur_loc->nrpc;
	float proposed_delay = proposed_loc->rpcdelay / proposed_loc->nrpc;
	if ((proposed_delay + HIST) < cur_delay) r = 4;

      }
    }
  }
  

  return r;
}


// assumes some of form of the triangle equality!
bool
locationtable::betterpred3 (chordID myID, chordID current, chordID target, 
			    chordID newpred)
{ 
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  bool r = false;
  if (between (myID, target, newpred)) { // is newpred a possible pred?
    location *c = getlocation (current);
    location *n = getlocation (newpred);
    if ((current == myID) && (newpred != myID)) {
      r = true;
    } else if ((c->nrpc == 0) || (n->nrpc == 0)) {
      r = between (current, target, newpred);
    } else {
      int nbit;
      if (nnodes <= 1) nbit = 0;
      else {
	float n = nnodes;
	float log2_nnodes = logf(n)/logf(2.0);
	float div = n / log2_nnodes;
	nbit = (int)ceilf ( logf(div)/logf(2.0));
      };
      u_long target_bits = topbits (nbit, target);
      u_long current_bits = topbits (nbit, current);
      u_long new_bits = topbits (nbit, newpred);
      u_long cur_diff = (target_bits > current_bits) ? 
	target_bits - current_bits :
	current_bits - target_bits;
      u_long new_diff = (target_bits > new_bits) ? 
	target_bits - new_bits : 
	new_bits - target_bits;

      float avg_delay = (float)rpcdelay/nrpc;
      float cur_delay = c->rpcdelay / c->nrpc;
      float new_delay = n->rpcdelay / n->nrpc;

      float cur_est = n1bits (cur_diff)*avg_delay + cur_delay;
      float new_est = n1bits (new_diff)*avg_delay + new_delay;

      float diff = cur_est - new_est;
      if (::fabs(diff) < 10000) 
	return between (current, target, newpred);

      r = (cur_est > new_est);
#if 0
      if (r) {
	char b[1024];
	sprintf (b, "cur_est=%f*%ld + %f=%f > new_est=%f*%ld + %f=%f\n", 
		 avg_delay, n1bits(cur_diff), cur_delay, cur_est,
		 avg_delay, n1bits(new_diff), new_delay, new_est);
	warn << "chose " << newpred << " since " << b;
      }
#endif      
     
    }
  }
  
  
  return r;
}


// assumes some of form of the triangle equality!
char
locationtable::betterpred_distest (chordID myID, chordID current, 
				   chordID target, 
				   chordID newpred)
{ 
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  char r = false;
  if (between (myID, target, newpred)) { // is newpred a possible pred?
    location *c = getlocation (current);
    location *n = getlocation (newpred);
    if ((current == myID) && (newpred != myID)) {
      r = 1;
    } else if ((c->nrpc == 0) || (n->nrpc == 0)) {
      if (between (current, target, newpred)) r = 2;
    } else {
      double D = (double)rpcdelay/nrpc;
      double cur_delay = (double)c->rpcdelay / c->nrpc;
      double new_delay = (double)n->rpcdelay / n->nrpc;
      double log2 = log(2);
      int N = nnodes;

      bigint dist_c = distance (current, target)*N;
      assert (NBIT > 32);
      int dist_size = dist_c.nbits ();
      int shift = (dist_size - 32 > 0) ? dist_size - 32 : 0;
      bigint high_bits = dist_c >> shift;
      double dist_c_exp = (double)high_bits.getui ();
      double fdist_c = ldexp (dist_c_exp, shift);
      double logdist_c = log (fdist_c)/log2;
      if (logdist_c < 0.0) logdist_c = 0.0;
      double d_current = (logdist_c - 160.0)*D + cur_delay;
      
      bigint dist_p = distance (newpred, target)*N;
      assert (NBIT > 32);
      dist_size = dist_p.nbits ();
      shift = (dist_size - 32 > 0) ? dist_size - 32 : 0;
      high_bits = dist_p >> shift;
      double dist_p_exp = (double)high_bits.getui ();
      double fdist_p = ldexp (dist_p_exp, shift);
      double logdist_p = log (fdist_p)/log2;
      if (logdist_p < 0.0) logdist_p = 0.0;
      double d_proposed = (logdist_p - 160.0)*D + new_delay;

      if (d_proposed < d_current) 
	r = 3;
      if (1) {
	char b[1024];
	sprintf (b, "d_cur = %f = %f*%f + %f; d_proposed = %f = %f*%f + %f", 
		 d_current, logdist_c - 160.0, D, cur_delay,
		 d_proposed, logdist_p - 160.0, D, new_delay);
	if (r) 
	  warn << "choosing " << newpred << " over " << current << " since " << b << "\n";
	else
	  warn << "choosing " << current << " over " << newpred << " since " << b << "\n";
      }
    }
  }
  
  
  return r;
}

bool
locationtable::betterpred_greedy (chordID myID, chordID current, 
				  chordID target, chordID newpred) 
{
  bool r = false;
  if ((current == myID) && (newpred != myID)) return true;
  if (between (myID, target, newpred)) { 
    location *c = getlocation (current);
    location *n = getlocation (newpred);
    if (c->nrpc == 0) return true;
    float cur_delay = c->rpcdelay / c->nrpc;
    if (n->nrpc == 0) return false;
    float new_delay = n->rpcdelay / n->nrpc;
    r = (new_delay < cur_delay);
  }
  return r;
}

chordID
locationtable::closestpredloc (chordID x) 
{
  chordID n = x;
  for (location *l = locs.first (); l; l = locs.next (l)) {
    if (l->refcnt == 0) continue;
    if ((x == n) || betterpred1 (n, x, l->n)) n = l->n;
  }
  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return n;
}

void
locationtable::cacheloc (chordID &x, net_address &r)
{
  if (locs[x] == NULL) {
    // warnx << "cacheloc: " << x << " at port " << r.port << "\n";
    location *loc = New location (x, r);
    locs.insert (loc);
    add_cachedlocs (loc);
  }
}

void
locationtable::updateloc (chordID &x, net_address &r)
{
  if (locs[x] == NULL) {
    // warnx << "updateloc: ADD " << x << " at port " << r.port << "\n";
    location *loc = New location (x, r);
    loc->refcnt++;
    locs.insert (loc);
  } else {
    increfcnt (x);
    // XXX check whether address hasn't changed.
  }
}

void
locationtable::deleteloc (chordID &n)
{
  location *l = locs[n];
  assert (l);
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
  assert (l);
  l->refcnt++;
  if (l->refcnt == 1) {
    remove_cachedlocs (l);
  }
}

void
locationtable::checkrefcnt (int i)
{
  int n;
  int m;
  chordID x;

  for (location *l = locs.first (); l != NULL; l = locs.next (l)) {
    x = l->n;
    n = chordnode->countrefs (x);
    m = l->refcnt;
    if (n != m) {
      warnx << "checkrefcnt " << i << " for " << x << " : refcnt " 
            << l->refcnt << " appearances " << n << "\n";
      assert (0);
    }
  }
}

void
locationtable::touch_cachedlocs (location *l)
{
  if (l->refcnt > 0) return;
  assert (l->refcnt == 0);
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
  assert (l->refcnt == 0);
  warnx << "DELETE: " << l->n << "\n";
  locs.remove (l);
  cachedlocs.remove (l);
  size_cachedlocs--;
  delete l;
}

void
locationtable::remove_cachedlocs (location *l)
{
  assert (l->refcnt > 0);
  cachedlocs.remove (l);
  size_cachedlocs--;
}
