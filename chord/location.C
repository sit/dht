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
  nsent = 0;
  npending = 0;
  size_cachedlocs = 0;
  nvnodes = 0;
  nnodes = 0;
  nnodessum = 0;
  last_xid = 0;
  
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";

  delaycb (1, 0, wrap (this, &locationtable::ratecb));
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
locationtable::findsuccloc (chordID x) {
  chordID n = x;
  for (location *l = locs.first (); l; l = locs.next (l)) {
    if (l->refcnt == 0) continue;
    if ((x == n) || between (x, n, l->n)) n = l->n;
  }
  // warnx << "findsuccloc of " << x << " is " << n << "\n";
  return n;
}

bool
locationtable::betterpred1 (chordID current, chordID target, chordID candidate)
{
  return between (current, target, candidate);
}

// assumes some of form of the triangle equality!
bool
locationtable::betterpred2 (chordID myID, chordID current, chordID target, 
			    chordID newpred)
{ 
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  bool r = false;
  if (between (myID, target, newpred)) { // is newpred a possible pred?

    location *cur_loc = getlocation (current);
    location *proposed_loc = getlocation (newpred);

    if ((current == myID) && (newpred != myID)) {
      r = true;
    } else if ((cur_loc->nrpc == 0) || (proposed_loc->nrpc == 0)) {
      r = between (current, target, newpred);
    } else {
      u_long nbit = 2;
      //      if (nnodes <= 1) nbit = 0;
      // else nbit = log2 (nnodes / log2 (nnodes));

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
	r = true;
      } else if (n1bits (cur_diff) == n1bits (prop_diff)) {
	float cur_delay = cur_loc->rpcdelay / cur_loc->nrpc;
	float proposed_delay = proposed_loc->rpcdelay / proposed_loc->nrpc;
	r = proposed_delay < cur_delay;
#if 0
	if (r) {
	  char b[1024];
	  sprintf (b, "cdelay=%f > ndelay=%f\n", cdelay, ndelay);
	  warn << "chose " << newpred << " since " << b;
	}
#endif
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
      u_long nbit = 2;
      //      if (nnodes <= 1) nbit = 0;
      // else nbit = log2 (nnodes / log2 (nnodes));
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

bool
locationtable::betterpred_greedy (chordID myID, chordID current, 
				  chordID target, chordID newpred) 
{
  bool r = false;
  if (between (myID, target, newpred)) { 
    location *c = getlocation (current);
    location *n = getlocation (newpred);
    float cur_delay = c->rpcdelay / c->nrpc;
    float new_delay = n->rpcdelay / n->nrpc;
    r = (new_delay < cur_delay);
  }
  return r;
}

chordID
locationtable::findpredloc (chordID x) 
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
