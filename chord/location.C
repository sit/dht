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

#include "chord.h"

locationtable::locationtable (ptr<chord> _chordnode, int set_rpcdelay)
  : chordnode (_chordnode), rpcdelay (set_rpcdelay)
{
  nrpc = 0;
  nrpcfailed = 0;
  rpcdelay = 0;
}

void
locationtable::insert (chordID &n, sfs_hostname s, int p, chordID &source)
{
  location *l = New location (n, s, p, source);
  locs.insert (l);
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
  updateloc (x, r, x);
  if (n->alive) deleteloc (n->n);
  n->n = x;
  n->alive = true;
}

void
locationtable::replacenode (node *n)
{
  warnx << "replace " << n->n << " with " << findsuccloc (n->n) << "\n";
  n->n = findsuccloc (n->n);
  n->alive = true;
  increfcnt (n->n);
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

chordID
locationtable::findpredloc (chordID x) {
  chordID n = x;
  for (location *l = locs.first (); l; l = locs.next (l)) {
    if (l->refcnt == 0) continue;
    if ((x == n) || between (n, x, l->n)) n = l->n;
  }
  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return n;
}

void
locationtable::cacheloc (chordID &x, net_address &r, chordID &source)
{
  if (locs[x] == NULL) {
     warnx << "cacheloc: " << x << " at port " << r.port << " source: " 
    	  << source << "\n";
    location *loc = New location (x, r, source);
    locs.insert (loc);
    // doActionCallbacks(x, ACT_NODE_JOIN);
  }
}

void
locationtable::updateloc (chordID &x, net_address &r, chordID &source)
{
  if (locs[x] == NULL) {
    warnx << "updateloc: ADD " << x << " at port " << r.port << " source: " 
    	  << source << "\n";
    location *loc = New location (x, r, source);
    loc->refcnt++;
    locs.insert (loc);
    //    defp2p->doActionCallbacks(x, ACT_NODE_JOIN);
  } else {
    locs[x]->refcnt++;
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
  if (l->refcnt <= 0) {
    //    assert (n != defp2p->my_ID ());
    warnx << "REMOVE: " << l->n << "\n";
    assert (!l->connecting);
    locs.remove (l);
    delete l;
  }
}

void
locationtable::increfcnt (chordID &n)
{
  location *l = locs[n];
  assert (l);
  l->refcnt++;
}

void
locationtable::checkrefcnt (int i)
{
  int n;
  int m;
  chordID x;

  // warnx << "checkrefcnt: " << i << "\n";
  for (location *l = locs.first (); l != NULL; l = locs.next (l)) {
    x = l->n;
    n = chordnode->countrefs (x);
    m = l->connecting ? l->refcnt - 1 : l->refcnt;
    if (n != m) {
      //      warnx << "checkrefcnt " << i << " for " << x << " : refcnt " 
      //	      << l->refcnt << " appearances " << n << "\n";
    }
  }
}

