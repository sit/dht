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

locationtable::locationtable (ptr<chord> _chordnode, int set_rpcdelay, 
			      int _max_cache, int _max_connections)
  : chordnode (_chordnode), max_cachedlocs (_max_cache), 
    max_connections (_max_connections), rpcdelay (set_rpcdelay)
{
  nrpc = 0;
  nrpcfailed = 0;
  rpcdelay = 0;
  nconnections = 0;
  size_cachedlocs = 0;
  size_connections = 0;
}

void
locationtable::insert (chordID &n, sfs_hostname s, int p, chordID &source)
{
  location *l = New location (n, s, p, source);
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
  updateloc (x, r, x);
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

bool
locationtable::betterpred2 (chordID current, chordID target, chordID newpred)
{ 
  location *c = getlocation (current);
  location *n = getlocation (newpred);
  if ((c->nrpc == 0) || (n->nrpc == 0)) {
    return between (current, target, newpred);
  } else {
    float cdelay = c->rpcdelay / c->nrpc;
    float ndelay = n->rpcdelay / n->nrpc;
    float delayratio = cdelay/ndelay;
    // fprintf (stderr, "cdelay %f ndelay %f delayratio %f\n", cdelay, ndelay, 
    //     delayratio);
    chordID cdiff = distance (c->n, target);
    chordID ndiff = distance (n->n, target);
    double cd = 1; // cdiff.getdouble ();  XXX requires changes to bigint.h
    double nd = 1; // ndiff.getdouble ();  XXX
    double diffratio = cd / nd;
    // fprintf (stderr, "cdiff: %f ndiff %f diffratio %f\n", cd, nd, 
    //     diffratio);
    if ((delayratio <= 1) && (diffratio <= 1)) 
      return 0; // c is closer in chord space and time
    else if ((delayratio > 1) && (diffratio > 1))
      return 1; // n is closer in chord space and time
    else   // true, if n is closer in time than in space
      return (delayratio > diffratio);
  }
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
locationtable::cacheloc (chordID &x, net_address &r, chordID &source)
{
  if (locs[x] == NULL) {
    // warnx << "cacheloc: " << x << " at port " << r.port << " source: " 
    //  << source << "\n";
    location *loc = New location (x, r, source);
    locs.insert (loc);
    add_cachedlocs (loc);
  }
}

void
locationtable::updateloc (chordID &x, net_address &r, chordID &source)
{
  if (locs[x] == NULL) {
    // warnx << "updateloc: ADD " << x << " at port " << r.port << " source: " 
    //  << source << "\n";
    location *loc = New location (x, r, source);
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
    m = l->connecting ? l->refcnt - 1 : l->refcnt;
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
  //  warnx << "add_cachedlocs : add " << l->n << " size lru " 
  //          << size_cachedlocs << " max lru " << max_cachedlocs << "\n";
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
  assert (!l->connecting);
  locs.remove (l);
  delete_connections (l);
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
