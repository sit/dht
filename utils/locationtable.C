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

#include "id_utils.h"
#include "location.h"
#include "locationtable.h"
#include "misc_utils.h"
#include "modlogger.h"
#define loctrace modlogger ("loctable")

#include "configurator.h"

// XXX should use configurator value??
#define NSUCC 16

struct locationtable_init {
  locationtable_init ();
} li;

locationtable_init::locationtable_init ()
{
  Configurator::only ().set_int ("locationtable.maxcache",
				 (160 + 16 + 16 + 16));
}

#if 0
void
locationtable::printloc (locwrap *l)
{
  assert (l);
  warnx << "Locwrap " << l->n_;
  warnx << " type " << l->type_;
  if (l->type_ & LOC_REGULAR)
    warnx << " (A=" << l->loc_->alive () << ")\n";
}
#endif /* 0 */

locationtable::locwrap::locwrap (ptr<location> l, loctype lt) :
  loc_ (l), type_ (lt)
{
  n_ = l->id ();
}

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

inline bool
locationtable::locwrap::good ()
{
  return ((type_ & LOC_REGULAR) &&
	  loc_ &&
	  loc_->alive () &&
	  loc_->vnode () >= 0);
}


locationtable::locationtable (int _max_cache)
  : size_cachedlocs (0),
    max_cachedlocs (_max_cache), 
    nnodessum (0),
    nnodes (0),
    nvnodes (0)
{
}

size_t
locationtable::size ()
{
  return locs.size ();
}

size_t
locationtable::usablenodes ()
{
  locwrap *cur = locs.first ();
  size_t good = 0;
  while (cur) {
    if (cur->good ())
      good++;
    cur = locs.next (cur);
  }
  
  return good;
}

u_long
locationtable::estimate_nodes ()
{
  size_t good = usablenodes ();
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
locationtable::realinsert (ref<location> l)
{
  if (size_cachedlocs >= max_cachedlocs) {
    delete_cachedlocs ();
  }
  loctrace << "insert " << l->id () << " " << l->address ()
	   << " " << l->vnode () << "\n";
  locwrap *lw = locs[l->id ()];
  if (lw) {
    if (lw->type_ & LOC_REGULAR) {
      warnx << "locationtable::realinsert: duplicate insertion of "
	    << l->id () << "\n";
      cachedlocs.remove (lw);
      cachedlocs.insert_tail (lw);
      return;
    }
    lw->type_ |= LOC_REGULAR;
    lw->loc_ = l;
  } else {
    lw = New locwrap (l, LOC_REGULAR);
    locs.insert (lw);
    loclist.insert (lw);
  }

  cachedlocs.insert_tail (lw);
  size_cachedlocs++;
  return;
}

ptr<location>
locationtable::insert (const chord_node &n)
{
  vec<float> coords;
  for (unsigned int i = 0; i < n.coords.size (); i++)
    coords.push_back ((float)n.coords[i]);
  return insert (n.x, n.r.hostname, n.r.port, coords);
}

ptr<location>
locationtable::insert (const chordID &n, 
		       const chord_hostname &s, 
		       int p, 
		       const vec<float> &coords)
{
  ptr<location> loc = lookup (n);
  if (loc != NULL) {
    loc->set_alive (true);
    return loc;
  }
    
  net_address r;
  r.hostname = s;
  r.port = p;
  
  loc = New refcounted<location> (n, r, coords);
  if (loc->vnode () < 0)
    return NULL;
  realinsert (loc);
  return loc;
}

void
locationtable::delete_cachedlocs (void)
{
  if (!size_cachedlocs)
    return;

  // First, try to find a bad node
  locwrap *lw = cachedlocs.first;
  while (lw && lw->good ()) {
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
    // Continue to skip over those that are directly pinned
    if ((prev (lw) && prev (lw)->type_ & LOC_PINSUCC) ||
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

  // XXX only MTF (okay, it's really "MTT") if the node is alive?
  cachedlocs.remove (l);
  cachedlocs.insert_tail (l);
  return l->loc_;
}

bool
locationtable::lookup_anyloc (const chordID &n, chordID *r)
{
  for (locwrap *l = locs.first (); l != NULL; l = locs.next (l)) {
    if (!l->good ()) continue;
    if (l->loc_->id () != n) {
      *r = l->loc_->id ();
      return true;
    }
  }
  return false;
}

ptr<location>
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

#if 0
  chordID n = l->loc_->id ();

  // Brute force check to make sure we have the right answer.
  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
    if ((l->type_ & LOC_REGULAR) == 0) continue;
    if (!l->loc_->alive) continue;
    if (l->loc_->vnode < 0) continue;
    if ((x == nbar) || between (x, nbar, l->loc_->n)) nbar = l->loc_->n;
  }
  if (n != nbar) {
    warnx << "Searching for " << x << "\n";
    loclist.traverse (wrap (this, &locationtable::printloc));
    panic << "locationtable::closestsuccloc " << nbar << " vs " << n << "\n";
  }
  // warnx << "closestsuccloc of " << x << " is " << n << "\n";
#endif /* 0 */

  return l->loc_;
}

ptr<location>
locationtable::closestpredloc (const chordID &x, vec<chordID> failed) 
{
  locwrap *l = locs[x];
  if (l) {
    l = prev (l);
  } else {
    l = loclist.closestpred (x);
  }
  while (l && (!l->good () || (l->loc_ && in_vector (failed, l->loc_->id ()))))
    l = prev (l);
  
#if 0
  chordID n = l->loc_->id ();

  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
    if ((l->type_ & LOC_REGULAR) == 0) continue;
    if (!l->loc_->alive) continue;
    if (l->loc_->vnode < 0) continue;
    if ((x == nbar) || between (nbar, x, l->loc_->n)) nbar = l->loc_->n;
  }
  if (n != nbar) {
    warnx << "Searching for " << x << "\n";
    loclist.rtraverse (wrap (this, &locationtable::printloc));
    panic << "locationtable::closestpredloc " << nbar << " vs " << n << "\n";
  }
#endif /* 0 */  
  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return l->loc_;
}

ptr<location>
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
  
  // warnx << "findpredloc of " << x << " is " << n << "\n";
  return l->loc_;
}

bool
locationtable::cached (const chordID &x)
{
  locwrap *l = locs[x];
  return (l && l->type_ & LOC_REGULAR);
}

bool
locationtable::remove (locwrap *lw)
{
  if (!lw)
    return false;
  
  locs.remove (lw);
  loclist.remove (lw->n_);
  cachedlocs.remove (lw);
  size_cachedlocs--;

  if (lw->type_ & LOC_REGULAR)
    loctrace << "delete " << lw->loc_->id () << " "
	     << lw->loc_->address () << "\n";
  delete lw;
  return true;
}
  
ptr<location>
locationtable::first_loc ()
{
  locwrap *f = cachedlocs.first;
  while (f && !f->loc_) {
    warn << "spinning in first_loc\n";
    f = cachedlocs.next (f);
  }
  if (!f) return NULL;
  return f->loc_;
}

ptr<location>
locationtable::next_loc (chordID n)
{
  locwrap *f = locs[n];
  assert (f);
  locwrap *nn = cachedlocs.next (f);
  while (nn && !nn->loc_) {
    warn << "spinning in next_loc\n";
    nn = cachedlocs.next (nn);
  }
  if (nn)
    return nn->loc_;
  else
    return NULL;
}
