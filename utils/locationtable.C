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

struct locationtable_init {
  locationtable_init ();
} li;

locationtable_init::locationtable_init ()
{
  // Gross hack.
  Configurator::only ().set_int ("locationtable.maxcache",
				 (160 + 16 + 16 + 16));
}

locationtable::locwrap::locwrap (ptr<location> l) :
  loc_ (l),
  n_ (l->id ()),
  pinned_ (false)
{
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
  assert (loc_->vnode () >= 0);
  return loc_->alive ();
}


locationtable::locationtable (int _max_cache)
  : max_cachedlocs (_max_cache), 
    nnodessum (0),
    nnodes (0),
    nvnodes (0),
    pins_updated_ (false)
{
}

locationtable::~locationtable ()
{
  locwrap *lcur, *lnext;
  for (lcur = loclist.first (); lcur; lcur = lnext) {
    lnext = loclist.next (lcur);
    remove (lcur);
  }
  
  pininfo *pcur, *pnext;
  for (pcur = pinlist.first (); pcur; pcur = pnext) {
    pnext = pinlist.next (pcur);
    delete pcur;
  }
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
  loctrace << "insert " << l->id () << " " << l->address ()
	   << " " << l->vnode () << "\n";
  locwrap *lw = locs[l->id ()];
  assert (!lw);
  
  lw = New locwrap (l);
  locs.insert (lw);
  loclist.insert (lw);
  cachedlocs.insert_tail (lw);

  pins_updated_ = false;
  if (size () > max_cachedlocs)
    evict (size () - max_cachedlocs);
}

ptr<location>
locationtable::insert (const chord_node &n)
{
  vec<float> coords;
  for (unsigned int i = 0; i < n.coords.size (); i++)
    coords.push_back ((float)n.coords[i]);
  return insert (n.x, n.r.hostname, n.r.port, n.vnode_num, coords);
}

ptr<location>
locationtable::insert (const chordID &n, 
		       const chord_hostname &s, 
		       int p, int v,
		       const vec<float> &coords)
{
  ptr<location> loc = lookup (n);
  if (loc != NULL) {
    // Try to dampen node becoming alive so it only happens once
    // a minute.
    if (!loc->alive ()) {
      timespec ts;
      clock_gettime (CLOCK_REALTIME, &ts);
      if (ts.tv_sec - loc->dead_time () > 60)
	loc->set_alive (true);
      else
	loctrace << "locationtable::insert: damping " << loc->id () << "\n";
    }
    return loc;
  }
    
  net_address r;
  r.hostname = s;
  r.port = p;
  
  loc = New refcounted<location> (n, r, v, coords);
  if (loc->vnode () < 0)
    return NULL;
  realinsert (loc);
  return loc;
}

// XXX this code currently only handles a single predecessor. NOT pred lists.
void
locationtable::figure_pins (void)
{
  warnx << "locationtable::figure_pins\n";
  bool done = false;
  
  // Set this up front.
  pins_updated_ = true;

  pininfo *cpin = pinlist.first ();
  locwrap *cur = loclist.first ();

  // Clear everything initially.  Then turn stuff on later.
  while (cur) {
    cur->pinned_ = false;
    cur = loclist.next (cur);
  }
  
  if (cpin == NULL)
    return;

  // Pin the predecessors.
  // XXX I am too lame to figure out how to merge this in with
  //     the scan below, though of course, it should be possible.
  while (cpin) {
    if (cpin->pinpred_ > 0) {
      locwrap *p = loclist.closestpred (cpin->n_);
      p->pinned_ = true;
      warnx << "pinning pred " << p->n_ << "\n";
    }
    cpin = pinlist.next (cpin);
  }
  
  cpin = pinlist.first ();
  cur = loclist.first ();  
  unsigned short to_pin = 0;
  
  // Walk through both the pinlist and the loclist.
  // We always want the pin's ID to be ahead of the loclist ID
  // so that we know how far forward we'll have to go.
  
  // warnx << "cur pin = " << cpin->n_ << ", " << cpin->pinsucc_ << "\n";
  while (!done) {
    while (cpin && cpin->n_ < cur->n_) {
      // All consecutive pins [between two nodes] are effectively collapsed;
      // select the "max" pin amount from among these two locwraps.
      if (cpin->pinsucc_ > to_pin)
	to_pin = cpin->pinsucc_;
      
      cpin = pinlist.next (cpin);
      if (!cpin) break;
      // warnx << "<>step pin = " << cpin->n_ << " " << cpin->pinsucc_ << "\n";
    }
    // cpin->n_ >= cur->n_
    if (!cpin) {
      // Last pin.  Scan until its influence expires...
      while (cur && to_pin > 0) {
  	warnx << "pinning cur " << cur->n_ << "\n";
	cur->pinned_ = true;
	to_pin--;
	cur  = next (cur);
//  	warnx << "<>step cur = " << cur->n_ << "\n";
      }
      done = true;
      // ...done.
    } else {
      // Scan until the appearance of the next pin.
      while (cur && cur->n_ < cpin->n_) {
//  	warnx << "<>step cur = " << cur->n_ << "\n";
	if (to_pin > 0) {
	  warnx << "pinning cur " << cur->n_ << "\n";
	  cur->pinned_ = true;
	  to_pin--;
	}
	cur  = loclist.next (cur);
      }
      // cpin->n_ <= cur->n_
      // There's some oddity here b/c strictly speaking succ(x) := x.
      // However the n elements of the pinned successors of x does not
      // include x.  Thus if you want to pin the node itself, you have
      // to call pin(x) so that pinself is set.
      if (cur->n_ == cpin->n_) {
	if (cpin->pinself_) {
	  cur->pinned_ = true;
	  if (to_pin > 0)
	    to_pin--;
	  warnx << "pinning self " << cur->n_ << "\n";
	}
	cur  = loclist.next (cur);
      }
      // cpin->n_ <= cur->n_
    }
  }
}

// Evict n nodes. If n == 0, evict as many as you can.
void
locationtable::evict (size_t n)
{
  if (!pins_updated_)
    figure_pins ();

  pins_updated_ = false;
  bool done = false;
  
  bool all = (n == 0);
  if (all) n = size ();
    
  // Attempt to evict in least recently used order.
  bool badonly = true;
  locwrap *cur = cachedlocs.first;
  locwrap *next = NULL;
  while (!done) {
    while (cur && n > 0) {
      next = cachedlocs.next (cur);
      // Definitely skip pinned nodes.
      // If flushing all, that's the only condition.
      // If we're not flushing all, then check to see if we're looking
      // for bad nodes only.
      if (!cur->pinned_ &&
	  (all || (badonly ? !cur->good () : true)))
      {
	remove (cur);
	n--;
      }
      cur = next;
    }
    if (n == 0)
      done = true; // satisfied the user's request
    else if (all) {
      assert (cur == NULL);
      done = true; // one iteration is enough
    } else if (badonly == false)
      done = true; // two iterations is enough.
    else {
      // Restart for second time, if necessary.
      cur = cachedlocs.first;
      badonly = false;
    }
  }
  if (n > 0 && !all) 
    loctrace << "evict: failed to evict all requested; "
	     << n << " more requested.\n";
}

void
locationtable::pin (const chordID &x, short num)
{
  pins_updated_ = false;
  pininfo *p = pinlist.search (x);
  if (num < -1)
    fatal << "unsupported predecessor pin amount " << num << ".\n";
  if (p) {
    if (num == 0)
      p->pinself_ = true;
    else if (num > 0)
      p->pinsucc_ = num;
    else
      p->pinpred_ = -num;
  } else {
    if (num == 0)
      p = New pininfo (x, true, 0, 0);
    else if (num > 0)
      p = New pininfo (x, false, num, 0);
    else
      p = New pininfo (x, false, 0, -num);
    pinlist.insert (p);
  }
}

void
locationtable::unpin (const chordID &x)
{
  pins_updated_ = false;
  pinlist.remove (x);
  
}

void
locationtable::flush (void)
{
  evict (0);
}

ptr<location>
locationtable::lookup_or_create (const chord_node &n)
{
  ptr<location> loc = lookup (n.x);
  if (loc != NULL)
    return loc;

  loc = New refcounted<location> (n);
  if (loc->vnode () < 0)
    return NULL;
  return loc;
}

ptr<location>
locationtable::lookup (const chordID &n)
{
  locwrap *l = locs[n];
  if (!l)
    return NULL;

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
  int sz = size ();
  locwrap *l = locs[x];
  if (!l)
    l = loclist.closestsucc (x);

  // ...and now narrow it down to someone who's "good".
  while (l && !l->good () && sz-- > 0)
    l = next (l);

  assert (sz > 0); // could it be that we have no good nodes?
#if 0
  chordID n = l->loc_->id ();

  // Brute force check to make sure we have the right answer.
  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
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
  int sz = size ();
  locwrap *l = locs[x];
  if (l) {
    l = prev (l);
  } else {
    l = loclist.closestpred (x);
  }
  while (l && (!l->good () || in_vector (failed, l->loc_->id ()))
           && sz-- > 0)
    l = prev (l);

  assert (sz > 0);
#if 0
  chordID n = l->loc_->id ();

  chordID nbar = x;
  for (l = locs.first (); l; l = locs.next (l)) {
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
  return (l != NULL);
}

bool
locationtable::remove (locwrap *lw)
{
  if (!lw)
    return false;
  
  loctrace << "delete " << lw->loc_->id () << " "
	   << lw->loc_->address () << "\n";
  
  pins_updated_ = false;
  
  locs.remove (lw);
  loclist.remove (lw->n_);
  cachedlocs.remove (lw);

  delete lw;
  return true;
}
  
ptr<location>
locationtable::first_loc ()
{
  locwrap *f = loclist.first ();
  while (f && !f->loc_) {
    warn << "spinning in first_loc\n";
    f = loclist.next (f);
  }
  if (!f) return NULL;
  return f->loc_;
}

ptr<location>
locationtable::next_loc (chordID n)
{
  locwrap *f = locs[n];
  assert (f);
  locwrap *nn = loclist.next (f);
  while (nn && !nn->loc_) {
    warn << "spinning in next_loc\n";
    nn = loclist.next (nn);
  }
  if (nn)
    return nn->loc_;
  else
    return NULL;
}
