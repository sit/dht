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

#include <assert.h>
#include <async.h>
#include <misc_utils.h>
#include "stabilize.h"

// The code in this file deals with stabilization: a continuous
// "process" that updates the various structures (finger table,
// successor list, etc.)  maintained by chord.  Every so many
// milliseconds two different functions are run:
// 1. stabilize_continuous 2. stabilize_backoff
//
// Stabilize_continuous should be used for data structures that need to
// be checked frequently (e.g., successor) so that they can be
// repaired quickly.  Stabilize_continous runs every cts_timer_base
// milliseconds.  However, it will slows down exponentially if it
// doesn't receive a response within cts_timer_base milliseconds
// (i.e., before the next iteration starts).  Every iteration it will
// try to speed up additively with stabilize_decrease_timer milliseconds.
// It will never be slower than cts_timer_max.
//
// Stabilize_backoff is used to check data structures for which it is
// OK that they are out of date somewhat.  Stabilize_backoff runs
// every bo_timer_init milliseconds until the ring is stable.  Then,
// it slows than by a factor stabilize_slowdown_factor each iteration,
// but never slower than bo_timer_max seconds; we want to make
// sure that tables are checked with some minimum frequency.  When the
// ring comes unstable, stabilize_backoff speeds up each iteration
// with stabilize_decrease_timer until the ring is stable again; we
// want to make sure that repairs happen with some urgency.  Like
// stabilize_continuous, stabilize_backoff will slow down if it
// doesn't receive a response before the next iteration starts.

// All time values in milliseconds
const u_int32_t stabilize_manager::stabilize_decrease_timer = 1000;
const float     stabilize_manager::stabilize_slowdown_factor = 1.2;

// Base is target value; will slow down from init to base value.
const u_int32_t stabilize_manager::cts_timer_init = 1 * 1000;
const u_int32_t stabilize_manager::cts_timer_base = 20 * 1000;
const u_int32_t stabilize_manager::cts_timer_max  = 2 * cts_timer_base;

// Note, finger_table stabilization is implemented one-finger per
// backoff call.  We really want to stabilize all fingers every
// bo_timer_base ms but to do that we have to guess how many fingers
// there are.  8 seconds should be approximately right for 400 nodes
// stabilizing l(400)/l(2) fingers in one minute.
const u_int32_t stabilize_manager::bo_timer_init  = 1 * 1000;
const u_int32_t stabilize_manager::bo_timer_base  = 8 * 1000;
const u_int32_t stabilize_manager::bo_timer_max   = 2 * bo_timer_base;

stabilize_manager::stabilize_manager (chordID _myID) :
  myID (_myID),
  stable (false),
  stabilize_continuous_tmo (NULL),
  stabilize_backoff_tmo (NULL)
{
}

stabilize_manager::~stabilize_manager (void)
{
  stop ();
}

void
stabilize_manager::start (void)
{
  if (!stabilize_continuous_tmo)
    stabilize_continuous (cts_timer_init);
  if (!stabilize_backoff_tmo)
    stabilize_backoff (bo_timer_init);
}

void
stabilize_manager::stop ()
{
  if (stabilize_continuous_tmo) {
    warnx << "stop " << myID << " switch off cont stabilize timer\n";
    timecb_remove (stabilize_continuous_tmo);
    stabilize_continuous_tmo = NULL;
  }
  if (stabilize_backoff_tmo) {
    warnx << "stop " << myID << " switch off backoff stabilize timer\n";
    timecb_remove (stabilize_backoff_tmo);
    stabilize_backoff_tmo = NULL;
  }
}


bool
stabilize_manager::isstable (void) 
{
  bool ok = true;
  for (unsigned int i = 0; i < clients.size (); i++) {
    ref<stabilizable> c = clients[i];
    ok = ok && c->isstable ();
    // warnx << myID << ": isstable " << i << " is " << ok << "\n";
    if (!ok)
      return false;
  }
  return ok;
}
					
void
stabilize_manager::stabilize_continuous (u_int32_t t)
{
  stabilize_continuous_tmo = NULL;
  if (continuous_stabilizing ()) { // stabilizing too fast
    t = 2 * t;
    warnx << gettime () << " " << myID
	  <<" stabilize_continuous: slow down " << t << "\n";
  } else {
    // Try to keep t around cts_timer_base
    if (t >= cts_timer_base)   t -= stabilize_decrease_timer;
    if (t <  cts_timer_base/2) t  = (int)(stabilize_slowdown_factor * t);

    for (unsigned int i = 0; i < clients.size (); i++)
      clients[i]->do_continuous ();
  }

  if (t > cts_timer_max)
    t = cts_timer_max;

  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  continuous_timer = t;
  stabilize_continuous_tmo =
    delaycb (sec, nsec, wrap (this, 
			      &stabilize_manager::stabilize_continuous, t));
}

void
stabilize_manager::stabilize_backoff (u_int32_t t)
{
  bool stablenow = isstable ();
  stabilize_backoff_tmo = NULL;
  if (!stable && stablenow) {
    stable = true;
    warnx << gettime () << " " << myID
	  << ": stabilize: stable! stabilize timer " << t << "\n";
  } else if (!stablenow) {
    stable = false;
  }
  if (backoff_stabilizing ()) {
    t *= 2;
    //    warnx << gettime () << " " << myID
    //	  <<" stabilize_backoff: slow down " << t << "\n";
  } else {
    for (unsigned int i = 0; i < clients.size (); i++)
      clients[i]->do_backoff ();

    if (stablenow && (t <= bo_timer_max))
      t = (int)(stabilize_slowdown_factor * t);
    else if (t > bo_timer_base)  // ring is unstable; speed up stabilization
      t -= stabilize_decrease_timer;
  }
  
  if (t > bo_timer_max)
    t = bo_timer_max;

  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  backoff_timer = t;
  stabilize_backoff_tmo =
    delaycb (sec, nsec, wrap (this, &stabilize_manager::stabilize_backoff, t));
}


bool
stabilize_manager::continuous_stabilizing ()
{
  // if anyone is stabilizing, we're still in progress
  bool ok = false;
  for (unsigned int i = 0; i < clients.size (); i++) {
    ok = clients[i]->continuous_stabilizing () || ok;
  }
  return ok;
}

bool
stabilize_manager::backoff_stabilizing ()
{
  // if anyone is stabilizing, we're still in progress
  bool ok = false;
  for (unsigned int i = 0; i < clients.size (); i++) {
    ok = clients[i]->backoff_stabilizing () || ok;
  }
  return ok;
}
