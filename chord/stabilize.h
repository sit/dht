/*
 *
 * Copyright (C) 2002 Emil Sit (sit@lcs.mit.edu)
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

#ifndef _STABILIZE_H_
#define _STABILIZE_H_

#include "chord_prot.h"
// only needed so that we can print out myID

class location;

// Parent class for things that need stabilization.
class stabilizable {
 public:
  virtual ~stabilizable () { }
  // called to initiate next round of continuous stabilization
  virtual void do_continuous () { return; }
  // called to initiate next round of backoff stabilization
  virtual void do_backoff () { return; }
  // indicator of whether or not still in progress. however,
  // do_continuous and do_backoff should not break if called
  // when stabilizing returns true.
  virtual bool continuous_stabilizing () { return false; }
  virtual bool backoff_stabilizing () { return false; }
  // indicator that we believe that we are stable.
  // You must implement this function!
  virtual bool isstable () = 0;

  virtual void fill_nodelistres (chord_nodelistres *res) = 0;
  virtual void fill_nodelistresext (chord_nodelistextres *res) = 0;
};

// Class to manage stabilization timers, etc.
class stabilize_manager {
  // All time values in milliseconds
  static const u_int32_t stabilize_decrease_timer;
  static const float stabilize_slowdown_factor;

  static const u_int32_t cts_timer_init;
  static const u_int32_t cts_timer_base;
  static const u_int32_t cts_timer_max;

  static const u_int32_t bo_timer_init;
  static const u_int32_t bo_timer_base;
  static const u_int32_t bo_timer_max;

  chordID myID;
  
  bool stable;

  vec<ref<stabilizable> > clients;

  timecb_t *stabilize_continuous_tmo;
  timecb_t *stabilize_backoff_tmo;
  u_int32_t continuous_timer;
  u_int32_t backoff_timer;

 public:
  stabilize_manager (chordID _myID);
  ~stabilize_manager ();

  bool isstable (void);

  u_int32_t cts_timer (void) { return continuous_timer; }
  u_int32_t bo_timer (void) { return backoff_timer; }
  void start (void);
  void stop (void);

  void register_client (ref<stabilizable> c) { clients.push_back (c); }
  void stabilize_backoff (u_int32_t t);
  void stabilize_continuous (u_int32_t t);

  // operations in progress? ask our clients please.
  bool continuous_stabilizing ();
  bool backoff_stabilizing ();
};

#endif /* _STABILIZE_H_ */
