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

#ifndef _FINGERLIKE_H_
#define _FINGERLIKE_H_

#include "chord_types.h"
#include "stabilize.h"

class vnode;
class locationtable;

class fingerlike_iter {
 protected:
  vec<chordID> nodes;
  size_t next_;
 public:
  virtual void reset () { next_ = 0; }
  virtual size_t size () { return nodes.size (); }
  virtual chordID next () {
    assert (next_ < nodes.size ());
    return nodes[next_++];
  };
  virtual bool done () { return next_ >= nodes.size (); };

  fingerlike_iter () : next_ (0) {};
  virtual ~fingerlike_iter () {};
};

class fingerlike : public stabilizable {
public:
  virtual void print () = 0;
  virtual void stats () = 0;
  virtual void init (ptr<vnode> v, ptr<locationtable> locs, chordID ID) = 0;

  virtual chordID closestpred (const chordID &x) = 0;
  virtual chordID closestpred (const chordID &x, vec<chordID> fail) = 0;
  virtual chordID closestsucc (const chordID &x) = 0;

  // Snapshot the current set of fingers and allow code to iterate
  // through their chordIDs
  virtual ref<fingerlike_iter> get_iter () = 0;
};
#endif /* _FINGERLIKE_H_ */
