/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __CHORD_OBSERVER_H
#define __CHORD_OBSERVER_H

#include "p2psim/observer.h"
#include "protocols/chord.h"

class ChordObserver : public Observer {
public:
  ChordObserver(Args*);
  ~ChordObserver();
  static ChordObserver* Instance(Args*);
  virtual void kick(Observed *, ObserverInfo *);
  vector<Chord::IDMap> get_sorted_nodes();
  void addnode() { _totallivenodes++; printf("chordobserver %llu %d\n", now(), _totallivenodes);}
  void delnode() { _totallivenodes--; printf("chordobserver %llu %d\n", now(), _totallivenodes);}

private:
  static ChordObserver *_instance;
  bool _initnodes;
  string _type;

  void init_state();
  vector<Chord::IDMap> ids;
  uint _totallivenodes;
};

#endif // __CHORD_OBSERVER_H
