/*
 * Copyright (c) 2003-2005 Jinyang Li
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
  void static_simulation();
  virtual void kick(Observed *, ObserverInfo *);
  vector<Chord::IDMap> get_sorted_nodes();
  void addnode(Chord::IDMap n) { 
    if(_oracle_num) return;
    vector<Chord::IDMap>::iterator p =
      upper_bound(ids.begin(),ids.end(),n,Chord::IDMap::cmp);
    if (p->id!=n.id) {
      ids.insert(p,1,n);
    }
    _totallivenodes++; 
  }
  void delnode(Chord::IDMap n) { 
    vector<Chord::IDMap>::iterator p =
      find(ids.begin(),ids.end(),n);
    ids.erase(p);
    _totallivenodes--; 
  }

private:
  static ChordObserver *_instance;
  string _type;
  unsigned int _oracle_num;

  void init_state();
  vector<Chord::IDMap> ids;
  uint _totallivenodes;
};

#endif // __CHORD_OBSERVER_H
