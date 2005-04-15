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

#ifndef __ONEHOP_OBSERVER_H
#define __ONEHOP_OBSERVER_H

#include "p2psim/observer.h"
#include "protocols/onehop.h"

class OneHopObserver : public Observer {
public:
  OneHopObserver(Args*);
  ~OneHopObserver();
  static OneHopObserver* Instance(Args*);
  vector<OneHop::IDMap> get_sorted_nodes();
  OneHop::IDMap get_rand_alive_node();
  void addnode(OneHop::IDMap n) { 
    vector<OneHop::IDMap>::iterator p =
      upper_bound(ids.begin(),ids.end(),n,OneHop::IDMap::cmp);
    if (p->id!=n.id) {
      ids.insert(p,1,n);
    }
  }
  void delnode(OneHop::IDMap n) { 
    vector<OneHop::IDMap>::iterator p =
      find(ids.begin(),ids.end(),n);
    ids.erase(p);
  }

  virtual void kick(Observed *, ObserverInfo *) {};
private:
  static OneHopObserver *_instance;
  string _type;

  void init_state();
  vector<OneHop::IDMap> ids;
};

#endif // __CHORD_OBSERVER_H
