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

#include "onehopobserver.h"
#include "p2psim/node.h"
#include "p2psim/args.h"
#include "p2psim/network.h"
#include "protocols/protocolfactory.h"

#include <iostream>
#include <set>
#include <algorithm>
#include <stdio.h>


using namespace std;

OneHopObserver* OneHopObserver::_instance = 0;

OneHopObserver*
OneHopObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New OneHopObserver(a);
  return _instance;
}


OneHopObserver::OneHopObserver(Args *a) : _type("OneHop")
{
  _instance = this;

  ids.clear();
  const set<Node*> *l = Network::Instance()->getallnodes();
  for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
    OneHop *t = dynamic_cast<OneHop*>(*pos);
    ids.push_back(t->idmap());
  }
  sort(ids.begin(),ids.end(),OneHop::IDMap::cmp);
}

vector<OneHop::IDMap>
OneHopObserver::get_sorted_nodes()
{
  assert(ids.size()>0);
  return ids;
}

OneHop::IDMap
OneHopObserver::get_rand_alive_node()
{
  uint r;
  while (1) {
    r = (random() % 10);
    if (Network::Instance()->alive(ids[r].ip))
      return ids[r];
  }
}

OneHopObserver::~OneHopObserver()
{
}
