/*
 * Copyright (c) 2003 Jinyang Li
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

#include "chordobserver.h"
#include "p2psim/node.h"
#include "p2psim/args.h"
#include "p2psim/network.h"
#include "protocols/protocolfactory.h"

#include <iostream>
#include <set>
#include <algorithm>
#include <stdio.h>


using namespace std;

ChordObserver* ChordObserver::_instance = 0;

ChordObserver*
ChordObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New ChordObserver(a);
  return _instance;
}


ChordObserver::ChordObserver(Args *a) : _type("Chord")
{
  _instance = this;
  if (a) _initnodes = atoi((*a)["initnodes"].c_str());
  _totallivenodes = 0;

  assert(_type.find("Chord") == 0 || _type.find("Koorde") == 0);

  ids.clear();
  const set<Node*> *l = Network::Instance()->getallnodes();
  Chord::IDMap n;
  for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
    Chord *t = dynamic_cast<Chord*>(*pos);
    n.ip = t->ip();
    n.id = t->id();
    n.choices = 1;
    ids.push_back(n);
  }
  sort(ids.begin(),ids.end(),Chord::IDMap::cmp);
#ifdef CHORD_DEBUG
  for (uint i = 0; i < ids.size(); i++) 
    printf("%qx %u\n", ids[i].id, ids[i].ip);
#endif
}

vector<Chord::IDMap>
ChordObserver::get_sorted_nodes()
{
  assert(ids.size()>0);
  return ids;
}

ChordObserver::~ChordObserver()
{
}

void
ChordObserver::kick(Observed *ob, ObserverInfo *oi)
{
  
}
