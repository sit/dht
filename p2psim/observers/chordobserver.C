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
#include "p2psim/protocol.h"
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


ChordObserver::ChordObserver(Args *a)
{
  _instance = this;
  assert(a);
  _initnodes = atoi((*a)["initnodes"].c_str());

  set<string> all = ProtocolFactory::Instance()->getnodeprotocols();
  assert(all.size()==1);
  for(set<string>::iterator pos=all.begin();pos!=all.end();++pos) {
    _type = *pos;
  }
  assert(_type.find("Chord") == 0);

  ids.clear();
  set<Protocol*> l = Network::Instance()->getallprotocols(_type);
  Chord::IDMap n;
  for(set<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
    Chord *t = dynamic_cast<Chord*>(*pos);
    t->registerObserver(this);
    n.ip = t->node()->ip();
    n.id = t->id();
    n.choices = 1;
    ids.push_back(n);
  }
  sort(ids.begin(),ids.end(),Chord::IDMap::cmp);
}

vector<Chord::IDMap>
ChordObserver::get_sorted_nodes()
{
  return ids;
}

ChordObserver::~ChordObserver()
{
}

void
ChordObserver::init_state()
{
  set<Protocol*> l = Network::Instance()->getallprotocols(_type);
  for(set<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
    Chord *t = dynamic_cast<Chord*>(*pos);
    t->init_state(ids);
  }
}

void
ChordObserver::kick(Observed *ob, ObserverInfo *oi)
{
  if(_initnodes){
    _initnodes = false;
    init_state();
  }
}
