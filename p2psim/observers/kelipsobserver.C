/*
 * Copyright (c) 2003-2005 Robert Morris
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

#include "kelipsobserver.h"
#include "p2psim/node.h"
#include "p2psim/args.h"
#include "p2psim/network.h"
#include <iostream>
#include <set>
#include <algorithm>
#include <stdio.h>

#include "protocols/kelips.h"

using namespace std;

KelipsObserver* KelipsObserver::_instance = 0;

KelipsObserver*
KelipsObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New KelipsObserver(a);
  return _instance;
}


KelipsObserver::KelipsObserver(Args *a)
  : _type("Kelips")
{
  _initnodes = atoi((*a)["initnodes"].c_str());

  const set<Node*> *l = Network::Instance()->getallnodes();
  for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
    Kelips *t = (Kelips*) *pos;
    t->registerObserver(this);
  }
}

KelipsObserver::~KelipsObserver()
{
}

void
KelipsObserver::init_state()
{
  const set<Node*> *l = Network::Instance()->getallnodes();
  for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
    Kelips *k = dynamic_cast<Kelips*>(*pos);
    k->init_state(l);
  }
}

void
KelipsObserver::kick(Observed *ob, ObserverInfo *oi)
{
  if(_initnodes){
    _initnodes = false;
    init_state();
  }
}
