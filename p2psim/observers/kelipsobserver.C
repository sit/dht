/*
 * Copyright (c) 2003 Robert Morris
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
#include "p2psim/protocol.h"
#include "p2psim/args.h"
#include "p2psim/network.h"
#include <iostream>
#include <list>
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


KelipsObserver::KelipsObserver(Args *a) : Oldobserver(a)
{
  _reschedule = 0;
  _reschedule = atoi((*a)["reschedule"].c_str());
  _num_nodes = atoi((*a)["numnodes"].c_str());
  assert(_num_nodes > 0);

  _init_num = atoi((*a)["initnodes"].c_str());
  lid.clear();
}

KelipsObserver::~KelipsObserver()
{
}

void
KelipsObserver::init_state()
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  DEBUG(1) << "KelipsObserver::init_state " << now() << endl;
  for(list<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
    Kelips *t = (Kelips*) *pos;
    t->init_state(l);
  }
}

void
KelipsObserver::execute()
{

  if(_init_num) {
    init_state();
    _init_num = 0;
  }

  DEBUG(1) << "KelipsObserver executing" << endl;
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  //i only want to sort it once after all nodes have joined! 
  if (lid.size() != _num_nodes) {
    lid.clear();
    for (pos = l.begin(); pos != l.end(); ++pos) {
      Kelips *c = (Kelips *)(*pos);
      assert(c);
      // only care about live nodes
      if( c->node()->alive() ) {
	lid.push_back(c->id ());
      }
    }

    sort(lid.begin(), lid.end());
  }

  for (pos = l.begin(); pos != l.end(); ++pos) {
    Kelips *c = (Kelips *)(*pos);
    assert(c);
    if (c->node()->alive() && !c->stabilized(lid)) {
      DEBUG(1) << now() << " NOT STABILIZED" << endl;
      if (_reschedule > 0) reschedule(_reschedule);
      return;
    }

  }

  DEBUG(0) << now() << " STABILIZED" << endl;
}
