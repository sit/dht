/*
 * Copyright (c) 2003-2005 Jeremy Stribling
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

#include "tapestryobserver.h"
#include "p2psim/node.h"
#include "p2psim/args.h"
#include "p2psim/network.h"
#include "events/p2pevent.h"
#include <iostream>
#include <list>
#include <algorithm>
#include <stdio.h>

#include "protocols/tapestry.h"

using namespace std;

TapestryObserver* TapestryObserver::_instance = 0;

TapestryObserver*
TapestryObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New TapestryObserver(a);
  return _instance;
}


TapestryObserver::TapestryObserver(Args *a) : _type( "Tapestry" )
{

  _oracle_num = a->nget( "oracle", 0, 10 );
  lid.clear();

  const set<Node*> *l = Network::Instance()->getallnodes();
  for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
    Tapestry *t = (Tapestry*) *pos;
    t->registerObserver(this);
  }
  _stabilized = false;
  lid.clear();
}

TapestryObserver::~TapestryObserver()
{
}

void
TapestryObserver::kick(Observed *o, ObserverInfo *oi )
{


  if( !_stabilized ) {

    DEBUG(1) << "TapestryObserver executing" << endl;
    const set<Node*> *l = Network::Instance()->getallnodes();
    set<Node*>::iterator pos;
    
    //i only want to sort it once after all nodes have joined! 
    Tapestry *c = 0;
    if (!lid.empty()) {
      lid.clear();
      for (pos = l->begin(); pos != l->end(); ++pos) {
	c = (Tapestry *)(*pos);
	assert(c);
	// only care about live nodes
	if( c->alive() ) {
	  lid.push_back(c->id ());
	}
      }
      
      sort(lid.begin(), lid.end());
    }
    
    for (pos = l->begin(); pos != l->end(); ++pos) {
      c = (Tapestry *)(*pos);
      assert(c);
      if (c->alive() && !c->stabilized(lid)) {
	DEBUG(1) << now() << " NOT STABILIZED" << endl;
	return;
      }
      
    }
  
    _stabilized = true;
    DEBUG(0) << now() << " STABILIZED" << endl;
    return;
  }

  if( !oi )
    return;

  char *event = (char *) oi;
  assert( event );
  string event_s(event);

  if( _oracle_num > 0 ) {
    Tapestry *n = (Tapestry *) o;
    assert( n );

    set<Node*>::iterator pos;
    Tapestry *c = 0;
    if( event_s == "join" ) {
      n->initstate();
    }

    const set<Node*> *l = Network::Instance()->getallnodes();
    for (pos = l->begin(); pos != l->end(); ++pos) {
      c = (Tapestry *)(*pos);
      assert(c);
      // only care about live nodes
      if( c->alive() && c->ip() != n->ip() ) {
        if( event_s == "crash" ) {
          c->oracle_node_died( n->ip(), n->id(), l );
        } else if( event_s == "join" ) {
          c->oracle_node_joined(n);
        } else {
          cout << event << " die!" << endl;
          assert( false ); // unknown event type
        }
      }
    }
  }
}
