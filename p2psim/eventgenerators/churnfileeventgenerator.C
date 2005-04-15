/*
 * Copyright (c) 2003-2005 Jeremy Stribling (thomer@csail.mit.edu)
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

#include "churnfileeventgenerator.h"
#include "protocols/protocolfactory.h"
#include "events/eventfactory.h"
#include "events/p2pevent.h"
#include "events/simevent.h"
#include "p2psim/eventqueue.h"
#include "p2psim/network.h"
#include <fstream>
#include <iostream>
using namespace std;

ChurnFileEventGenerator::ChurnFileEventGenerator(Args *args)
  : ChurnEventGenerator(args)
{
  _name = (*args)["file"];
}

void
ChurnFileEventGenerator::run()
{
  ifstream in(_name.c_str());
  if(!in) {
    cerr << "no such file " << _name 
	 << ", did you supply the file parameter?" << endl;
    taskexitall(0);
  }

  string line;

  // first register the exit event
  vector<string> simargs;
  simargs.push_back( _exittime_string );
  simargs.push_back( "exit" );
  SimEvent *se = New SimEvent( &simargs );
  add_event( se );

  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    assert( words.size() > 1 );

    IPAddress ip = atoi(words[0].c_str());
    words.erase(words.begin());

    // in the beginning, is this node alive or dead?
    string state = words[0];
    words.erase(words.begin());

    bool did_lookup = false;
    // if it starts alive, start it right away
    if( state == "alive" || ip == _wkn ) {
      Args *a = New Args();
      (*a)["wellknown"] = _wkn_string;
      P2PEvent *e = New P2PEvent(1, ip, "join", a);
      add_event(e);

      // also, start its first lookup
      a = New Args();
      Time tolookup = next_exponential( _lookupmean );
      if( 1 + tolookup < _exittime ) {
	(*a)["key"] = get_lookup_key();
	P2PEvent *e = New P2PEvent(1 + tolookup, ip, "lookup", a);
	add_event(e);
	did_lookup = true;
      } else {
	delete a;
      }
    } else {
      // any node that starts out dead must really start out dead --
      // that is, it's alive must be false
      Node *n = Network::Instance()->getnode(ip);
      n->set_alive(false);

    }

    while( !words.empty() && ip != _wkn ) {
      
      Args *a = New Args();
      Time time = atol( words[0].c_str() );
      words.erase(words.begin());

      if( state == "alive" ) {

	// make a crash event
	if( time < _exittime ) {
	  P2PEvent *e = New P2PEvent(time, ip, "crash", a);
	  add_event(e);
	  state = "dead";
	} else {
	  delete a;
	}

      } else if( state == "dead" ) {

	// make a join event
	if( time < _exittime ) {
	  (*a)["wellknown"] = _wkn_string;
	  P2PEvent *e = New P2PEvent(time, ip, "join", a);
	  add_event(e);
	  state = "alive";
	} else {
	  delete a;
	  a = NULL;
	}

	if( !did_lookup ) {
	  a = New Args();
	  Time tolookup = next_exponential( _lookupmean );
	  if( time + tolookup < _exittime ) {
	    (*a)["key"] = get_lookup_key();
	    P2PEvent *e = New P2PEvent(time + tolookup, ip, "lookup", a);
	    add_event(e);
	    did_lookup = true;
	  } else {
	    delete a;
	  }
	}

      } else {
	// Unrecognized initial state
	assert(0);
      }

    }
  }

  EventQueue::Instance()->go();
}

void
ChurnFileEventGenerator::kick(Observed *o, ObserverInfo* oi)
{

  assert( oi );

  Event *ev = (Event *) oi;
  assert( ev );

  if( ev->name() != "P2PEvent" ) {
    // not a p2p event, so we don't care
    return;
  }
  P2PEvent *p2p_observed = (P2PEvent *) ev;
  assert( p2p_observed );

  Args *a = New Args();
  IPAddress ip = p2p_observed->node->first_ip();

  if( p2p_observed->type == "lookup" ) {
    // pick a time for the next lookup
    Time tolookup = next_exponential( _lookupmean );
    (*a)["key"] = get_lookup_key();
    if( now() + tolookup < _exittime ) {
      P2PEvent *e = New P2PEvent(now() + tolookup, ip, "lookup", a);
      add_event(e);
    } else {
      delete a;
    }
    
  } else {
    delete a;
  }

}
