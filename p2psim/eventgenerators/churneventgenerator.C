/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu)
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

#include "churneventgenerator.h"
#include "p2psim/eventqueue.h"
#include "p2psim/network.h"
#include "events/p2pevent.h"
#include "events/simevent.h"
#include <math.h>
#include <time.h>
#include <list>
#include <stdlib.h>
using namespace std;

ChurnEventGenerator::ChurnEventGenerator(Args *args)
{
  if( (*args)["wkn"] == "" ) {
    _wkn_string = "1";
    _wkn = 1;
  } else {
    _wkn_string = (*args)["wkn"];
    _wkn = args->nget<IPAddress>("wkn", 1, 10);
  }

  _proto = (*args)["proto"];
  assert( _proto != "" );
  _lifemean = args->nget( "lifemean", 100000, 10 );
  _deathmean = args->nget( "deathmean", _lifemean, 10 );
  _lookupmean = args->nget( "lookupmean", 10000, 10 );

  if( (*args)["exittime"] == "" ) {
    _exittime_string = "200000";
    _exittime = 200000;
  } else {
    _exittime_string = (*args)["exittime"];
    _exittime = args->nget( "exittime", 200000, 10 );
  }

  _seed = args->nget( "seed", 0, 10 );

  if( _seed ) {
    srand( _seed );
  } else {
    srand( time(NULL) );
  }

  EventQueue::Instance()->registerObserver(this);
}

void
ChurnEventGenerator::run()
{
  // first register the exit event
  vector<string> simargs;
  simargs.push_back( _exittime_string );
  simargs.push_back( "exit" );
  SimEvent *se = New SimEvent( &simargs );
  add_event( se );

  // start all nodes at a random time between 1 and n (except the wkn, who
  // starts at 1)
  list<IPAddress> l = Network::Instance()->getallips();
  list<IPAddress>::iterator pos;
  IPAddress ip = 0;
  for( pos = l.begin(); pos != l.end(); ++pos ) {
    ip = (IPAddress)(*pos);

    Args *a = new Args();
    (*a)["wellknown"] = _wkn_string;
    uint jointime;
    if( ip == _wkn ) {
      jointime = 1;
    } else {
      // add one to the mod factor because typical 2^n network sizes
      // make really bad mod factors
      jointime = (rand()%(l.size()+1)) + 1;
    }
    if( now() + jointime < _exittime ) {
      P2PEvent *e = New P2PEvent(now() + jointime, _proto, ip, "join", a);
      add_event(e);
    }

    // also schedule their first lookup
    a = new Args();
    Time tolookup = next_exponential( _lookupmean );
    (*a)["key"] = get_lookup_key();
    if( now() + jointime + tolookup < _exittime ) {
      P2PEvent *e = New P2PEvent(now() + jointime + tolookup, _proto, 
				 ip, "lookup", a);
      add_event(e);
    }

  }

  EventQueue::Instance()->go();
}


void
ChurnEventGenerator::kick(Observed *o, ObserverInfo *oi)
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

  Args *a = new Args();
  IPAddress ip = p2p_observed->node->ip();
  if( p2p_observed->type == "join" ) {

    // the wellknown can't crash (***TODO: fix this?***)
    if( ip != _wkn ) {

      // pick a time for this node to die
      Time todie = next_exponential( _lifemean );
      if( now() + todie < _exittime ) {
	P2PEvent *e = New P2PEvent(now() + todie, _proto, ip, "crash", a);
	add_event(e);
      }

    }

  } else if( p2p_observed->type == "crash" ) {
    // pick a time for the node to rejoin
    Time tojoin = next_exponential( _deathmean );
    (*a)["wellknown"] = _wkn_string;
    if( now() + tojoin < _exittime ) {
      P2PEvent *e = New P2PEvent(now() + tojoin, _proto, ip, "join", a);
      add_event(e);
    }

  } else if( p2p_observed->type == "lookup" ) {
    // pick a time for the next lookup
    Time tolookup = next_exponential( _lookupmean );
    (*a)["key"] = get_lookup_key();
    if( now() + tolookup < _exittime ) {
      P2PEvent *e = New P2PEvent(now() + tolookup, _proto, ip, "lookup", a);
      add_event(e);
    }

  }

}

Time
ChurnEventGenerator::next_exponential( uint mean )
{

  assert( mean > 0 );

  double x = ( (double)rand() / (double)(RAND_MAX) );
  uint rt = (uint) ((-(mean*1.0))*log( 1 - x ));
  return (Time) rt;

}

string
ChurnEventGenerator::get_lookup_key()
{
  char buffer[ sizeof(int)*4+1 ];  // two longs concatted
  assert( buffer );
  sprintf( buffer, "%.8X", rand() );
  sprintf( buffer + sizeof(int)*2, "%.8X", rand() );
  return buffer;
}
