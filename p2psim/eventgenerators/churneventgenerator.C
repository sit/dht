/*
 * Copyright (c) 2003 Jeremy Stribling (strib@mit.edu)
 *                    Thomer M. Gil (thomer@csail.mit.edu)
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
 *
 *
 * See the HOWTO in eventgenerators/churneventgenerator.h .
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
#include <iostream>
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
  _lifemean = args->nget( "lifemean", 100000, 10 ); //0 means no failure
  _deathmean = args->nget( "deathmean", _lifemean, 10 ); //0 means no failure
  _lookupmean = args->nget( "lookupmean", 10000, 10 );

  if( (*args)["exittime"] == "" ) {
    _exittime_string = "200000";
    _exittime = 200000;
  } else {
    _exittime_string = (*args)["exittime"];
    _exittime = args->nget( "exittime", 200000, 10 );
  }

  _ipkeys = args->nget("ipkeys", 0, 10);

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
  set<IPAddress> tmpips = Network::Instance()->getallips();
  for(set<IPAddress>::const_iterator i = tmpips.begin(); i != tmpips.end(); ++i)
    _ips.push_back(*i);


  IPAddress ip = 0;
  for(u_int xxx = 0; xxx < _ips.size(); xxx++){
    ip = _ips[xxx];

    Args *a = New Args();
    (*a)["wellknown"] = _wkn_string;
    uint jointime;
    if( ip == _wkn ) {
      jointime = 1;
    } else {
      // add one to the mod factor because typical 2^n network sizes
      // make really bad mod factors
      jointime = (random()%(_ips.size()+1)) + 1;
    }
    if( now() + jointime < _exittime ) {
      P2PEvent *e = New P2PEvent(now() + jointime, _proto, ip, "join", a);
      add_event(e);
    }

    // also schedule their first lookup
    a = New Args();
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

  Args *a = New Args();
  IPAddress ip = p2p_observed->node->ip();
  if( p2p_observed->type == "join" ) {

    // the wellknown can't crash (***TODO: fix this?***) also if lifemean is zero, this node won't die
    if (( ip != _wkn ) && ( _lifemean > 0)) {

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

  double x = ( (double)random() / (double)(RAND_MAX) );
  uint rt = (uint) ((-(mean*1.0))*log( 1 - x ));
  return (Time) rt;

}

string
ChurnEventGenerator::get_lookup_key()
{
  if(_ipkeys){
    // for Kelips, use only keys equal to live IP addresses.
    for(int iters = 0; iters < 50; iters++){
      IPAddress ip = _ips[random() % _ips.size()];
      if(Network::Instance()->getnode(ip)->alive()){
        char buf[10];
        sprintf(buf, "%x", ip);
        return string(buf);
      }
    }
    assert(0);
  }
  
  // look up random 64-bit keys
  char buffer[20];
  // random() returns only 31 random bits.
  // so we need three to ensure all 64 bits are random.
  unsigned long long a = random();
  unsigned long long b = random();
  unsigned long long c = random();
  unsigned long long x = (a << 48) ^ (b << 24) ^ (c >> 4);
  sprintf(buffer, "%llX", x);
  return string(buffer);
}
