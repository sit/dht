/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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
 */

/* $Id: tapestry.C,v 1.2 2003/10/08 07:10:40 thomer Exp $ */
#include "tapestry.h"
#include "p2psim/network.h"
#include <stdio.h>
#include <math.h>
#include <iostream>
#include <map>
using namespace std;

Tapestry::Tapestry(Node *n)
  : DHTProtocol(n),
    _base(16),
    _bits_per_digit((uint) (log10(((double) _base))/log10((double) 2))),
    _digits_per_id((uint) 8*sizeof(GUID)/_bits_per_digit)
{
  joined = false;
  _my_id = get_id_from_ip(ip());
  TapDEBUG(2) << "Constructing" << endl;
  _rt = New RoutingTable(this);
  _waiting_for_join = New ConditionVar();
}

Tapestry::~Tapestry()
{
  TapDEBUG(2) << "Destructing" << endl;
  delete _rt;
  delete _waiting_for_join;
}

void
Tapestry::lookup(Args *args) 
{

  GUID key = args->nget<GUID>("key");

  TapDEBUG(2) << "Tapestry Lookup for key " << key << endl;

  lookup_args la;
  la.key = key;
  
  lookup_return lr;

  handle_lookup( &la, &lr );

  TapDEBUG(1) << "Lookup complete for key " << print_guid(key) << ": ip " <<
    lr.owner_ip << ", id " << print_guid(lr.owner_id) << ", hops " <<
    lr.hopcount << endl;

}

void 
Tapestry::handle_lookup(lookup_args *args, lookup_return *ret)
{

  // find the next hop for the key.  if it's me, i'm done
  IPAddress next = next_hop( args->key );
  if( next == ip() ) {
    ret->owner_ip = ip();
    ret->owner_id = id();
    // this will be incremented at each hop backwards
    ret->hopcount = 0;
  } else {
    // it's not me, so forward the query
    doRPC( next, &Tapestry::handle_lookup, args, ret );
    ret->hopcount++;
  }
 
}

void
Tapestry::insert(Args *args) 
{
  TapDEBUG(2) << "Tapestry Insert" << endl;
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Tapestry::join(Args *args)
{
  TapDEBUG(2) << "Tapestry join" << endl;

  IPAddress wellknown_ip = args->nget<IPAddress>("wellknown");
  TapDEBUG(3) << ip() << " Wellknown: " << wellknown_ip << endl;

  // might already be joined if init_state was used
  if( joined ) {
    return;
  }

  // if we're the well known node, we're done
  if( ip() == wellknown_ip ) {
    joined = true;
    _waiting_for_join->notifyAll();
    return;
  }

  // contact the well known machine, and have it start routing to the surrogate
  join_args ja;
  ja.ip = ip();
  ja.id = id();
  join_return jr;

  doRPC( wellknown_ip, &Tapestry::handle_join, &ja, &jr );

  // now that the multicast is over, it's time for nearest neighbor
  // ping everyone on the initlist
  int init_level = guid_compare( id(), jr.surr_id ); 
  vector<NodeInfo *> seeds;
  TapDEBUG(2) << "init level of " << init_level << endl;
  for( int i = init_level; i >= 0; i-- ) {
    
    // go through each member of the init list, add them to your routing 
    // table, and ask them for their forward and backward pointers 

    // to do this, set off two sets of asynchRPCs at once, one to ping all
    // the nodes and find out their distances, and another to get the nearest
    // neighbors.  these can't easily be combined since the nearest neighbor
    // call does a ping itself (so measured rtt would be double).
    RPCSet ping_rpcset;
    map<unsigned, ping_callinfo*> ping_resultmap;
    Time before_ping = now();
    multi_add_to_rt_start( &ping_rpcset, &ping_resultmap, &initlist );

    RPCSet nn_rpcset;
    map<unsigned, nn_callinfo*> nn_resultmap;
    unsigned int num_nncalls = 0;    
    for( uint j = 0; j < initlist.size(); j++ ) {
      NodeInfo ni = *(initlist[j]);

      // also do an async nearest neighbor call
      nn_args *na = New nn_args();
      na->ip = ip();
      na->id = id();
      na->alpha = i;
      nn_return *nr = New nn_return();
      unsigned rpc = asyncRPC( ni._addr, &Tapestry::handle_nn, na, nr );
      assert(rpc);
      nn_resultmap[rpc] = New nn_callinfo(ni._addr, na, nr);
      nn_rpcset.insert(rpc);
      num_nncalls++;

    }

    multi_add_to_rt_end( &ping_rpcset, &ping_resultmap, before_ping );

    for( uint j = 0; j < num_nncalls; j++ ) {

      bool ok;
      unsigned donerpc = rcvRPC( &nn_rpcset, ok );
      nn_callinfo *ncall = nn_resultmap[donerpc];
      nn_return nnr = *(ncall->nr);
      TapDEBUG(2) << ip() << " done with nn with " << ncall->ip << endl;

      for( uint k = 0; k < nnr.nodelist.size(); k++ ) {
	// make sure this one isn't on there yet
	// TODO: make this more efficient.  Maybe use a hash set or something
	NodeInfo *currseed = &(nnr.nodelist[k]);

	// don't add ourselves, duh
	if( currseed->_id == id() ) {
	  continue;
	}
	bool add = true;
	for( uint l = 0; l < seeds.size(); l++ ) {
	  if( *currseed == *(seeds[l]) ) {
	    add = false;
	    break;
	  }
	}
	if( add ) {
	  NodeInfo *newseed = New NodeInfo( currseed->_addr, currseed->_id );
	  seeds.push_back( newseed );
	  TapDEBUG(3) << " has a seed of " << newseed->_addr << " and " << 
	    print_guid( newseed->_id ) << endl;
	}
      }
      TapDEBUG(3) << "done with adding seeds with " << ncall->ip << endl;
      // TODO: for some reason the compiler gets mad if I try to delete nr
      // in the destructor
      delete ncall->nr;
      ncall->nr = NULL;
      delete ncall;
    }

    // now that we have all the seeds we want from this one, 
    // see if they are in the closest k
    NodeInfo * closestk[_k];
    // initialize to NULL
    for( uint k = 0; k < _k; k++ ) {
      closestk[k] = NULL;
    }
    bool closest_full = false;

    // add these guys to routing table
    multi_add_to_rt( &seeds );

    for( uint j = 0; j < seeds.size(); j++ ) {
      NodeInfo *currseed = seeds[j];
      // add them all to the routing table (this gets us the ping time for free
      TapDEBUG(3) << "about to get distance for " << currseed->_addr << endl;
      //add_to_rt( currseed->_addr, currseed->_id );
      TapDEBUG(3) << "added to rt for " << currseed->_addr << endl;
      currseed->_distance = ping( currseed->_addr, currseed->_id );
      TapDEBUG(3) << "got distance for " << currseed->_addr << endl;

      // is there anyone on the list farther than you?  if so, replace
      bool added = false;
      for( uint k = 0; k < _k; k++ ) {
	NodeInfo * currclose = closestk[k];
	if( currclose == NULL || 
	    (closest_full && currclose->_distance > currseed->_distance ) ) {
	  if( currclose != NULL ) {
	    delete currclose;
	  }
	  closestk[k] = currseed;
	  added = true;
	  if( k == _k - 1 ) {
	    closest_full = true;
	  }
	  //TapDEBUG(2) << "close is " << currseed->_addr << endl;
	  break;
	}
      }
      if( !added ) {
	// we're throwing this person away, so delete the memory
	delete currseed;
      }

    }

    TapDEBUG(2) << "gathered closest for level " << i << endl;

    // these k are the next initlist
    for( uint l = 0; l < initlist.size(); l++ ) {
      delete initlist[l];
    }
    initlist.clear();
    seeds.clear();
    for( uint k = 0; k < _k; k++ ) {
      NodeInfo *currclose = closestk[k];
      if( currclose != NULL ) {
	TapDEBUG(2) << "close is " << currclose->_addr << endl;
	initlist.push_back( currclose );
      }
    }
  }

  for( uint l = 0; l < initlist.size(); l++ ) {
    delete initlist[l];
  }

  joined = true;
  _waiting_for_join->notifyAll();
  TapDEBUG(2) << "join done" << endl;
  TapDEBUG(2) << *_rt << endl;

}

void
Tapestry::handle_join(join_args *args, join_return *ret)
{
  TapDEBUG(2) << "got a join message from " << args->ip << "/" << 
    print_guid(args->id) << endl;

  // if our join has not yet finished, we must delay the handling of this
  // person's join.
  while( !joined ) {
    _waiting_for_join->wait();
  }

  // route toward the root
  IPAddress next = next_hop( args->id );
  if( next == ip() ) {
    // we are the surrogate root for this node, start the integration
    
    TapDEBUG(2) << "is the surrogate root for " << args->ip << endl;

    // start by sending the new node all of the nodes in your table
    // up to the digit you share
    int alpha = guid_compare( id(), args->id );
    vector<NodeInfo *> thisrow;

    bool stop = true;
    for( int i = 0; i <= alpha; i++ ) {

      for( uint j = 0; j < _base; j++ ) {
	NodeInfo *ni = _rt->read( i, j );
	if( ni != NULL ) {
	  // keep going if there is someone else on this level
	  if( ni->_addr != ip() ) {
	    stop = false;
	  }
	  thisrow.push_back( ni );
	}
      }

      if( stop ) break;
      stop = true;

    }

    nodelist_args na;
    na.nodelist = thisrow;
    nodelist_return nr;
    doRPC( args->ip, &Tapestry::handle_nodelist, &na, &nr );

    // free the nodelist
    for( uint i = 0; i < na.nodelist.size(); i++ ) {
      delete na.nodelist[i];
    }

    // start the multicast
    mc_args mca;
    mca.new_ip = args->ip;
    mca.new_id = args->id;
    mca.alpha = alpha;
    mca.from_lock = false;
    mc_return mcr;

    // make the watchlist
    vector<bool *> wl;
    for( int i = 0; i < alpha+1; i++ ) {
      bool *level = new bool[_base];
      wl.push_back(level);
      for( uint j = 0; j < _base; j++ ) {
	wl[i][j] = false;
      }
    }
    mca.watchlist = wl;

    handle_mc( &mca, &mcr );

    // free the bools!
    for( int i = 0; i < alpha+1; i++ ) {
	bool *level = wl[i];
	delete level;
    }

    ret->surr_id = id();
    
  } else {
    // not the surrogate
    // recursive routing yo
    doRPC( next, &Tapestry::handle_join, args, ret );
  }

}

void 
Tapestry::handle_nodelist(nodelist_args *args, nodelist_return *ret)
{

  TapDEBUG(3) << "handling a nodelist message" << endl;

  // add each to your routing table
  multi_add_to_rt( &(args->nodelist) );
  /*
  for( uint i = 0; i < args->nodelist.size(); i++ ) {
    NodeInfo * curr_node = args->nodelist[i];
    add_to_rt( curr_node->_addr, curr_node->_id );
  }
  */

}

void
Tapestry::handle_mc(mc_args *args, mc_return *ret)
{

  TapDEBUG(2) << "got multicast message for " << args->new_ip << endl;

  // lock this node
  _rt->set_lock( args->new_ip, args->new_id );

  // make sure that it knows about you as well
  mcnotify_args mca;
  mca.ip = ip();
  mca.id = id();
  mcnotify_return mcr;

  // check the watchlist and see if you know about any nodes that
  // can fill those prefixes
  vector<NodeInfo *> nodelist;
  for( uint i = 0; i < args->watchlist.size(); i++ ) {
    for( uint j = 0; j < _base; j++ ) {
      if( !args->watchlist[i][j] ) {
	NodeInfo * ni = _rt->read( i, j );
	if( ni != NULL && ni->_addr != ip() && ni->_addr != args->new_ip ) {
	  nodelist.push_back( ni );
	  args->watchlist[i][j] = true;
	} else {
	  if( ni != NULL ) {
	    delete ni;
	  }
	}
      }
    }
  }
  mca.nodelist = nodelist;

  doRPC( args->new_ip, &Tapestry::handle_mcnotify, &mca, &mcr );

  // free the nodelist
  for( uint i = 0; i < nodelist.size(); i++ ) {
    delete nodelist[i];
  }

  // don't go on if this is from a lock
  if( args->from_lock ) {
    // we won't be doing a multicast, so it's ok to add it now
    add_to_rt( args->new_ip, args->new_id );
    return;
  }

  RPCSet rpcset;
  map<unsigned, mc_callinfo*> resultmap;
  unsigned int numcalls = 0;
  // then, find any other node that shares this prefix and multicast
  for( uint i = args->alpha; i < _digits_per_id; i++ ) {
    for( uint j = 0; j < _base; j++ ) {
      NodeInfo *ni = _rt->read( i, j );
      // don't RPC to ourselves or to the new node
      if( ni == NULL || ni->_addr == ip() || ni->_addr == args->new_ip ) {
	if( ni != NULL ) {
	  delete ni;
	}
	continue;
      } else {
	mc_args *mca = New mc_args();
	mc_return *mcr = New mc_return();
	mca->new_ip = args->new_ip;
	mca->new_id = args->new_id;
	mca->alpha = i + 1;
	mca->from_lock = false;
	mca->watchlist = args->watchlist;
	TapDEBUG(2) << "multicasting info for " << args->new_ip << " to " << 
	  ni->_addr << "/" << print_guid( ni->_id ) << endl;
	unsigned rpc = asyncRPC( ni->_addr, &Tapestry::handle_mc, mca, mcr );
	assert(rpc);
	resultmap[rpc] = New mc_callinfo(ni->_addr, mca, mcr);
	rpcset.insert(rpc);
	numcalls++;
	delete ni;
      }
      
    }
  }

  // also, multicast to any locks that might need it
  vector <NodeInfo> * locks = _rt->get_locks( args->new_id );
  for( uint i = 0; i < locks->size(); i++ ) {
    NodeInfo ni = (*locks)[i];
    if( ni._addr != args->new_ip ) {
	mc_args *mca = New mc_args();
	mc_return *mcr = New mc_return();
	mca->new_ip = args->new_ip;
	mca->new_id = args->new_id;
	mca->alpha = args->alpha;
	mca->from_lock = true;
	mca->watchlist = args->watchlist;
	TapDEBUG(2) << "multicasting info for " << args->new_ip << " to " << 
	  ni._addr << "/" << print_guid( ni._id ) << " as a lock " << endl;
	unsigned rpc = asyncRPC( ni._addr, &Tapestry::handle_mc, mca, mcr );
	assert(rpc);
	resultmap[rpc] = New mc_callinfo(ni._addr, mca, mcr);
	rpcset.insert(rpc);
	numcalls++;
    }
  }

  // wait for them all to return
  for( unsigned int i = 0; i < numcalls; i++ ) {
    bool ok;
    unsigned donerpc = rcvRPC( &rpcset, ok );
    mc_callinfo *ci = resultmap[donerpc];
    TapDEBUG(2) << "mc to " << ci->ip << " about " << args->new_ip << 
      " is done" << endl;
    delete ci;
  }

  // we wait until here to add the new node to your table
  // since other simultaneous inserts will require sending the mc to
  // at least one unlocked pointer, and if this guy is in the routing table
  // already, we may send to it even though its locked.
  add_to_rt( args->new_ip, args->new_id );

  _rt->remove_lock( args->new_ip, args->new_id );

  TapDEBUG(2) << "multicast done for " << args->new_ip << endl;

  // TODO: object pointer transferal

}

void 
Tapestry::handle_nn(nn_args *args, nn_return *ret)
{

  // send back all backward pointers
  vector<NodeInfo> nns;

  vector<NodeInfo> *bps = _rt->get_backpointers( args->alpha );
  for( uint i = 0; i < bps->size(); i++ ) {
    nns.push_back( (*bps)[i] );
  }

  // do we really need to send the forward pointers?  The theory paper says to,
  // but the Java implementation doesn't.  for now we won't.  TODO maybe.

  // add yourself to the list
  NodeInfo self( ip(), id() );
  nns.push_back( self );

  ret->nodelist = nns;

  // finally, add this guy to our table
  add_to_rt( args->ip, args->id );

}

void 
Tapestry::handle_ping(ping_args *args, ping_return *ret)
{
  TapDEBUG(4) << "pinged." << endl;
  // do nothing
}

void 
Tapestry::handle_mcnotify(mcnotify_args *args, mcnotify_return *ret)
{

  TapDEBUG(3) << "got mcnotify from " << args->ip << endl;
  NodeInfo *mc_node = new NodeInfo( args->ip, args->id );
  initlist.push_back( mc_node );

  // add all the nodes on the nodelist as well
  nodelist_args na;
  na.nodelist = args->nodelist;
  nodelist_return nr;
  handle_nodelist( &na, &nr );

}

void 
Tapestry::add_to_rt( IPAddress new_ip, GUID new_id )
{

  // first get the distance to this node
  // TODO: asynchronous pings would be nice
  // TODO: also, maybe a ping cache of some kind
  Time distance = ping( new_ip, new_id );

  // the RoutingTable takes care of placing (and removing) backpointers for us
  _rt->add( new_ip, new_id, distance );

  return;

}

bool
Tapestry::stabilized(vector<GUID> lid)
{
  
  // if we don't think we've joined, we can't possibly be stable
  if( !joined ) {
    TapDEBUG(1) << "Haven't even joined yet." << endl;
    return false;
  }

  // for every GUID, make sure it either exists in your routing table,
  // or someone else does

  for( uint i = 0; i < lid.size(); i++ ) {

    GUID currguid = lid[i];
    if( !_rt->contains(currguid) ) {
      int match = guid_compare( currguid, id() );
      if( match == -1 ) {
	TapDEBUG(1) << "doesn't have itself in the routing table!" << endl;
	return false;
      } else {
	NodeInfo * ni = _rt->read( match, get_digit( currguid, match ) );
	if( ni == NULL ) {
	  TapDEBUG(1) << "has a hole in the routing table at (" << match <<
	    "," << get_digit( currguid, match ) << ") where " << 
	    print_guid( currguid ) << " would fit." << endl;
	    return false;
	} else {
	  delete ni;
	}
      }
    }

  }
  
  return true;
}

void
Tapestry::init_state(list<Protocol *> lid)
{

  TapDEBUG(2) << "init_state: about to add everyone" << endl;
  // for every node but this own, add them all to your routing table
  vector<NodeInfo *> nodes;
  for(list<Protocol*>::const_iterator i = lid.begin(); i != lid.end(); ++i) {

    Tapestry *currnode = (Tapestry*) *i;
    if( currnode->ip() == ip() ) {
      continue;
    }
    
    // cheat and get the latency straight from the topology
    Time rtt = 2*Network::Instance()->gettopology()->latency( ip(), 
							      currnode->ip() );

    _rt->add( currnode->ip(), currnode->id(), rtt, false );
  }

  // now that everyone's been added, place backpointers on everyone 
  // who is still in the table
  for(list<Protocol*>::const_iterator i = lid.begin(); i != lid.end(); ++i) {

    Tapestry *currnode = (Tapestry*) *i;
    if( currnode->ip() == ip() ) {
      continue;
    }

    // TODO: should I send backpointers for levels lower than the
    // maximum match?  That would be a pain.
    if( _rt->contains( currnode->id() ) ) {
      currnode->got_backpointer( ip(), id(), 
				 guid_compare( id(), currnode->id() ), 
				 false );
    }
  }

  joined = true;
  TapDEBUG(2) << "init_state: finished adding everyone" << endl;

}

Time
Tapestry::ping( IPAddress other_node, GUID other_id )
{
  // if it's already in the table, don't worry about it
  if( _rt->contains( other_id ) ) {
    return _rt->get_time( other_id );
  }

  Time before = now();
  ping_args pa;
  ping_return pr;
  TapDEBUG(4) << "about to ping " << other_node << endl;
  doRPC( other_node, &Tapestry::handle_ping, &pa, &pr );
  TapDEBUG(4) << "done with ping " << other_node << endl;
  return now() - before;
}

void
Tapestry::multi_add_to_rt( vector<NodeInfo *> *nodes )
{
  RPCSet ping_rpcset;
  map<unsigned, ping_callinfo*> ping_resultmap;
  Time before_ping = now();
  multi_add_to_rt_start( &ping_rpcset, &ping_resultmap, nodes );
  multi_add_to_rt_end( &ping_rpcset, &ping_resultmap, before_ping );
}

void
Tapestry::multi_add_to_rt_start( RPCSet *ping_rpcset, 
				 map<unsigned, ping_callinfo*> *ping_resultmap,
				 vector<NodeInfo *> *nodes )
{
  ping_args pa;
  ping_return pr;
  for( uint j = 0; j < nodes->size(); j++ ) {
    NodeInfo *ni = (*nodes)[j];
    // do an asynchronous RPC to each one, collecting the ping times in the
    // process
    // however, no need to ping if we already know the ping time, right?
    if( !_rt->contains( ni->_id ) ) {
      unsigned rpc = asyncRPC( ni->_addr, 
			       &Tapestry::handle_ping, &pa, &pr );
      assert(rpc);
      (*ping_resultmap)[rpc] = New ping_callinfo(ni->_addr, 
						ni->_id);
      ping_rpcset->insert(rpc);
    }
  }
}

void
Tapestry::multi_add_to_rt_end( RPCSet *ping_rpcset, 
			       map<unsigned, ping_callinfo*> *ping_resultmap,
			       Time before_ping )
{
  // check for done pings
  assert( ping_rpcset->size() == ping_resultmap->size() );
  uint setsize = ping_rpcset->size();
  for( unsigned int j = 0; j < setsize; j++ ) {
    bool ok;
    unsigned donerpc = rcvRPC( ping_rpcset, ok );
    Time ping_time = now() - before_ping;
    ping_callinfo *pi = (*ping_resultmap)[donerpc];
    TapDEBUG(4) << "donerpc: " << donerpc << " ip: " << pi->ip << endl;
    // TODO: ok, but now how do I call rt->add without it placing 
    // backpointers synchronously?
    pi->rtt = ping_time;
  }
  
  // now that all the pings are done, we can add them to our routing table
  // (possibly sending synchronous backpointers around) without messing
  // up the measurements
  for(map<unsigned, ping_callinfo*>::iterator j=ping_resultmap->begin(); 
      j != ping_resultmap->end(); ++j) {
    ping_callinfo *pi = (*j).second;
    //assert( pi->rtt == ping( pi->ip, pi->id ) );
    TapDEBUG(4) << "ip: " << pi->ip << endl;
    // make sure it's not the default (we actually pinged this one)
    assert( pi->rtt != 87654 );
    _rt->add( pi->ip, pi->id, pi->rtt );
    delete pi;
  }
  
}

void
Tapestry::place_backpointer( RPCSet *bp_rpcset, 
			     map<unsigned, backpointer_args*> *bp_resultmap, 
			     IPAddress bpip, int level, 
			     bool remove )
{
  backpointer_args *bpa = New backpointer_args();
  bpa->ip = ip();
  bpa->id = id();
  bpa->level = level;
  bpa->remove = remove;
  backpointer_return bpr;
  TapDEBUG(3) << "sending bp to " << bpip << endl;
  unsigned rpc = asyncRPC( bpip, &Tapestry::handle_backpointer, bpa, &bpr );
  bp_rpcset->insert(rpc);
  (*bp_resultmap)[rpc] = bpa;
  // for now assume no failures
}

void
Tapestry::place_backpointer_end( RPCSet *bp_rpcset,
				 map<unsigned,backpointer_args*>*bp_resultmap) 
{

  // wait for each to finish
  uint setsize = bp_rpcset->size();
  for( unsigned int j = 0; j < setsize; j++ ) {
    bool ok;
    unsigned donerpc = rcvRPC( bp_rpcset, ok );
    backpointer_args *bpa = (*bp_resultmap)[donerpc];
    delete bpa;
  }
}

void 
Tapestry::handle_backpointer(backpointer_args *args, backpointer_return *ret)
{
  TapDEBUG(3) << "got a bp msg from " << args->id << endl;

  got_backpointer( args->ip, args->id, args->level, args->remove );

  // maybe this person should be in our table?
  //add_to_rt( args->ip, args->id );

}

void
Tapestry::got_backpointer( IPAddress bpip, GUID bpid, uint level, bool remove )
{

  if( remove ) {
    _rt->remove_backpointer( bpip, bpid, level );
  } else {
    _rt->add_backpointer( bpip, bpid, level );
  }

}

IPAddress
Tapestry::next_hop( GUID key )
{
  // first, figure out how many digits we share, and start looking at 
  // that level
  int level = guid_compare( id(), key );
  if( level == -1 ) {
    // it's us!
    return ip();
  }

  NodeInfo *ni = _rt->read( level, get_digit( key, level ) );

  // if it has an entry, use it
  if( ni != NULL ) {
    // this cannot be us, since this should be the entry where we differ
    // from the key, but just to be sure . . .
     assert( ni->_id != id() );
     IPAddress addr = ni->_addr;
     delete ni;
     return addr;
  } else {
    // if there's no such entry, it's time to surrogate route.  yeah baby.
    // keep adding 1 to the digit until you find a match.  If it's us,
    // go up to the next level and keep on trying till the end
    // (From Fig. 3 in the JSAC paper)

    for( uint i = level; i < _digits_per_id; i++ ) {

      uint j = get_digit( key, i );
      while( ni == NULL ) {

	// NOTE: we can't start with looking at the j++ entry, since
	// this might be the next level up, and we'd want to start at j
	ni = _rt->read( i, j );

	j++;
	if( j == _base ) {
	  // think circularly
	  j = 0;
	}
      }

      // if it is us, go around another time
      // otherwise, we've found the next hop
      if( ni->_addr != ip() ) {
	IPAddress addr = ni->_addr;
	delete ni;
	return addr;
      } else {
	if( ni != NULL ) {
	  delete ni;
	  ni = NULL;
	}
      }

    }
    
  }

  // didn't find a better surrogate, so it must be us
  return ip();

}

int
Tapestry::guid_compare( GUID key1, GUID key2 )
{

  // if they're the same, return -1
  if( key1 == key2 ) {
    return -1;
  }
  uint i = 0;
  // otherwise, figure out where they differ
  for( ; i < _digits_per_id; i++ ) {
    if( get_digit( key1, i ) != get_digit( key2, i ) ) {
      break;
    }
  }

  return (int) i;

}

void
Tapestry::leave(Args *args)
{
  crash (args);
}

void
Tapestry::crash(Args *args)
{
  TapDEBUG(2) << "Tapestry crash" << endl;
}

string
Tapestry::print_guid( GUID id )
{

  char buf[_digits_per_id+1];

  //printf( "initial guid: %16qx\n", id );
  // print it out, digit by digit
  // (in order to get leading zeros)
  for( uint i = 0; i < _digits_per_id; i++ ) {
    sprintf( &(buf[i]), "%x", get_digit(id, i) );
  }

  return string(buf);

}

void
Tapestry::print_guid( GUID id, ostream &s )
{
  for( uint i = 0; i < _digits_per_id; i++ ) {
    char digit_string[2];
    sprintf( digit_string, "%x", get_digit(id, i) );
    s << digit_string;
  }
}

uint
Tapestry::get_digit( GUID id, uint digit )
{

  // can't request a digit higher than what we have, right?
  // 0-based . . .
  assert( digit < _digits_per_id );

  // shift left to get rid of leading zeros
  GUID shifted_id = id << (digit*_bits_per_digit);
  // shift right to get rid of the others
  shifted_id = shifted_id >> ((_digits_per_id-1)*_bits_per_digit);

  return shifted_id;
}

//////////  RouteEntry  ///////////

RouteEntry::RouteEntry()
{
  _size = 0;
  _nodes = New NodeInfo *[NODES_PER_ENTRY];
}

RouteEntry::RouteEntry( NodeInfo *first_node )
{
  assert( first_node );
  _nodes = New NodeInfo *[NODES_PER_ENTRY];
  _nodes[0] = first_node;
  _size = 1;
}

RouteEntry::~RouteEntry()
{
  for( uint i = 0; i < _size; i++ ) {

    if( _nodes[i] != NULL ) {
      delete _nodes[i];
      _nodes[i] = NULL;
    }
  }
  delete [] _nodes;
}

NodeInfo *
RouteEntry::get_first()
{
  return _nodes[0];
}

NodeInfo *
RouteEntry::get_at( uint pos )
{
  assert( pos < NODES_PER_ENTRY );
  return _nodes[pos];
}

uint
RouteEntry::size()
{
  return _size;
}

bool
RouteEntry::add( NodeInfo *new_node, NodeInfo **kicked_out )
{
  
  assert( new_node );
  // if this node is already in the entry, just remove it so that it can
  // be resorted in with the new distance
  for( uint i = 0; i < _size; i++ ) {
    if( *new_node == *(_nodes[i]) ) {
      // if the distance is the same, don't do anything
      if( new_node->_distance != _nodes[i]->_distance ) {
	// positive if distance went up
	int diff = new_node->_distance - _nodes[i]->_distance;
	_nodes[i]->_distance = new_node->_distance;
	if( diff > 0 ) {
	  // distance went up, make sure it's still smaller than what 
	  // comes next
	  while( i+1 < _size && 
		 _nodes[i]->_distance > _nodes[i+1]->_distance ) {
	    NodeInfo * tmp = _nodes[i];
	    _nodes[i] = _nodes[i+1];
	    _nodes[i+1] = tmp;
	    i++;
	  }
	} else {
	  // distance went down, make sure it's still bigger than what 
	  // comes before
	  if( i > 0 ) {
	    cout << _nodes[i-1] << " " << endl;
	  }
	  while( i > 0 && 
		 _nodes[i]->_distance < _nodes[i-1]->_distance ) {
	    NodeInfo * tmp = _nodes[i];
	    _nodes[i] = _nodes[i-1];
	    _nodes[i-1] = tmp;
	    i--;
	  }
	}
      }
      // didn't actually add in the same node twice, so its false
      return false;
    }
  }

  // see if it's closer than any other entry so far
  for( uint i = 0; i < _size; i++ ) {
    if( new_node->_distance < _nodes[i]->_distance ) {
      NodeInfo *replacement = new_node;
      // insert the new node and shift the others down
      for( uint j = i; j < NODES_PER_ENTRY; j++ ) {
	NodeInfo *tmp = _nodes[j];
	_nodes[j] = replacement;
	replacement = tmp;
      }

      if( _size == NODES_PER_ENTRY ) {
	// this is the last one that got kicked out, and didn't get put back
	*kicked_out = replacement;
      }

      // did we add a new one?
      if( _size < NODES_PER_ENTRY ) {
	_size++;
      }

      return true;
    }
  }

  // if it wasn't put in by now, and there's still space in the entry,
  // it must belong at the end
  if( _size < NODES_PER_ENTRY ) {
    _nodes[_size] = new_node;
    _size++;
    return true;
  }

  return false;
}

////////  Routing Table ////////////

RoutingTable::RoutingTable( Tapestry *node )
{
  assert(node);
  _node = node;
  TapRTDEBUG(2) << "Routing Table constructor" << endl;
  _table = New RouteEntry **[_node->_digits_per_id];
  // initialize all the rows
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    _table[i] = New RouteEntry *[_node->_base];
    for( uint j = 0; j < _node->_base; j++ ) {
      _table[i][j] = NULL;
    }
  }

  // now we add ourselves to the table
  add( _node->ip(), _node->id(), 0 );

  _backpointers = New vector<NodeInfo> *[_node->_digits_per_id];
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    _backpointers[i] = 0;
  }

  // init the locks
  _locks = New vector<NodeInfo> **[_node->_digits_per_id];
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    _locks[i] = New vector<NodeInfo> *[_node->_base];
  }

}

RoutingTable::~RoutingTable()
{
  TapRTDEBUG(3) << "rt destroyed\n" << endl;
  //delete all route entries
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    for( uint j = 0; j < _node->_base; j++ ) {
      if( _table[i][j] != NULL ) {
	delete _table[i][j];
	_table[i][j] = NULL;
      }
    }
    delete [] _table[i];
  }
  delete [] _table;
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    for( uint j = 0; j < _node->_base; j++ ) {
      if( _locks[i][j] != NULL ) {
	delete _locks[i][j];
	_locks[i][j] = NULL;
      }
    }
    delete _locks[i];
  }
  delete [] _locks;
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    if( _backpointers[i] != NULL ) {
      delete _backpointers[i];
    }
  }
  delete [] _backpointers;

}

bool
RoutingTable::add( IPAddress ip, GUID id, Time distance )
{
  return add( ip, id, distance, true );
}

bool
RoutingTable::add( IPAddress ip, GUID id, Time distance, bool sendbp )
{
  Tapestry::RPCSet bp_rpcset;
  map<unsigned,Tapestry::backpointer_args*> bp_resultmap;
  bool in_added = false;

  // find the spots where it fits and add it
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    uint j = _node->get_digit( id, i );
    NodeInfo *new_node = New NodeInfo( ip, id, distance );
    RouteEntry *re = _table[i][j];
    if( re == NULL ) {
      // brand new entry, huzzah
      re = New RouteEntry( new_node );
      _table[i][j] = re;
      in_added = true;
      if( ip != _node->ip() && sendbp ) {
	_node->place_backpointer( &bp_rpcset, &bp_resultmap, ip, i, false );
      }
    } else {
      // add it to an existing one
      NodeInfo *kicked_out_node = NULL;
      bool level_added = re->add( new_node, &kicked_out_node );
      in_added = in_added | level_added;
      if( kicked_out_node != NULL ) {
	// tell the node we're no longer pointing to it at this level
	if( sendbp ) {
	  _node->place_backpointer( &bp_rpcset, &bp_resultmap, 
				    kicked_out_node->_addr, i, 
				    true );
	}
	TapRTDEBUG(4) << "kicked out " << kicked_out_node->_addr << endl;
	delete kicked_out_node;
      }
      if( level_added && ip != _node->ip() ) {
	// tell the node we are pointing to it
	if( sendbp ) {
	  _node->place_backpointer( &bp_rpcset, &bp_resultmap, ip, i, false );
	}
      } else {
	delete new_node;
      }
    }
    // if the last digit wasn't a match, we're done
    if( j != _node->get_digit( _node->id(), i ) ) {
      break;
    }
  }  

  // wait for bp rpcs to finish
  if( sendbp ) {
    _node->place_backpointer_end( &bp_rpcset, &bp_resultmap );
  }
  return in_added;
}

NodeInfo *
RoutingTable::read( uint i, uint j )
{
    //  TapRTDEBUG(2) << "read " << _table << endl;
  if( _table[i] == NULL ) {
    TapRTDEBUG(2) << "read " << i << " " << j << endl;
  }
  RouteEntry *re = _table[i][j];
  if( re == NULL || re->get_first() == NULL ) {
    return NULL;
  } else {
    return New NodeInfo(re->get_first()->_addr, re->get_first()->_id);
  }
}

bool
RoutingTable::contains( GUID id )
{
  int alpha = _node->guid_compare( _node->id(), id );  
  if( alpha == -1 ) {
    alpha = _node->_digits_per_id - 1;
  }
  RouteEntry *re = _table[alpha][_node->get_digit( id, alpha )];
  if( re != NULL ) {
    // see if it's in there anywhere
    for( uint i = 0; i < re->size(); i++ ) {
      NodeInfo *ni = re->get_at(i);
      if( ni->_id == id ) {
	return true;
      }
    }
  }
  return false;

}

Time
RoutingTable::get_time( GUID id )
{

  uint alpha = _node->guid_compare( _node->id(), id );  
  RouteEntry *re = _table[alpha][_node->get_digit( id, alpha )];
  if( re != NULL ) {
    // see if it's in there anywhere
    for( uint i = 0; i < re->size(); i++ ) {
      NodeInfo *ni = re->get_at(i);
      if( ni->_id == id ) {
	return ni->_distance;
      }
    }
  }
  // some maximum time (-1 won't work because it's unsigned)
  return MAXTIME;

}

void 
RoutingTable::add_backpointer( IPAddress ip, GUID id, uint level )
{
  vector<NodeInfo> * this_level = get_backpointers(level);
  NodeInfo new_node( ip, id );
  this_level->push_back( new_node );
}

void 
RoutingTable::remove_backpointer( IPAddress ip, GUID id, uint level )
{
  
  vector<NodeInfo> * this_level = get_backpointers(level);
  NodeInfo new_node( ip, id );
  for( vector<NodeInfo>::iterator i = this_level->begin(); 
       i != this_level->end(); i++ ) {
    NodeInfo curr_node = *i;
    if( curr_node == new_node ) {
      this_level->erase(i);
      // only erase the first occurance
      return;
    }
  }
}

vector<NodeInfo> *
RoutingTable::get_backpointers( uint level )
{
  vector<NodeInfo> * this_level = _backpointers[level];
  if( this_level == NULL ) {
    this_level = New vector<NodeInfo>;
    _backpointers[level] = this_level;
  }
  return this_level;
}

void 
RoutingTable::set_lock( IPAddress ip, GUID id )
{
  vector<NodeInfo> * this_level = get_locks(id);
  if( this_level == NULL ) {
    // can't lock yourself
    return;
  }
  NodeInfo new_node( ip, id );
  bool add = true;
  for(vector<NodeInfo>::iterator i=this_level->begin(); 
      i != this_level->end(); ++i) {
    if( *i == new_node ) {
      add = false;
    }
  }
  if( add ) {
    this_level->push_back( new_node );
  }
}

void 
RoutingTable::remove_lock( IPAddress ip, GUID id )
{
  vector<NodeInfo> * this_level = get_locks(id);
  NodeInfo new_node( ip, id );
  for(vector<NodeInfo>::iterator i=this_level->begin(); 
      i != this_level->end(); ++i) {
    if( *i == new_node ) {
      this_level->erase(i);
      // only erase the first occurance
      return;
    }
  }

}

vector<NodeInfo> *
RoutingTable::get_locks( GUID id )
{
  int match = _node->guid_compare( _node->id(), id );
  if( match == -1 ) {
    // can't lock yourself
    return NULL;
  }
  int lastsame = 0;
  if( match > 0 ) {
      lastsame = _node->get_digit(id, match-1);
  }
  vector<NodeInfo> * this_level = _locks[match][lastsame];
  if( this_level == NULL ) {
    this_level = New vector<NodeInfo>;
    _locks[match][lastsame] = this_level;
  }
  return this_level;
}

ostream&
RoutingTable::insertor( ostream &s ) const
{
  s<< "RoutingTable for " << _node->ip() << "/";
  _node->print_guid( _node->id(), s );
  s << endl;

  // now print out each row until you hit an empty one
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    s << i << ": ";
    uint count = 0;
    for( uint j = 0; j < _node->_base; j++ ) {
      RouteEntry *re = _table[i][j];
      if( re == NULL || re->get_first() == NULL ) {
	s << "--- ";
      } else {
	NodeInfo *ni = re->get_first();
	s << ni->_addr << "/";
	_node->print_guid( ni->_id, s );
	s << " ";
	count++;
      }
    }
    s << endl;
    // if we only found ourselves in this row, get out
    if( count <= 1 ) {
      break;
    }
  }

  return s;
}

///// operator overloading ///////////

ostream& 
operator<< (ostream &s, RoutingTable const &rt)
{
  return rt.insertor(s);
}


bool 
operator== ( const NodeInfo & one, const NodeInfo & two )
{
  return one._addr == two._addr && one._id == two._id;
} 
