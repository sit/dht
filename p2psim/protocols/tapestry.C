/*
 * Copyright (c) 2003 Jeremy Stribling
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

/* $Id: tapestry.C,v 1.21 2003/11/03 06:55:52 strib Exp $ */
#include "tapestry.h"
#include "p2psim/network.h"
#include <stdio.h>
#include <math.h>
#include <iostream>
#include <map>
using namespace std;

Tapestry::Tapestry(Node *n, Args a)
  : P2Protocol(n),
    _base(a.nget<uint>("base", 16, 10)),
    _bits_per_digit((uint) (log10(((double) _base))/log10((double) 2))),
    _digits_per_id((uint) 8*sizeof(GUID)/_bits_per_digit),
    _redundant_lookup_num(a.nget<uint>("redundant_lookup_num", 3, 10))
{
  joined = false;
  _joining = false;
  _stab_scheduled = false;
  _my_id = get_id_from_ip(ip());
  TapDEBUG(2) << "Constructing" << endl;
  _rt = New RoutingTable(this, a.nget<uint>("redundancy", 3, 10));
  _waiting_for_join = New ConditionVar();

  //stabilization timer
  _stabtimer = a.nget<uint>("stabtimer", 10000, 10);
  _join_num = 0;

  // init stats
  while (stat.size() < (uint) STAT_SIZE) {
    stat.push_back(0);
    num_msgs.push_back(0);
  }

}

Tapestry::~Tapestry()
{

  delete _rt;
  delete _waiting_for_join;

  TapDEBUG(2) << "Destructing" << endl;

  TapDEBUG(0) << "PARAMS: base " << _base << " stabtimer " << _stabtimer <<
    " redundant_lookup_num " << _redundant_lookup_num << endl;

  // print out statistics
  print_stats();

}

void
Tapestry::record_stat(stat_type type, uint num_ids, uint num_else )
{

  TapDEBUG(5) << "record stat " << type << endl;

  assert(stat.size() > (uint) type);
  stat[type] += 20 + 4*num_ids + num_else;
  num_msgs[type]++;

}

void
Tapestry::print_stats()
{

  TapDEBUG(0) << "STATS: " <<
    "join " << stat[STAT_JOIN] << " " << num_msgs[STAT_JOIN] <<
    " lookup " << stat[STAT_LOOKUP] << " " << num_msgs[STAT_LOOKUP] <<
    " nodelist " << stat[STAT_NODELIST] << " " << num_msgs[STAT_NODELIST] <<
    " mc " << stat[STAT_MC] << " " << num_msgs[STAT_MC] <<
    " ping " << stat[STAT_PING] << " " << num_msgs[STAT_PING] <<
    " backpointer " << stat[STAT_BACKPOINTER] << " " 
	      << num_msgs[STAT_BACKPOINTER] <<
    " mcnotify " << stat[STAT_MCNOTIFY] << " " << num_msgs[STAT_MCNOTIFY] <<
    " nn " << stat[STAT_NN] << " " << num_msgs[STAT_NN] <<
    " repair " << stat[STAT_REPAIR] << " " << num_msgs[STAT_REPAIR] <<
    endl;
}

void
Tapestry::lookup(Args *args) 
{

  GUID key = args->nget<GUID>("key");

  TapDEBUG(0) << "Tapestry Lookup for key " << print_guid(key) << endl;

  // we'll be trying this X times, to simulate application-level retries
  uint num_tries = 1;

  lookup_args la;
  la.key = key;
  la.looker = ip();

  lookup_return lr;
  while( num_tries <= 5 ) {
    
    lr.hopcount = 0;
    lr.failed = false;
    
    handle_lookup( &la, &lr );

    if( !lr.failed ) {
      if( lr.owner_id == lr.real_owner_id ) {
	TapDEBUG(0) << "Lookup complete for key " << print_guid(key) 
		    << ": ip " << lr.owner_ip << ", id " 
		    << print_guid(lr.owner_id) << ", hops " << lr.hopcount
		    << ", numtries " << num_tries << endl;
      } else {
	TapDEBUG(0) << "Lookup incorrect for key " << print_guid(key) 
		    << ": ip " << lr.owner_ip << ", id " 
		    << print_guid(lr.owner_id) << ", real root " 
		    << print_guid(lr.real_owner_id) << " hops " 
		    << lr.hopcount << ", numtries " << num_tries << endl;
      }
      break;
    } else {
      TapDEBUG(0) << "one lookup failed for key " << print_guid(key) 
		  << ", numtries " << num_tries << endl;
    }

    num_tries++;

  }

  if( lr.failed ) {
    TapDEBUG(0) << "Lookup failed for key " << print_guid(key) << endl;
  }

}

void 
Tapestry::handle_lookup(lookup_args *args, lookup_return *ret)
{

  TapDEBUG(5) << "hl enter" << endl;

  // find the next hop for the key.  if it's me, i'm done
  IPAddress *ips = New IPAddress[_redundant_lookup_num];
  for( uint i = 0; i < _redundant_lookup_num; i++ ) {
    ips[i] = 0;
  }
  next_hop( args->key, &ips, _redundant_lookup_num );
  uint i = 0;
  for( ; i < _redundant_lookup_num; i++ ) {
    IPAddress next = ips[i];
    if( next == 0 ) {
      continue;
    }
    if( next == ip() ) {
      ret->owner_ip = ip();
      ret->owner_id = id();
      // this will be incremented at each hop backwards
      ret->hopcount = 0;
      ret->real_owner_id = lookup_cheat( args->key );
      break;
    } else {
      // it's not me, so forward the query
      record_stat(STAT_LOOKUP, 1, 0);
      bool succ = doRPC( next, &Tapestry::handle_lookup, args, ret );
      if( succ ) {
	record_stat( STAT_LOOKUP, 1, 2 );
      }
      if( succ && !ret->failed ) {
	// don't need to try the next one
	ret->hopcount++;
	break;
      } else {
	// print out that a failure happened
	TapDEBUG(0) << "Failure happened during the lookup of key " << 
	  print_guid(args->key) << ", trying to reach node " << next << 
	  " for node " << args->looker << endl;
	ret->failed = false;
      }
    }
  }

  // if we were never successful, set the failed flag
  if( i == _redundant_lookup_num ) {
    ret->failed = true;
  }

  delete ips;
  TapDEBUG(5) << "hl exit " << endl;

}

void
Tapestry::insert(Args *args) 
{
  TapDEBUG(2) << "Tapestry Insert" << endl;
}

void
Tapestry::have_joined()
{
   joined = true;
   _joining = false;
   _waiting_for_join->notifyAll();
   TapDEBUG(0) << "Finishing joining." << endl;
   if( !_stab_scheduled ) {
     delaycb( _stabtimer, &Tapestry::check_rt, (void *) 0 );
     _stab_scheduled = true;
   }
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Tapestry::join(Args *args)
{

  TapDEBUG(5) << "j enter" << endl;

  notifyObservers();

  if( _joining ) {
    TapDEBUG(0) << "Tried to join while joining -- ignoring" << endl;
    return;
  }

  IPAddress wellknown_ip = args->nget<IPAddress>("wellknown");
  TapDEBUG(3) << ip() << " Wellknown: " << wellknown_ip << endl;

  // might already be joined if init_state was used
  if( joined ) {
    TapDEBUG(5) << "j exit" << endl;
    return;
  }

  _joining = true;
  uint curr_join = ++_join_num;

  TapDEBUG(0) << "Tapestry join " << curr_join << endl;

  // if we're the well known node, we're done
  if( ip() == wellknown_ip ) {
    have_joined();
    TapDEBUG(5) << "j exit" << endl;
    return;
  }

  // contact the well known machine, and have it start routing to the surrogate
  join_args ja;
  ja.ip = ip();
  ja.id = id();
  join_return jr;
  uint attempts = 30;

  // keep attempting to join until you can't attempt no mo'
  do {
    attempts--;
    jr.failed = false;
    record_stat(STAT_JOIN, 1, 0);
    bool succ = doRPC( wellknown_ip, &Tapestry::handle_join, &ja, &jr );

    // make sure we haven't crashed and/or started another join
    if( !node()->alive() || _join_num != curr_join ) {
      TapDEBUG(0) << "Old join " << curr_join << " aborting" << endl;
      return;
    }
    
    if( !succ ) {
      TapDEBUG(0) << "Well known ip died!  BAD!\n";
      return;
    } else {
      record_stat(STAT_JOIN, 1, 1);
    }
  } while( jr.failed && attempts >= 0 );

  if( jr.failed ) {
    TapDEBUG(0) << "All my joins failed!  BAD!" << endl;
    return;
  }

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
    TapDEBUG(3) << "initing level " << i << " out of " << init_level 
		<< " size = " << initlist.size() << endl;
    multi_add_to_rt_start( &ping_rpcset, &ping_resultmap, &initlist, 
			   NULL, true );

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
      record_stat(STAT_NN, 1, 1);
      unsigned rpc = asyncRPC( ni._addr, &Tapestry::handle_nn, na, nr );
      assert(rpc);
      nn_resultmap[rpc] = New nn_callinfo(ni._addr, na, nr);
      nn_rpcset.insert(rpc);
      num_nncalls++;

    }

    multi_add_to_rt_end( &ping_rpcset, &ping_resultmap, before_ping, NULL, 
			 false );

    for( uint j = 0; j < num_nncalls; j++ ) {

      bool ok;
      unsigned donerpc = rcvRPC( &nn_rpcset, ok );
      nn_callinfo *ncall = nn_resultmap[donerpc];
      nn_return nnr = *(ncall->nr);
      TapDEBUG(2) << "done with nn with " << ncall->ip << endl;

      if( ok ) {
	record_stat( STAT_NN, nnr.nodelist.size(), 0 );
	for( uint k = 0; k < nnr.nodelist.size(); k++ ) {
	  // make sure this one isn't on there yet
	  // TODO: make this more efficient.  Maybe use a hash set or something
	  NodeInfo *currseed = nnr.nodelist[k];
	  
	  // don't add ourselves, duh
	  if( currseed->_id == id() ) {
	    delete currseed;
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
	    seeds.push_back( currseed );
	    TapDEBUG(3) << " has a seed of " << currseed->_addr << " and " << 
	      print_guid( currseed->_id ) << endl;
	  } else {
	    delete currseed;
	  }
	}
	TapDEBUG(3) << "done with adding seeds with " << ncall->ip << endl;
      }
      // TODO: for some reason the compiler gets mad if I try to delete nr
      // in the destructor
      delete ncall->nr;
      ncall->nr = NULL;
      delete ncall;
    }

    // make sure we haven't crashed and/or started another join
    if( !node()->alive() || _join_num != curr_join ) {
      TapDEBUG(0) << "Old join " << curr_join << " aborting" << endl;
      // need to delete all the seeds
      for( uint j = 0; j < seeds.size(); j++ ) {
	NodeInfo *currseed = seeds[j];
	delete currseed;
      }
      return;
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
    map< IPAddress, Time> timing;
    multi_add_to_rt( &seeds, &timing );

    // make sure we haven't crashed and/or started another join
    if( !node()->alive() || _join_num != curr_join ) {
      TapDEBUG(0) << "Old join " << curr_join << " aborting" << endl;
      // need to delete all the seeds
      for( uint j = 0; j < seeds.size(); j++ ) {
	NodeInfo *currseed = seeds[j];
	delete currseed;
      }
      return;
    }

    for( uint j = 0; j < seeds.size(); j++ ) {
      NodeInfo *currseed = seeds[j];
      // add them all to the routing table (this gets us the ping time for free
      TapDEBUG(3) << "about to get distance for " << currseed->_addr << endl;
      //add_to_rt( currseed->_addr, currseed->_id );
      TapDEBUG(3) << "added to rt for " << currseed->_addr << endl;
      //bool ok = false;
      currseed->_distance = timing[currseed->_addr];
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

    // make sure we haven't crashed and/or started another join
    if( !node()->alive() || _join_num != curr_join ) {
      TapDEBUG(0) << "Old join " << curr_join << " aborting" << endl;
      // need to delete all the currclose now
      for( uint j = 0; j < _k; j++ ) {
	NodeInfo *currseed = closestk[j];
	delete currseed;
      }
      return;
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
  initlist.clear();

  have_joined();
  TapDEBUG(5) << "j exit" << endl;
  TapDEBUG(2) << "join done" << endl;
  TapDEBUG(2) << *_rt << endl;

}

void
Tapestry::handle_join(join_args *args, join_return *ret)
{

  TapDEBUG(5) << "hj enter" << endl;
  TapDEBUG(2) << "got a join message from " << args->ip << "/" << 
    print_guid(args->id) << endl;

  // not allowed to participate in your own join!
  if( args->ip == ip() ) {
    ret->failed = true;
    TapDEBUG(5) << "hj exit" << endl;
    return;
  }

  // if our join has not yet finished, we must delay the handling of this
  // person's join.
  while( !joined ) {
    _waiting_for_join->wait();
    // hmmm. if we're now dead, indicate some kind of timeout probably
    if( !node()->alive() ) {
      ret->failed = true;
      TapDEBUG(5) << "hj exit" << endl;
      return; //TODO: ????
    }
  }

  // route toward the root
  IPAddress *ips = New IPAddress[_redundant_lookup_num];
  for( uint i = 0; i < _redundant_lookup_num; i++ ) {
    ips[i] = 0;
  }
  next_hop( args->id, &ips, _redundant_lookup_num );
  uint i = 0;
  for( ; i < _redundant_lookup_num; i++ ) {
    IPAddress next = ips[i];
    if( next == 0 ) {
      continue;
    }
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
      record_stat(STAT_NODELIST, na.nodelist.size(), 0);
      unsigned rpc = asyncRPC( args->ip, &Tapestry::handle_nodelist, 
			       &na, &nr );
      
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
	bool *level = New bool[_base];
	wl.push_back(level);
	for( uint j = 0; j < _base; j++ ) {
	  wl[i][j] = false;
	}
      }
      mca.watchlist = wl;
      
      handle_mc( &mca, &mcr );

      // finish up the nodelist rpc
      RPCSet rpcset;
      rpcset.insert(rpc);
      bool ok;
      rcvRPC( &rpcset, ok );
      if( ok ) {
	record_stat(STAT_NODELIST, 0, 0);
      }      
      // free the nodelist
      for( uint i = 0; i < na.nodelist.size(); i++ ) {
	delete na.nodelist[i];
      }

      
      // free the bools!
      for( int i = 0; i < alpha+1; i++ ) {
	bool *level = wl[i];
	delete level;
      }
      
      ret->surr_id = id();
      break;
    } else {
      // not the surrogate
      // recursive routing yo
      record_stat(STAT_JOIN, 1, 0);
      bool succ = doRPC( next, &Tapestry::handle_join, args, ret );
      if( succ ) {
	record_stat(STAT_JOIN, 1, 1);
      }
      if( succ && !ret->failed ) {
	// success!
	break;
      } else {
	ret->failed = false;
      }
    }
  }

  // if we were never successful, set the failed flag
  if( i == _redundant_lookup_num ) {
    ret->failed = true;
  }
  delete ips;

  TapDEBUG(5) << "hj exit" << endl;

}

void 
Tapestry::handle_nodelist(nodelist_args *args, nodelist_return *ret)
{

  TapDEBUG(5) << "nhl enter" << endl;
  TapDEBUG(3) << "handling a nodelist message" << endl;

  // add each to your routing table
  multi_add_to_rt( &(args->nodelist), NULL );
  /*
  for( uint i = 0; i < args->nodelist.size(); i++ ) {
    NodeInfo * curr_node = args->nodelist[i];
    add_to_rt( curr_node->_addr, curr_node->_id );
  }
  */

  TapDEBUG(5) << "nhl exit" << endl;

}

void
Tapestry::handle_mc(mc_args *args, mc_return *ret)
{

  TapDEBUG(5) << "mc enter" << endl;
  TapDEBUG(2) << "got multicast message for " << args->new_ip << endl;

  // not allowed to participate in your own join!
  if( args->new_ip == ip() ) {
    TapDEBUG(5) << "mc exit" << endl;
    return;
  }

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

  record_stat(STAT_MCNOTIFY, 1+nodelist.size(), 0);
  unsigned mcnrpc = asyncRPC( args->new_ip, &Tapestry::handle_mcnotify, 
			       &mca, &mcr );

  // don't go on if this is from a lock
  if( !args->from_lock ) {

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
	  record_stat(STAT_MC, 1, 2+mca->watchlist.size()*_base);
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
    vector <NodeInfo *> * locks = _rt->get_locks( args->new_id );
    for( uint i = 0; i < locks->size(); i++ ) {
      NodeInfo *ni = (*locks)[i];
      if( ni->_addr != args->new_ip ) {
	mc_args *mca = New mc_args();
	mc_return *mcr = New mc_return();
	mca->new_ip = args->new_ip;
	mca->new_id = args->new_id;
	mca->alpha = args->alpha;
	mca->from_lock = true;
	mca->watchlist = args->watchlist;
	TapDEBUG(2) << "multicasting info for " << args->new_ip << " to " << 
	  ni->_addr << "/" << print_guid( ni->_id ) << " as a lock " << endl;
	record_stat(STAT_MC, 1, 2+mca->watchlist.size()*_base);
	unsigned rpc = asyncRPC( ni->_addr, &Tapestry::handle_mc, mca, mcr );
	assert(rpc);
	resultmap[rpc] = New mc_callinfo(ni->_addr, mca, mcr);
	rpcset.insert(rpc);
	numcalls++;
      }
    }
    
    // wait for them all to return
    for( unsigned int i = 0; i < numcalls; i++ ) {
      bool ok;
      unsigned donerpc = rcvRPC( &rpcset, ok );
      if( ok ) {
	record_stat(STAT_MC, 0, 0);
      }
      mc_callinfo *ci = resultmap[donerpc];
      TapDEBUG(2) << "mc to " << ci->ip << " about " << args->new_ip << 
	" is done.  ok = " << ok << endl;
      delete ci;
    }
  }

  // finish up mcnotify
  RPCSet mcnrpcset;
  mcnrpcset.insert(mcnrpc);
  bool ok;
  rcvRPC( &mcnrpcset, ok );
  if( ok ) {
    record_stat(STAT_MCNOTIFY, 0, 0);
  }
  // free the nodelist
  for( uint i = 0; i < nodelist.size(); i++ ) {
    delete nodelist[i];
  }
  if( !ok ) {
    _rt->remove_lock( args->new_ip, args->new_id );
    TapDEBUG(3) << "Notify to new node failed, abandoning mc!" << endl;
    TapDEBUG(5) << "mc exit" << endl;
    return;
  }

  // we wait until here to add the new node to your table
  // since other simultaneous inserts will require sending the mc to
  // at least one unlocked pointer, and if this guy is in the routing table
  // already, we may send to it even though its locked.
  add_to_rt( args->new_ip, args->new_id );

  _rt->remove_lock( args->new_ip, args->new_id );

  TapDEBUG(2) << "multicast done for " << args->new_ip << endl;

  // TODO: object pointer transferal
  TapDEBUG(5) << "mc exit" << endl;

}

void 
Tapestry::handle_nn(nn_args *args, nn_return *ret)
{

  TapDEBUG(5) << "nn enter" << endl;
  // send back all backward pointers
  vector<NodeInfo *> nns;

  vector<NodeInfo *> *bps = _rt->get_backpointers( args->alpha );
  for( uint i = 0; i < bps->size(); i++ ) {
    NodeInfo *newnode = New NodeInfo( ((*bps)[i])->_addr, ((*bps)[i])->_id ); 
    nns.push_back( newnode );
  }

  // send all forward pointers at that level
  for( uint i = 0; i < _base; i++ ) {
    NodeInfo *newnode = _rt->read( args->alpha, i );
    if( newnode != NULL ) {
      nns.push_back( newnode );
    }
  }

  // add yourself to the list
  nns.push_back( New NodeInfo( ip(), id() ) );

  ret->nodelist = nns;

  // finally, add this guy to our table
  add_to_rt( args->ip, args->id );
  TapDEBUG(5) << "nn exit" << endl;

}

void 
Tapestry::handle_repair(repair_args *args, repair_return *ret)
{

  TapDEBUG(5) << "rep enter" << endl;
  // send back all backward pointers
  vector<NodeInfo> nns;

  assert( args->level < _digits_per_id && args->digit < _base );

  // TODO: limit the number?
  RouteEntry *re = _rt->get_entry( args->level, args->digit );
  for( uint i = 0; re != NULL && i < re->size(); i++ ) {
    NodeInfo *next = re->get_at(i);
    if( next != NULL && next->_addr != ip() && next->_id != args->bad_id ) {
	nns.push_back( *next );
    }
  }

  ret->nodelist = nns;
  TapDEBUG(5) << "rep exit" << endl;
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

  TapDEBUG(5) << "mcn enter" << endl;
  TapDEBUG(3) << "got mcnotify from " << args->ip << endl;
  NodeInfo *mc_node = New NodeInfo( args->ip, args->id );
  initlist.push_back( mc_node );

  // add all the nodes on the nodelist as well
  nodelist_args na;
  na.nodelist = args->nodelist;
  nodelist_return nr;
  handle_nodelist( &na, &nr );
  TapDEBUG(5) << "mcn exit" << endl;

}

void 
Tapestry::add_to_rt( IPAddress new_ip, GUID new_id )
{

  // first get the distance to this node
  // TODO: asynchronous pings would be nice
  // TODO: also, maybe a ping cache of some kind
  bool ok = true;
  Time distance = ping( new_ip, new_id, ok );

  if( ok ) {
    // the RoutingTable takes care of placing (and removing) backpointers 
    // for us
    _rt->add( new_ip, new_id, distance );
  }

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

  // TODO: do we need locking in this function?

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
Tapestry::check_rt(void *x)
{

  TapDEBUG(2) << "Checking the routing table" << endl;

  Time t = now();

  // do nothing if we should be dead or not fully alive
  if( !joined ) {
    _stab_scheduled = false;
    return;
  }

  // ping everyone in the routing table to
  //  - update latencies
  //  - ensure they're still alive
  
  // the easy way to do this is just to make a vector of all the nodes
  // in the routing table and simply try to add them all to the routing table.
  vector<NodeInfo *> nodes;
  for( uint i = 0; i < _digits_per_id; i++ ) {
    for( uint j = 0; j < _base; j++ ) {
      RouteEntry *re = _rt->get_entry( i, j );
      if( re == NULL || (re->get_first() != NULL && 
			 re->get_first()->_addr == ip() ) ) {
	// if this an entry with our own ip, don't bother delving further
	// the backups of yourself will appear in later levels
	// there should be no other duplicates though, so vector is safe
	continue;
      }
      for( uint k = 0; k < re->size(); k++ ) {
	nodes.push_back( re->get_at(k) );
      }
    }
  }

  RPCSet ping_rpcset;
  map<unsigned, ping_callinfo*> ping_resultmap;
  Time before_ping = now();
  multi_add_to_rt_start( &ping_rpcset, &ping_resultmap, &nodes, NULL, false );
  multi_add_to_rt_end( &ping_rpcset, &ping_resultmap, before_ping, NULL, true);
  TapDEBUG(2) << "finished checking routing table " << now() << " " << t << endl;

  // reschedule
  if ( t + _stabtimer < now()) { 
    //stabilizating has run past _stabtime seconds
    // schedule it in one ms, to avoid recursion
    TapDEBUG(2) << "rescheduling check_rt in 1" << endl;
    delaycb( 1, &Tapestry::check_rt, (void *) 0 );
  } else {
    Time later = _stabtimer - (now() - t);
    TapDEBUG(2) << "rescheduling check_rt in " << later << endl;
    delaycb( later, &Tapestry::check_rt, (void *) 0 );
  }

}

void
Tapestry::init_state(set<Protocol *> lid)
{

  // TODO: we shouldn't need locking in here, right?

  TapDEBUG(2) << "init_state: about to add everyone" << endl;
  // for every node but this own, add them all to your routing table
  vector<NodeInfo *> nodes;
  for(set<Protocol*>::const_iterator i = lid.begin(); i != lid.end(); ++i) {

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
  for(set<Protocol*>::const_iterator i = lid.begin(); i != lid.end(); ++i) {

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

  have_joined();
  TapDEBUG(2) << "init_state: finished adding everyone" << endl;

}

Time
Tapestry::ping( IPAddress other_node, GUID other_id, bool &ok )
{
  // if it's already in the table, don't worry about it
  if( _rt->contains( other_id ) ) {
    return _rt->get_time( other_id );
  }

  Time before = now();
  ping_args pa;
  ping_return pr;
  TapDEBUG(4) << "about to ping " << other_node << endl;
  record_stat(STAT_PING, 0, 0);
  ok = doRPC( other_node, &Tapestry::handle_ping, &pa, &pr );
  if( ok ) {
    record_stat(STAT_PING, 0, 0);
  }
  TapDEBUG(4) << "done with ping " << other_node << endl;
  return now() - before;
}

void
Tapestry::multi_add_to_rt( vector<NodeInfo *> *nodes, 
			   map<IPAddress, Time> *timing )
{
  RPCSet ping_rpcset;
  map<unsigned, ping_callinfo*> ping_resultmap;
  Time before_ping = now();
  multi_add_to_rt_start( &ping_rpcset, &ping_resultmap, nodes, timing, true );
  multi_add_to_rt_end( &ping_rpcset, &ping_resultmap, before_ping, timing, 
		       false );
}

void
Tapestry::multi_add_to_rt_start( RPCSet *ping_rpcset, 
				 map<unsigned, ping_callinfo*> *ping_resultmap,
				 vector<NodeInfo *> *nodes, 
				 map<IPAddress, Time> *timing, 
				 bool check_exist )
{
  ping_args pa;
  ping_return pr;
  for( uint j = 0; j < nodes->size(); j++ ) {
    NodeInfo *ni = (*nodes)[j];
    // do an asynchronous RPC to each one, collecting the ping times in the
    // process
    // however, no need to ping if we already know the ping time, right?
    if( !check_exist || !_rt->contains( ni->_id ) ) {
      record_stat(STAT_PING, 0, 0);
      unsigned rpc = asyncRPC( ni->_addr, 
			       &Tapestry::handle_ping, &pa, &pr );
      assert(rpc);
      (*ping_resultmap)[rpc] = New ping_callinfo(ni->_addr, 
						 ni->_id);
      ping_rpcset->insert(rpc);
      if( timing != NULL ) {
	(*timing)[ni->_addr] = 1000000;
      }
    } else if( check_exist && _rt->contains( ni->_id ) && timing != NULL ) {
      (*timing)[ni->_addr] = _rt->get_time( ni->_id );
    }
  }
}

void
Tapestry::multi_add_to_rt_end( RPCSet *ping_rpcset, 
			       map<unsigned, ping_callinfo*> *ping_resultmap,
			       Time before_ping, map<IPAddress, Time> *timing,
			       bool repair )
{
  // check for done pings
  assert( ping_rpcset->size() == ping_resultmap->size() );
  uint setsize = ping_rpcset->size();
  for( unsigned int j = 0; j < setsize; j++ ) {
    bool ok;
    unsigned donerpc = rcvRPC( ping_rpcset, ok );
    Time ping_time = now() - before_ping;
    ping_callinfo *pi = (*ping_resultmap)[donerpc];
    if( !ok ) {
      // we failed to contact this node.  remove from rt.
      pi->failed = true;
    } else {
      record_stat( STAT_PING, 0, 0 );
      // TODO: ok, but now how do I call rt->add without it placing 
      // backpointers synchronously?
      pi->rtt = ping_time;
      if( timing != NULL ) {
	(*timing)[pi->ip] = _rt->get_time( pi->id );
      }
    }
    TapDEBUG(3) << "multidone ip: " << pi->ip << " finished " << (j+1) << 
      " total " << setsize << endl;
  }
  
  // now that all the pings are done, we can add them to our routing table
  // (possibly sending synchronous backpointers around) without messing
  // up the measurements
  vector<GUID> removed;
  for(map<unsigned, ping_callinfo*>::iterator j=ping_resultmap->begin(); 
      j != ping_resultmap->end(); ++j) {
    ping_callinfo *pi = (*j).second;
    //assert( pi->rtt == ping( pi->ip, pi->id ) );
    TapDEBUG(4) << "ip: " << pi->ip << endl;
    if( pi->failed ) {
      // failed! remove!
      _rt->remove( pi->id );
      assert( !_rt->contains( pi->id ) );
      TapDEBUG(1) << "removing failed node " << pi->ip << endl;
      if( repair ) {
	  removed.push_back( pi->id );
      }
    } else {
      // make sure it's not the default (we actually pinged this one)
      assert( pi->rtt != 87654 );
      _rt->add( pi->ip, pi->id, pi->rtt );
    }
    delete pi;
  }

  // now, for each that should be repaired, ask the live nodes in its slot
  // for a new possibility and ask all live nodes in all
  // the levels above the slot.
  if( repair ) {
      RPCSet repair_rpcset;
      map<unsigned, repair_callinfo*> repair_resultmap;
      for(vector<GUID>::iterator i=removed.begin(); i != removed.end(); ++i) {

	  int match = guid_compare( id(), *i );
	  assert( match >= 0 ); // shouldn't be me
	  uint digit = get_digit( *i, match );

	  for( uint j = _digits_per_id-1; j >= (uint) match; j-- ) {
	      for( uint k = 0; k < _base; k++ ) {
		  NodeInfo *ni = _rt->read( j, k );
		  if( ni == NULL ) { 
		      continue;
		  }
		  assert( ni->_id != *i );
		  if( ni->_addr != ip() ) {
		      record_stat(STAT_REPAIR, 1, 2);
		      repair_return *rr = New repair_return();
		      repair_args *ra = New repair_args();
		      ra->bad_id = *i;
		      ra->level = (uint) match;
		      ra->digit = digit;

		      unsigned rpc = asyncRPC( ni->_addr, 
					       &Tapestry::handle_repair, 
					       ra, rr );
		      assert(rpc);
		      repair_resultmap[rpc] = New repair_callinfo( ra, rr );
		      repair_rpcset.insert(rpc);
		  }
		  delete ni;
	      }
	      // don't wrap the uint around past 0
	      if( j == 0 ) {
		break;
	      }
	  }
      }

      uint rsetsize = repair_rpcset.size();
      vector<NodeInfo *> toadd;
      for( unsigned int i = 0; i < rsetsize; i++ ) {
	  bool ok;
	  unsigned donerpc = rcvRPC( &repair_rpcset, ok );
	  repair_callinfo *rc = repair_resultmap[donerpc];
	  repair_return *rr = rc->rr;
	  if( ok ) {
	      record_stat( STAT_REPAIR, rr->nodelist.size(), 0 );
	      for( vector<NodeInfo>::iterator j=rr->nodelist.begin();
		   j != rr->nodelist.end(); j++ ) {

		  // make sure this isn't already on the toadd list, and
		  // that it wasn't removed by us
		  // TODO: this is all sorts of inefficient
		  bool add = true;
		  for( vector<GUID>::iterator k=removed.begin(); 
		       k != removed.end(); ++k) {
		      if( *k == (*j)._id ) {
			  add = false;
			  break;
		      }
		  }

		  if( add ) {
		      for( vector<NodeInfo *>::iterator k=toadd.begin(); 
			   k != toadd.end(); ++k) {
			  if( (**k)._id == (*j)._id ) {
			      add = false;
			      break;
			  }
		      }
		  }

		  if( add ) {
		      toadd.push_back( New NodeInfo( j->_addr, j->_id ) );
		  }

	      }
	  }
	  delete rr;
	  rc->rr = NULL;
	  delete rc;
      }

      multi_add_to_rt( &toadd, NULL );

      // delete
      for( uint i = 0; i < toadd.size(); i++ ) {
	delete toadd[i];
	toadd[i] = NULL;
      }
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
  record_stat(STAT_BACKPOINTER, 1, 2);
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
  RoutingTable *rt_old = _rt;
  for( unsigned int j = 0; j < setsize; j++ ) {
    bool ok;
    unsigned donerpc = rcvRPC( bp_rpcset, ok );
    if( ok ) {
      record_stat(STAT_BACKPOINTER, 0, 0);
    }
    backpointer_args *bpa = (*bp_resultmap)[donerpc];
    delete bpa;
  }
  // must have the lock again before returning
  if( _rt != rt_old ) {
    // the routing table changed while we were away (i.e. we crashed)
    // TODO: in the future, if place_backpointer_end is ever called not at
    // the end of an rt function, we might need to do something here
    TapDEBUG(3) << "rt changed while away in place_backpointer_end" << endl;
  }
}

void 
Tapestry::handle_backpointer(backpointer_args *args, backpointer_return *ret)
{
  TapDEBUG(3) << "got a bp msg from " << args->id << endl;

  TapDEBUG(5) << "bp enter" << endl;
  got_backpointer( args->ip, args->id, args->level, args->remove );
  TapDEBUG(5) << "bp exit" << endl;

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
  IPAddress *ip = New IPAddress[1];
  ip[0] = 0;
  next_hop( key, &ip, 1 );
  IPAddress rt = ip[0];
  delete ip;
  return rt;
}

void
Tapestry::next_hop( GUID key, IPAddress** ips, uint size )
{
  // first, figure out how many digits we share, and start looking at 
  // that level
  int level = guid_compare( id(), key );
  if( level == -1 ) {
    // it's us!
    if( size > 0 ) {
      (*ips)[0] = ip();
    }
    return;
  }

  RouteEntry *re = _rt->get_entry( level, get_digit( key, level ) );

  // if it has an entry, use it
  if( re != NULL && re->get_first() != NULL ) {
    // this cannot be us, since this should be the entry where we differ
    // from the key, but just to be sure . . .
     assert( re->get_first()->_addr != id() );
     for( uint i = 0; i < re->size() && i < size; i++ ) {
       (*ips)[i] = re->get_at(i)->_addr;
     }
     return;
  } else {
    // if there's no such entry, it's time to surrogate route.  yeah baby.
    // keep adding 1 to the digit until you find a match.  If it's us,
    // go up to the next level and keep on trying till the end
    // (From Fig. 3 in the JSAC paper)

    for( uint i = level; i < _digits_per_id; i++ ) {

      uint j = get_digit( key, i );
      while( re == NULL || re->get_first() == NULL ) {

	// NOTE: we can't start with looking at the j++ entry, since
	// this might be the next level up, and we'd want to start at j
	re = _rt->get_entry( i, j );

	j++;
	if( j == _base ) {
	  // think circularly
	  j = 0;
	}
      }
      TapDEBUG(4) << "looking for key " << print_guid(key) << ", level " <<
	i << ", digit " << j << " is " << print_guid(re->get_first()->_id) << 
	endl;
      // if it is us, go around another time
      // otherwise, we've found the next hop
      if( re->get_first()->_addr != ip() ) {
	for( uint k = 0; k < re->size() && k < size; k++ ) {
	  (*ips)[k] = re->get_at(k)->_addr;
	}
	return;
      }
      re = NULL;
    }
    
  }

  // didn't find a better surrogate, so it must be us
  if( size > 0 ) {
    (*ips)[0] = ip();
  }

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
  TapDEBUG(0) << "Tapestry crash" << endl;
  
  node()->crash();

  // clear out routing table, and any other state that might be lying around
  uint r = _rt->redundancy();
  delete _rt;
  joined = false;

  // clear join state also
  _joining = false;
  for( uint l = 0; l < initlist.size(); l++ ) {
    delete initlist[l];
  }
  initlist.clear();

  _rt = New RoutingTable(this, r);
  // TODO: should be killing these waiting RPCs instead of allowing them
  // to finish normally.  bah.
  _waiting_for_join->notifyAll();
  TapDEBUG(0) << "crash exit" << endl;
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

Tapestry::GUID
Tapestry::lookup_cheat( GUID key ) 
{

  // using global knowledge, figure out who the owner of this key should
  // be, given the set of live nodes
  set<Protocol*> l = Network::Instance()->getallprotocols("Tapestry");
  set<Protocol*>::iterator pos;
  vector<Tapestry::GUID> lid;

  //i only want to sort it once after all nodes have joined! 
  Tapestry *c = 0;
  int maxmatch = -2;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Tapestry *)(*pos);
    assert(c);
    // only care about live nodes
    if( c->node()->alive() ) {
      int match = guid_compare( c->id(), key );
      if( match == -1 ) {
	return c->id();
      } else if( match > maxmatch ) {
	maxmatch = match;
	lid.clear();
      }
      if( match == maxmatch ) {
	lid.push_back(c->id ());
      }
    }
  }

  // now we have a list of all the nodes that match a maximum number of digits
  // find the closest one in terms of surrogate routing.
  GUID bestmatch = lid[0];
  for( uint i = 1; i < lid.size(); i++ ) {
    GUID next = lid[i];
    TapDEBUG(4) << "comparing " << print_guid(bestmatch) << " with " <<
      print_guid( next ) << endl;
    // surrogate routing sucks.  must find the one who has a maxmatch digit
    // closest but greater than the keys.  If not, the one with the lowest
    // maxmatch digit.  For ties, go up a level and repeat
    for( uint j = 0; j < _digits_per_id - maxmatch; j++ ) {
      uint next_digit = get_digit( next, maxmatch+j );
      uint best_digit = get_digit( bestmatch, maxmatch+j );
      uint key_digit = get_digit( key, maxmatch+j );
      if( (next_digit >= key_digit && best_digit >= key_digit && 
	   next_digit < best_digit ) ||
	  (next_digit < key_digit && best_digit < key_digit &&
	   next_digit < best_digit) ||
	  (next_digit >= key_digit && best_digit < key_digit) ) {

	TapDEBUG(4) << "new best: digit " << (maxmatch+j) << " " << next_digit 
		    << " " << best_digit << " " << key_digit << endl;
	bestmatch = next;
	break;

      } else if( next_digit != best_digit ) {
	// it's not a tie, so no need to go on
	break;
      }

    }

  }

  return bestmatch;

}

//////////  RouteEntry  ///////////

RouteEntry::RouteEntry( uint redundancy )
{
  _size = 0;
  NODES_PER_ENTRY = redundancy;
  _nodes = New NodeInfo *[NODES_PER_ENTRY];
}

RouteEntry::RouteEntry( NodeInfo *first_node, uint redundancy )
{
  assert( first_node );
  NODES_PER_ENTRY = redundancy;
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

void
RouteEntry::remove_at( uint pos )
{
  assert( pos < NODES_PER_ENTRY );
  if( pos >= _size ) {
    // don't even have this guy
    return;
  } else {
    // bring everyone else up one
    uint i = pos;
    delete _nodes[i];
    for( ; i+1 < _size; i++ ) {
      _nodes[i] = _nodes[i+1];
    }
    _nodes[i] = NULL;
    _size--;
  }
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

RoutingTable::RoutingTable( Tapestry *node, uint redundancy )
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

  _redundancy = redundancy;

  // now we add ourselves to the table
  add( _node->ip(), _node->id(), 0 );

  _backpointers = New vector<NodeInfo *> *[_node->_digits_per_id];
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    _backpointers[i] = NULL;
  }

  // init the locks
  _locks = New vector<NodeInfo *> **[_node->_digits_per_id];
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    _locks[i] = New vector<NodeInfo *> *[_node->_base];
    for( uint j = 0; j < _node->_base; j++ ) {
      _locks[i][j] = NULL;
    }
  }

}

RoutingTable::~RoutingTable()
{
  TapRTDEBUG(3) << "rt destroyed" << endl;
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
      for( uint j = 0; j < _backpointers[i]->size(); j++ ) {
	delete (*_backpointers[i])[j];
      }
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
      re = New RouteEntry( new_node, _redundancy );
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

void
RoutingTable::remove( GUID id )
{
  Tapestry::RPCSet bp_rpcset;
  map<unsigned,Tapestry::backpointer_args*> bp_resultmap;

  // find the spots where it could fit and remove it
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    uint j = _node->get_digit( id, i );
    RouteEntry *re = _table[i][j];
    if( re != NULL ) {

      for( uint k = 0; k < re->size(); k++ ) {
	NodeInfo *ni = re->get_at(k);
	if( ni->_id == id ) {
	  TapRTDEBUG(3) << "going to remove id " << _node->print_guid(id) << 
	      " from " << i << "," << j << "," << k << endl;
	  re->remove_at(k);
	  break;
	  _node->place_backpointer( &bp_rpcset, &bp_resultmap, 
				    ni->_addr, i, true );
	}
      }
    }

    // if the last digit wasn't a match, we're done
    if( j != _node->get_digit( _node->id(), i ) ) {
      break;
    }
  }  

  // wait for bp rpcs to finish
  _node->place_backpointer_end( &bp_rpcset, &bp_resultmap );

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

RouteEntry *
RoutingTable::get_entry( uint i, uint j )
{
    //  TapRTDEBUG(2) << "read " << _table << endl;
  RouteEntry *re = _table[i][j];
  if( re == NULL ) {
    return NULL;
  } else {
    return re;
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
  vector<NodeInfo *> * this_level = get_backpointers(level);
  NodeInfo *new_node = New NodeInfo( ip, id );
  this_level->push_back( new_node );
}

void 
RoutingTable::remove_backpointer( IPAddress ip, GUID id, uint level )
{
  
  vector<NodeInfo *> * this_level = get_backpointers(level);
  NodeInfo new_node( ip, id );
  for( vector<NodeInfo *>::iterator i = this_level->begin(); 
       i != this_level->end(); i++ ) {
    NodeInfo curr_node = **i;
    if( curr_node == new_node ) {
      delete *i;
      this_level->erase(i);
      // only erase the first occurance
      return;
    }
  }
}

vector<NodeInfo *> *
RoutingTable::get_backpointers( uint level )
{
  vector<NodeInfo *> * this_level = _backpointers[level];
  if( this_level == NULL ) {
    this_level = New vector<NodeInfo *>;
    _backpointers[level] = this_level;
  }
  return this_level;
}

void 
RoutingTable::set_lock( IPAddress ip, GUID id )
{
  vector<NodeInfo *> * this_level = get_locks(id);
  if( this_level == NULL ) {
    // can't lock yourself
    return;
  }
  NodeInfo *new_node = New NodeInfo( ip, id );
  bool add = true;
  for(vector<NodeInfo *>::iterator i=this_level->begin(); 
      i != this_level->end(); ++i) {
    if( **i == *new_node ) {
      add = false;
    }
  }
  if( add ) {
    this_level->push_back( new_node );
    TapRTDEBUG(3) << "locks-adding " << ip << " to " << this_level << endl;
  } else {
    delete new_node;
  }
}

void 
RoutingTable::remove_lock( IPAddress ip, GUID id )
{
  vector<NodeInfo *> * this_level = get_locks(id);
  NodeInfo new_node( ip, id );
  TapRTDEBUG(3) << "locks-removed called for " << ip << endl;
  for(vector<NodeInfo *>::iterator i=this_level->begin(); 
      i != this_level->end(); ++i) {
    if( **i == new_node ) {
      delete *i;
      this_level->erase(i);
      // only erase the first occurance
      TapRTDEBUG(3) << "locks-removing " << ip << " to " << this_level << endl;
      return;
    }
  }

}

vector<NodeInfo *> *
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
  vector<NodeInfo *> * this_level = _locks[match][lastsame];
  if( this_level == NULL ) {
    this_level = New vector<NodeInfo *>;
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

  uint ones = 0;
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
      ones++;
      if( ones > 4 ) {
	break;
      }
    } else {
      ones = 0;
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

