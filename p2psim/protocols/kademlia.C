// {{{ headers
/*
 * Copyright (c) 2003 Thomer M. Gil
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

#include "kademlia.h"
#include "p2psim/network.h"
#include "p2psim/threadmanager.h"
#include <deque>
#include <iostream>
using namespace std;

bool Kademlia::docheckrep = false;
unsigned Kademlia::k = 0;
unsigned Kademlia::alpha = 0;
unsigned Kademlia::debugcounter = 1;
unsigned Kademlia::stabilize_timer = 0;
unsigned Kademlia::refresh_rate = 0;
Kademlia::k_nodeinfo_pool *Kademlia::pool = 0;

double Kademlia::_rpc_bytes = 0;
double Kademlia::_good_latency = 0;
double Kademlia::_good_hops = 0;
int Kademlia::_good_lookups = 0;
int Kademlia::_ok_failures = 0;  // # legitimate lookup failures
int Kademlia::_bad_failures = 0; // lookup failed, but node was live

set<Kademlia::NodeID> *Kademlia::_all_kademlias = 0;
hash_map<Kademlia::NodeID, Kademlia*> *Kademlia::_nodeid2kademlia = 0;

#define NODES_ITER(x) for(set<k_nodeinfo*, Kademlia::closer>::const_iterator i = (x)->begin(); i != (x)->end(); ++i)


// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(Node *n, Args a)
  : P2Protocol(n), _id(ConsistentHash::ip2chid(n->ip()))
{
  KDEBUG(1) << "id: " << printID(_id) << ", ip: " << ip() << endl;
  if(getenv("P2PSIM_CHECKREP"))
    docheckrep = strcmp(getenv("P2PSIM_CHECKREP"), "0") ? true : false;

  if(!k) {
    k = a.nget<unsigned>("k", 20, 10);
    KDEBUG(1) << "k = " << k << endl;
  }

  if(!alpha) {
    alpha = a.nget<unsigned>("alpha", 3, 10);
    KDEBUG(1) << "alpha = " << alpha << endl;
  }

  if(!stabilize_timer) {
    stabilize_timer = a.nget<unsigned>("stabilize_timer", 10000, 10);
    KDEBUG(1) << "stabilize_timer = " << stabilize_timer << endl;
  }

  if(!refresh_rate) {
    refresh_rate = a.nget<unsigned>("refresh_rate", 1000, 10);
    KDEBUG(1) << "refresh_rate = " << refresh_rate << endl;
  }

  if(!Kademlia::pool)
    Kademlia::pool = New k_nodeinfo_pool();
  _me = Kademlia::pool->pop(_id, ip());
  _joined = false;

  _root = New k_bucket(0, this);
  _root->leaf = false;

  // build the entire k-bucket tree
  k_bucket *b = _root;
  unsigned bit;
  for(unsigned i=0; ; i++) {
    bit = Kademlia::getbit(_id, i);

    // leafside
    b->child[bit^1] = New k_bucket(b, this);
    b->child[bit^1]->nodes = New k_nodes(b->child[bit^1]);
    b->child[bit^1]->replacement_cache = New set<k_nodeinfo*, Kademlia::younger>;
    b->child[bit^1]->leaf = true;

    // nodeside
    b->child[bit] = New k_bucket(b, this);
    b = b->child[bit];
    b->leaf = false;

    if(i == Kademlia::idsize-2)
      break;
  }
  b->nodes = New k_nodes(b);
  b->replacement_cache = New set<k_nodeinfo*, Kademlia::younger>;
  b->leaf = true;
}

Kademlia::NodeID Kademlia::closer::n = 0;
Kademlia::NodeID Kademlia::IDcloser::n = 0;
// }}}
// {{{ Kademlia::~Kademlia
Kademlia::~Kademlia()
{
  KDEBUG(1) << "Kademlia::~Kademlia" << endl;
  /*
  for(hash_map<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i)
    delete (*i).second;
  delete _root;
  delete _me;
  */
  if(ip() == 1){
    printf("rpc_bytes %.0f\n", _rpc_bytes);
    printf("%d good, %d ok failures, %d bad failures\n",
           _good_lookups, _ok_failures, _bad_failures);
    if(_good_lookups > 0) {
      printf("avglat %.1f avghops %.2f\n",
             _good_latency / _good_lookups,
             _good_hops / _good_lookups);
    }
  }
}
// }}}
// {{{ Kademlia::initstate
void 
Kademlia::initstate(set<Protocol*> *l)
{
  KDEBUG(1) << "Kademlia::initstate" << endl;

  if(!_all_kademlias) {
    KDEBUG(1) << "allocating all" << endl;
    _all_kademlias = New set<NodeID>;
    _nodeid2kademlia = New hash_map<NodeID, Kademlia*>;
    for(set<Protocol*>::const_iterator i = l->begin(); i != l->end(); ++i) {
      if((*i)->proto_name() != proto_name())
        continue;

      Kademlia *k = (Kademlia *) *i;
      _all_kademlias->insert(k->id());
      (*_nodeid2kademlia)[k->id()] = k;
    }
  }

  Kademlia::NodeID upper = 0, lower = 0;
  for(unsigned depth=0; depth<Kademlia::idsize; depth++) {
    upper = lower = 0;
    for(unsigned i=0; i<Kademlia::idsize; i++) {
      unsigned bit = Kademlia::getbit(_id, i);
      if(i < depth) {
        lower |= (((Kademlia::NodeID) bit) << Kademlia::idsize-i-1);
        upper |= (((Kademlia::NodeID) bit) << Kademlia::idsize-i-1);
      } else if(i == depth) {
        lower |= (((Kademlia::NodeID) bit^((Kademlia::NodeID)1)) << Kademlia::idsize-i-1);
        upper |= (((Kademlia::NodeID) bit^((Kademlia::NodeID)1)) << Kademlia::idsize-i-1);
      } else if(i > depth)
        upper |= (((Kademlia::NodeID) 1) << Kademlia::idsize-i-1);
    }

    KDEBUG(2) << " initstate lower = " << Kademlia::printbits(lower) << endl;
    KDEBUG(2) << " initstate upper = " << Kademlia::printbits(upper) << endl;

    // create a set of canditates
    vector<NodeID> candidates;
    for(set<NodeID>::const_iterator i = _all_kademlias->begin(); i != _all_kademlias->end(); ++i) {
      if(*i >= lower && *i <= upper)
        candidates.push_back(*i);
    }

    // if candidate set size is less than k, then insert them all.
    if(candidates.size() <= Kademlia::k) {
      for(vector<NodeID>::const_iterator i = candidates.begin(); i != candidates.end(); ++i) {
        Kademlia *k = (*_nodeid2kademlia)[*i];
        assert(k);
        insert(k->id(), k->node()->ip(), true);
        touch(k->id());
      }
      continue;
    }

    // if candidate set is too large, pick k at random
    unsigned count = 0;
    while(count <= Kademlia::k && candidates.size()) {
      unsigned index = random() % candidates.size();
      Kademlia *k = (*_nodeid2kademlia)[candidates[index]];
      assert(k);

      if(flyweight.find(k->id()) != flyweight.end())
        continue;

      insert(k->id(), k->node()->ip(), true);
      touch(k->id());
      count++;

      // make a copy of candidates and remove the selected index
      vector<NodeID> tmp;
      for(unsigned i=0; i<candidates.size(); i++) {
        if(i == index)
          continue;
        tmp.push_back(candidates[i]);
      }
      candidates.clear();
      candidates = tmp;
    }
  }
  _joined = true;
}

// }}}
// {{{ Kademlia::join
void
Kademlia::join(Args *args)
{
  KDEBUG(1) << "Kademlia::join" << endl;
  if(_joined)
    return;

  IPAddress wkn = args->nget<IPAddress>("wellknown");
  assert(wkn);
  assert(node()->alive());

  // I am the well-known node
  if(wkn == ip()) {
    KDEBUG(2) << "Node " << printID(_id) << " is wellknown." << endl;
    delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
    _joined = true;
    return;
  }

join_restart:
  // lookup my own key with well known node.
  lookup_args la(_id, ip(), _id);
  lookup_result lr;
  KDEBUG(2) << "join: lookup my id." << endl;
  record_stat(STAT_LOOKUP, 1, 0);
  bool b = doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  assert(b);
  record_stat(STAT_LOOKUP, lr.results.size(), 0);

  if(!node()->alive())
    return;

  // put well known node in k-buckets
  KDEBUG(2) << "join: lr.rid = " << printID(lr.rid) << endl;
  update_k_bucket(lr.rid, wkn);

  // put all nodes that wkn told us in k-buckets
  KDEBUG(2) << "join: lr.results.size() = " << lr.results.size() << endl;
  for(set<k_nodeinfo*, older>::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i) {
    KDEBUG(2) << "join: lr.results iterator id = " << printID((*i)->id) << ", ip = " << (*i)->ip << endl;
    if(flyweight.find((*i)->id) == flyweight.end() && (*i)->id != _id) {
      // XXX: the touch is WRONG.  For all we know, the node is dead.
      insert((*i)->id, (*i)->ip);
      touch((*i)->id);
    }
  }

  // get our ``successor'' and compute length
  // of prefix we have in common
  k_collect_closest getsuccessor(_id);
  _root->traverse(&getsuccessor, this);
  NodeID succ_id = *getsuccessor.results.begin();
  KDEBUG(2) << "join: succ_id is " << printID(succ_id) << endl;
  unsigned cpl = common_prefix(_id, succ_id);

  // all entries further away than him need to be refreshed.
  // see section 2.3
  for(int i=cpl-1; i>=0; i--) {
    // XXX: should be random
    lookup_args la(_id, ip(), (_id ^ (((Kademlia::NodeID) 1)<<i)));
    lookup_result lr;

    // are we dead?  bye.
    if(!node()->alive())
      return;

    // if we believe our successor died, then start again
    if(flyweight.find(succ_id) == flyweight.end()) {
      KDEBUG(2) << " restarting join" << endl;
      clear();
      goto join_restart;
    }
    
    // if the RPC failed, or the node is now dead, start over
    k_nodeinfo *ki = flyweight[succ_id];
    KDEBUG(2) << "join: iteration " << i << ", ki->id is " << printID(ki->id) << ", ip = " << ki->ip << ", cpl = " << cpl << endl;
    record_stat(STAT_LOOKUP, 1, 0);
    assert(ki->ip <= 1837);
    if(!doRPC(ki->ip, &Kademlia::do_lookup, &la, &lr)) {
      clear();
      goto join_restart;
    }
    record_stat(STAT_LOOKUP, lr.results.size(), 0);

    if(!node()->alive())
      return;

    for(set<k_nodeinfo*, older>::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
      if(flyweight.find((*i)->id) == flyweight.end() && (*i)->id != _id)
        insert((*i)->id, (*i)->ip);
        //  ... but not touch.  we didn't actually talk to the node.
  }

  _joined = true;
  delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::crash
void
Kademlia::crash(Args *args)
{
  // destroy k-buckets
  KDEBUG(1) << "Kademlia::crash" << endl;
  assert(node()->alive());
  node()->crash();
  for(hash_map<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i)
    Kademlia::pool->push((*i).second);

  // prepare for coming back up
  flyweight.clear();
  _root->collapse();
  _joined = false;
}

// }}}
// {{{ Kademlia::clear
void
Kademlia::clear()
{
  // destroy k-buckets
  KDEBUG(1) << "Kademlia::clear" << endl;
  for(hash_map<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i)
    Kademlia::pool->push((*i).second);

  flyweight.clear();
  _root->collapse();
}

// }}}
// {{{ Kademlia::lookup
//
// we assume lookups are only for node IDs, but we have to cheat to do this.
void
Kademlia::lookup(Args *args)
{
  if(!_joined)
    return;

  IPAddress key_ip = args->nget<NodeID>("key");
  // find node with this IP
  Kademlia *k = (Kademlia*) Network::Instance()->getnode(key_ip)->getproto(proto_name());
  NodeID key = k->id();

  KDEBUG(1) << "Kademlia::lookup: " << printID(key) << endl;
  assert(node()->alive());
  assert(_nodeid2kademlia->find(key) != _nodeid2kademlia->end());

  // return_immediately = true because we're doing actually FIND_VALUE
  lookup_args la(_id, ip(), key, true);
  lookup_result lr;

  Time before = now();
  do_lookup(&la, &lr);

  // XXX: this shouldn't happen :(
  if(!lr.results.size()) {
    cout << lr.log.str() << endl;
    assert(false);
  }

  // get best match
  k_nodeinfo *ki = *lr.results.begin();
  assert(lr.results.size());
  assert(ki);
  ki->checkrep();

  bool alive_and_joined = (*_nodeid2kademlia)[key]->node()->alive() &&
                          (*_nodeid2kademlia)[key]->_joined;

  // now ping that node
  ping_args pa(ki->id, ki->ip);
  ping_result pr;
  record_stat(STAT_PING, 0, 0);
  assert(ki->ip <= 1837);
  if(!doRPC(ki->ip, &Kademlia::do_ping, &pa, &pr) && node()->alive()) {
    KDEBUG(2) << "Kademlia::lookup: ping RPC to " << Kademlia::printID(ki->id) << " failed " << endl;
    if(flyweight.find(ki->id) != flyweight.end())
      erase(ki->id);
  }
  record_stat(STAT_PING, 0, 0);
  Time after = now();

  KDEBUG(0) << "Kademlia::lookup, latency " << after - before << endl;
  
  // did we find that node?
  if(key == ki->id){
    _good_lookups += 1;
    _good_latency += (after - before);
    _good_hops += lr.hops;
  } else if(alive_and_joined){
    _bad_failures += 1;
  } else {
    _ok_failures += 1;
  }

}

// }}}
// {{{ Kademlia::do_ping
void
Kademlia::do_ping(ping_args *args, ping_result *result)
{
  // put the caller in the tree, but never ourselves
  KDEBUG(1) << "Kademlia::do_ping from " << printID(args->id) << endl;
  update_k_bucket(args->id, args->ip);
}

// }}}
// {{{ Kademlia::do_lookup/find_value

#define LOG_KDEBUG(x, y) KDEBUG(x) << y;

// lresult->log << y;    

void
Kademlia::do_lookup(lookup_args *largs, lookup_result *lresult)
{
  LOG_KDEBUG(1, "Kademlia::do_lookup: node " << printbits(largs->id) << " does lookup for " << printbits(largs->key) << ", flyweight.size() = " << flyweight.size() << endl);
  assert(node()->alive());

  update_k_bucket(largs->id, largs->ip);

  // get a list of nodes sorted on distance to largs->key
  k_collect_closest successors(largs->key);
  _root->traverse(&successors, this);

  // we can't do anything but return ourselves
  closer::n = largs->key;
  if(!successors.results.size()) {
    LOG_KDEBUG(2, "do_lookup: my tree is empty; returning myself." << endl);
    lresult->results.insert(Kademlia::pool->pop(_me));
    lresult->rid = _id;
    return;
  }

  // initialize result set with the successors of largs->key
  closer::n = largs->key;
  for(set<NodeID, IDcloser>::const_iterator i = successors.results.begin(); i != successors.results.end(); ++i) {
    assert(flyweight.find(*i) != flyweight.end());
    k_nodeinfo *ki = flyweight[*i];
    assert(ki);
    k_nodeinfo *newki = Kademlia::pool->pop(ki);
    assert(newki);
    LOG_KDEBUG(2, "do_lookup: initializing resultset, adding = " << Kademlia::printbits(newki->id) << ", dist = " << printbits(distance(newki->id, largs->key)) << endl);
    lresult->results.insert(newki);
  }

  // also insert ourselves.  this can't really hurt, because it will be thrown
  // out of the set anyway, if it wasn't part of the answer.  this avoids
  // returning an empty set.
  k_nodeinfo *me = Kademlia::pool->pop(_me);
  LOG_KDEBUG(2, "insert me = " << Kademlia::printbits(_me->id) << " to be safe " << endl);
  lresult->results.insert(me);

  // destroy all entries in results that are not part of the best k
  while(lresult->results.size() > Kademlia::k) {
    k_nodeinfo *i = *lresult->results.rbegin();
    lresult->results.erase(lresult->results.find(i));
    LOG_KDEBUG(2, "do_lookup: deleting in truncate id = " << printbits(i->id) << ", ip = " << i->ip << endl);
    Kademlia::pool->push(i);
  }

  // asked: one entry for each node we sent an RPC to
  // replied: one entry for each node we got a reply from
  // outstanding_rpcs: keyed by rpc-token, gets all info associated with RPC
  // rpcset: set of outstanding rpcs
  // last_alpha_round: contains the IDs of the nodes we're RPCing in this round
  //                   of alpha RPCs
  hash_map<NodeID, bool> asked;
  hash_map<NodeID, bool> replied;
  hash_map<unsigned, callinfo*> *outstanding_rpcs = New hash_map<unsigned, callinfo*>;
  assert(outstanding_rpcs);
  RPCSet *rpcset = New RPCSet;
  assert(rpcset);
  set<NodeID> last_alpha_round;
  last_alpha_round.clear();

  //
  // - (for FIND_VALUE:) stop if the best result is, in fact, the key we're
  //   looking for.
  // - stop if we've had an answer from the best k nodes in the result set
  // - find best alpha unqueried nodes in result set
  // - send an RPC to those alpha nodes
  // - wait for an RPC to come back
  // - update results set
  //
  NodeID last_in_set = 0;
  while(true) {
    LOG_KDEBUG(2, "do_lookup: top of the loop. outstaning rpcs = " << outstanding_rpcs->size() << endl);

    // stop if we've had an answer from the best k nodes we know of.
    // also mark the fact whether we asked everyone already
    unsigned k_counter = 0;
    bool we_are_done = false;
    bool asked_all = true;

    closer::n = largs->key;
    assert(lresult->results.size() <= Kademlia::k);
    for(set<k_nodeinfo*, closer>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i) {
      LOG_KDEBUG(2, "do_lookup: finished? looking at " << printbits((*i)->id) << endl);

      // asked_all has to be false if there's any node in the resultset that we
      // haven't queried yet
      if(asked.find((*i)->id) == replied.end())
        asked_all = false;

      // Did we find the key?
      //
      // If we bail out here, we're actually implementing Kademlia's FIND_VALUE,
      // not LOOKUP, which is indicated by largs->return_immediately.
      if((*i)->id == largs->key && largs->return_immediately) {
        we_are_done = true;
        break;
      }

      if(replied.find((*i)->id) == replied.end()) {
        LOG_KDEBUG(2, "do_lookup: finished? haven't gotten a reply from " << printbits((*i)->id) << " yet, so no." << endl);
        break;
      }

      if(++k_counter >= Kademlia::k) {
        LOG_KDEBUG(2, "do_lookup: finished? we've reached Kademlia::k.  so, yes." << endl);
        we_are_done = true;
        break;
      }
    }

    // there are no more outstanding RPCs and we asked everyone we could have
    // asked.  there's simply nothing else we can do.
    if(!outstanding_rpcs->size() && asked_all) {
      delete rpcset;
      delete outstanding_rpcs;
      break;
    }

    // we_are_done: we've had an answer from the best k nodes we know of.
    // reap outstanding RPCs if necessary
    if(we_are_done) {
      if(outstanding_rpcs->size()) {
        reap_info *ri = New reap_info();
        ri->k = this;
        ri->rpcset = rpcset;
        ri->outstanding_rpcs = outstanding_rpcs;
        LOG_KDEBUG(2, "do_lookup: we_are_done, reaper has the following crap." << endl);
        for(hash_map<unsigned, callinfo*>::const_iterator i = outstanding_rpcs->begin(); i != outstanding_rpcs->end(); ++i) {
          LOG_KDEBUG(2, "do_lookup: callinfo->ki->id = " << printbits(i->second->ki->id) << endl);
        }
        ThreadManager::Instance()->create(Kademlia::reap, (void*) ri);
      }
      break;
    }


    //
    // did we get a reply from all the nodes we RPCd in
    // the last round of alpha RPCs?
    //
    bool last_alphas_replied = true;
    for(set<NodeID>::const_iterator i = last_alpha_round.begin(); i != last_alpha_round.end(); ++i)
      if(replied.find(*i) == replied.end()) {
        last_alphas_replied = false;
        break;
      }


    // did the last round of alpha nodes not reveal anything new?
    // blast out RPCs to all remaining nodes
    //
    // lresult->results.size() is too high for effective_alpha, since some of
    // them we RPCd already, but it doesn't really matter.  we're only going to
    // send as many RPCs as there are unqueried nodes
    unsigned effective_alpha = Kademlia::alpha;
    if((*lresult->results.rbegin())->id == last_in_set && last_alphas_replied) {
      LOG_KDEBUG(2, "do_lookup: all last alpha replied and that didn't change anything.  setting effective_alpha." << endl);
      effective_alpha = lresult->results.size();
    }


    //
    // pick effective_alpha nodes from the k best guys in the resultset,
    // provided we didn't already ask them, and, to the best of our knowledge,
    // they're still alive.
    //
    k_nodeinfo* toask[effective_alpha];
    for(unsigned i=0; i<effective_alpha; i++)
      toask[i] = 0;

    k_counter = 0;
    unsigned j = 0;
    for(set<k_nodeinfo*, closer>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i) {
      if(asked.find((*i)->id) == asked.end()) {
        LOG_KDEBUG(2, "do_lookup: setting toask[" << j << "] to " << printbits((*i)->id) << endl);
        toask[j++] = *i;
      }
      if(++k_counter >= Kademlia::k || j >= effective_alpha)
        break;
    };


    //
    // send an RPC to all selected alpha nodes
    //
    last_alpha_round.clear();
    for(unsigned i=0; i<effective_alpha; i++) {
      if(toask[i] == 0)
        break;

      LOG_KDEBUG(2, "do_lookup: toask[ " << i << "] id = " << printbits(toask[i]->id) << ", ip = " << toask[i]->ip << endl);


      closer::n = largs->key;
      lookup_args *la = New lookup_args(_id, ip(), largs->key);
      lookup_result *lr = New lookup_result;
      assert(la && lr);
      assert(toask[i]);
      // assert(toask[i]->ip <= 1024 && toask[i]->ip > 0);

      record_stat(STAT_LOOKUP, 1, 0);
      LOG_KDEBUG(2, "do_lookup: asyncRPC to " << printbits(toask[i]->id) << ", ip = " << toask[i]->ip << endl);
      assert(toask[i]->ip <= 1837);
      unsigned rpc = asyncRPC(toask[i]->ip, &Kademlia::find_node, la, lr);
      callinfo *ci = New callinfo(Kademlia::pool->pop(toask[i]), la, lr);
      assert(ci);
      assert(rpc);
      rpcset->insert(rpc);
      asked[toask[i]->id] = true;
      last_alpha_round.insert(toask[i]->id);

      // record this outstanding RPC
      (*outstanding_rpcs)[rpc] = ci;
    }

    // this was one more hop
    lresult->hops++;

    LOG_KDEBUG(2, "do_lookup: results before rcvRPC loop" << endl);
    closer::n = largs->key;
    for(set<k_nodeinfo*, closer>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i)
      LOG_KDEBUG(2, "do_lookup: id = " << printbits((*i)->id) << ", dist = " << printbits(distance(largs->key, (*i)->id)) << ", ip = " << (*i)->ip << endl);

    //
    // wait for all alpha RPCs to get back
    //
    last_in_set = (*lresult->results.rbegin())->id;
    while(outstanding_rpcs->size()) {
      LOG_KDEBUG(2, "do_lookup: thread " << threadid() << " going into rcvRPC, outstanding = " << outstanding_rpcs->size() << endl);

      bool ok;
      unsigned donerpc = rcvRPC(rpcset, ok);
      callinfo *ci = (*outstanding_rpcs)[donerpc];
      assert(ci);
      replied[ci->ki->id] = true;
      outstanding_rpcs->erase(outstanding_rpcs->find(donerpc));

      // if the node is dead, then remove this guy from our flyweight and, if he
      // was in our results, from the results.  since we didn't make any
      // progress at all, we should give rcvRPC another chance.
      closer::n = largs->key;
      if(!ok) {
        LOG_KDEBUG(2, "do_lookup: RPC to " << Kademlia::printbits(ci->ki->id) << " failed" << endl);
        if(flyweight.find(ci->ki->id) != flyweight.end())
          erase(ci->ki->id);
        if(lresult->results.find(ci->ki) != lresult->results.end())
          lresult->results.erase(lresult->results.find(ci->ki));
        LOG_KDEBUG(2, "do_lookup: RPC to " << Kademlia::printbits(ci->ki->id) << " failed" << endl);
        delete ci;
        continue;
      }

      //
      // RPC was ok
      //
      LOG_KDEBUG(2, "do_lookup: " << Kademlia::printbits(ci->ki->id) << " replied for lookup " << printbits(largs->key) << endl);
      record_stat(STAT_LOOKUP, ci->lr->results.size(), 0);
      update_k_bucket(ci->lr->rid, ci->ki->ip);

      // put results that this node tells us in our results set
      closer::n = largs->key;
      for(set<k_nodeinfo*, older>::const_iterator i = ci->lr->results.begin(); i != ci->lr->results.end(); ++i) {
        LOG_KDEBUG(2, "do_lookup: RETURNED RESULT entry id = " << printbits((*i)->id) << ", dist = " << printbits(distance(largs->key, (*i)->id)) << ", ip = " << (*i)->ip << endl);
        k_nodeinfo *n = Kademlia::pool->pop(*i);
        assert(n);
        pair<set<k_nodeinfo*, closer>::iterator, bool> rv = lresult->results.insert(n);
        if(!rv.second)
          Kademlia::pool->push(n);
      }
      delete ci;

      // destroy all entries in results that are not part of the best k
      while(lresult->results.size() > Kademlia::k) {
        k_nodeinfo *i = *lresult->results.rbegin();
        lresult->results.erase(lresult->results.find(i));
        LOG_KDEBUG(2, "do_lookup: deleting in truncate id = " << printbits(i->id) << ", ip = " << i->ip << endl);
        Kademlia::pool->push(i);
      }

      // little improvement on Kademlia!  we don't have to wait for the
      // remaining RPCs if the resultset already changed.  we can break out
      // immediately
      if((*lresult->results.rbegin())->id != last_in_set) {
        LOG_KDEBUG(2, "do_lookup: this RPC caused last to change.  breaking out." << endl);
        break;
      }
      LOG_KDEBUG(2, "do_lookup: this RPC did NOT cause last to change.  NOT breaking out." << endl);
    }
  }

  LOG_KDEBUG(2, "do_lookup: results that I'm returning to " << printbits(largs->id) << " who was looking key " << printbits(largs->key) << endl);
  NODES_ITER(&lresult->results) {
    LOG_KDEBUG(2, "do_lookup: result: id = " << printbits((*i)->id) << ", lastts = " << (*i)->lastts << ", ip = " << (*i)->ip << endl);
  }

  // put ourselves as replier
  lresult->rid = _id;
}

// }}}
// {{{ Kademlia::find_node
// Kademlia's FIND_NODE.  Returns the best k from its own k-buckets
void
Kademlia::find_node(lookup_args *largs, lookup_result *lresult)
{
  KDEBUG(2) << "find_node invoked by " << printID(largs->id) << ", looking for " << printID(largs->key) << ", calling thread = " << largs->tid << ", inserting reply into lresult " << endl;
  assert(node()->alive());

  update_k_bucket(largs->id, largs->ip);
  lresult->rid = _id;


  closer::n = largs->key;

  // deal with the empty case
  if(!flyweight.size()) {
    k_nodeinfo *p = Kademlia::pool->pop(_id, ip());
    assert(p);
    KDEBUG(2) << "find_node: tree is empty. returning myself, ip = " << ip() << endl;
    pair<set<k_nodeinfo*, closer>::iterator, bool> rv = lresult->results.insert(p);
    if(!rv.second)
      Kademlia::pool->push(p);
    return;
  }

  //
  _root->find_node(largs->key, &lresult->results, 0);

  KDEBUG(2) << "find_node: returning:" << endl;
  NODES_ITER(&lresult->results) {
    KDEBUG(2) << "find_node: id = " << Kademlia::printID((*i)->id) << ", dist = " << Kademlia::printID(distance(largs->key, (*i)->id)) << endl;
  }
}


// }}}
// {{{ Kademlia::reschedule_stabilizer
void
Kademlia::reschedule_stabilizer(void *x)
{
  KDEBUG(1) << "Kademlia::reschedule_stabilizer" << endl;
  if(!node()->alive()) {
    KDEBUG(2) << "Kademlia::reschedule_stabilizer returning because I'm dead." << endl;
    return;
  }
  stabilize();
  delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::stabilize
void 
Kademlia::stabilize()
{
  KDEBUG(1) << "Kademlia::stabilize" << endl;
  assert(node()->alive());

  if(Kademlia::docheckrep) {
    k_check check;
    _root->traverse(&check, this);
  }

  // stabilize
  k_stabilizer stab;
  _root->traverse(&stab, this);

  // tell the observers.  maybe they're happy now.
  notifyObservers();
}

// }}}
// {{{ Kademlia::stabilized
bool
Kademlia::stabilized(vector<NodeID> *lid)
{
  // remove ourselves from lid
  vector<NodeID> copylid;
  for(vector<NodeID>::const_iterator i = lid->begin(); i != lid->end(); ++i)
    if(*i != id())
      copylid.push_back(*i);

  k_stabilized stab(&copylid);
  _root->traverse(&stab, this);
  return stab.stabilized();
}

// }}}
// {{{ Kademlia::getbit
//
// Returns the i-th bit in n.  0 is the most significant bit.
//
inline
unsigned
Kademlia::getbit(NodeID n, unsigned i)
{
  return (n & (((NodeID) 1)<<((sizeof(NodeID)*8)-i-1))) ? 1 : 0;
}

// }}}
// {{{ Kademlia::touch
// pre: id is in the flyweight and id is in the tree
// post: lastts for id is updated, update propagated to k-bucket.  if this is
//    the first touch, then firstts is updated as well
void
Kademlia::touch(NodeID id)
{
  // KDEBUG(1) << "Kademlia::touch " << Kademlia::printID(id) << endl;

  assert(id);
  assert(flyweight.find(id) != flyweight.end());

  // if(Kademlia::docheckrep) {
  //   k_finder find(id);
  //   _root->traverse(&find, this);
  //   assert(find.found() == 1);
  // }

  if(!flyweight[id]->firstts)
    flyweight[id]->firstts = now();
  _root->insert(id, true);
}

// }}}
// {{{ Kademlia::insert
// pre: id and ip are valid, id is not yet in this flyweight
// post: id->ip mapping in flyweight, and k-bucket
//
void
Kademlia::insert(NodeID id, IPAddress ip, bool init_state)
{
  KDEBUG(1) << "Kademlia::insert " << Kademlia::printID(id) << ", ip = " << ip << endl;

  assert(id && ip);
  assert(flyweight.find(id) == flyweight.end());

  k_nodeinfo *ni = Kademlia::pool->pop(id, ip);
  KDEBUG(2) << "Kademlia::insert " << Kademlia::printID(id) << ", ip = " << ip << endl;
  assert(ni);
  flyweight[id] = ni;
  _root->insert(id, false, init_state);
}
// }}}
// {{{ Kademlia::erase
void
Kademlia::erase(NodeID id)
{
  KDEBUG(1) << "Kademlia::erase " << Kademlia::printID(id) << endl;

  assert(flyweight.find(id) != flyweight.end());
  _root->erase(id);
  KDEBUG(2) << "Kademlia::erase deleting id = " << printID(id) << ", ip = " << flyweight[id]->ip << endl;
  Kademlia::pool->push(flyweight[id]);
  flyweight.erase(flyweight.find(id));
}
// }}}
// {{{ Kademlia::update_k_bucket
inline void
Kademlia::update_k_bucket(NodeID id, IPAddress ip)
{
  // update k-bucket
  if(id == _id)
    return;

  if(flyweight.find(id) == flyweight.end())
    insert(id, ip);
  touch(id);
}
// }}}
// {{{ Kademlia::common_prefix
unsigned
Kademlia::common_prefix(Kademlia::NodeID k1, Kademlia::NodeID k2)
{
  unsigned size = 0;
  for(unsigned i=0; i<idsize; i++)
    if(getbit(k1, i) == getbit(k2, i))
      size++;
    else
      break;
  return size;
}

// }}}
// {{{ Kademlia::record_stat
void
Kademlia::record_stat(stat_type type, uint num_ids, uint num_else )
{
  _rpc_bytes += 20 + num_ids * 4 + num_else; // paper says 40 bytes per node entry

  // assert(stat.size() > (unsigned) type);
  // stat[type] += 20 + 4*num_ids + num_else;
  // num_msgs[type]++;
}
// }}}
// {{{ Kademlia::distance
Kademlia::NodeID
Kademlia::distance(Kademlia::NodeID from, Kademlia::NodeID to)
{
  return from ^ to;
}

// }}}
// {{{ Kademlia::printbits
string
Kademlia::printbits(NodeID id)
{
  char buf[128];

  unsigned j=0;
  for(int i=idsize-1; i>=0; i--)
    sprintf(&(buf[j++]), "%llu", (id >> i) & 0x1);
  sprintf(&(buf[j]), ":%llx", id);

  return string(buf);
}

// }}}
// {{{ Kademlia::printID
string
Kademlia::printID(NodeID id)
{
  char buf[128];
  sprintf(buf, "%llx", id);
  return string(buf);
}

// }}}
// {{{ Kademlia::reap
// reaps outstanding RPCs, but snatches out the info that's contained in it.
// I.e., we update k-buckets.
void
Kademlia::reap(void *r)
{
  reap_info *ri = (reap_info *) r;
  NodeID _id = ri->k->id();

  KDEBUG(1) << "Kademlia::reap" << endl;
  assert(ri->rpcset->size() == ri->outstanding_rpcs->size());

  while(ri->outstanding_rpcs->size()) {
    bool ok;
    unsigned donerpc = ri->k->rcvRPC(ri->rpcset, ok);
    callinfo *ci = (*ri->outstanding_rpcs)[donerpc];
    assert(ci);

    KDEBUG(2) << "Kademlia::reap ok = " << ok << ", ki = " << endl;
    if(ok) {
      ri->k->update_k_bucket(ci->lr->rid, ci->ki->ip);
      ri->k->record_stat(STAT_LOOKUP, ci->lr->results.size(), 0);
    } else if(ri->k->flyweight.find(ci->ki->id) != ri->k->flyweight.end())
      ri->k->erase(ci->ki->id);

    ri->outstanding_rpcs->erase(ri->outstanding_rpcs->find(donerpc));
    delete ci;
  }

  delete ri;
  threadexits(0);
}
// }}}

// {{{ k_nodes::k_nodes
k_nodes::k_nodes(k_bucket *parent) : _parent(parent)
{
  assert(_parent);
  nodes.clear();
}
// }}}
// {{{ k_nodes::~k_nodes
k_nodes::~k_nodes()
{
}
// }}}
// {{{ k_nodes::insert
/*
 * pre: n is in the flyweight.
 * post: if n was contained in the set, its lastts is updated and
 *              its position in the set is updated.
 *       if it wasn't in the set, it is inserted.
 */
void
k_nodes::insert(Kademlia::NodeID n, bool touch)
{
  Kademlia::NodeID _id = _parent->kademlia()->id();
  // KDEBUG(1) << "k_nodes::insert " << Kademlia::printID(n) << endl;

  checkrep(false);

  assert(n);
  assert(_parent->kademlia()->flyweight.find(n) != _parent->kademlia()->flyweight.end());
  k_nodeinfo *ninfo = _parent->kademlia()->flyweight[n];
  assert(ninfo);
  // ninfo->checkrep();

  // if already in set, and we're not going to touch the timestamp, we're done.
  nodeset_t::iterator pos = nodes.find(ninfo);
  if(pos != nodes.end() && !touch)
    return;

  // if already in set: remove it since its position will change. then update
  // timestamp.
  if(pos != nodes.end() && touch) {
    // KDEBUG(2) << "k_nodes::insert found " << Kademlia::printID(n) << ", removing..." << endl;
    nodes.erase(pos);
    _nodes_by_id.erase(_nodes_by_id.find(ninfo));
    ninfo->lastts = now();
  }

  // insert
  if(!(nodes.insert(ninfo).second && _nodes_by_id.insert(ninfo).second))
    assert(false);

  // truncate end
  if(nodes.size() > Kademlia::k) {
    k_nodeinfo *ninfo = *nodes.rbegin();
    KDEBUG(2) << "k_nodes::insert truncating " << Kademlia::printID(ninfo->id) << endl;
    nodes.erase(nodes.find(ninfo));
    _nodes_by_id.erase(_nodes_by_id.find(ninfo));
    // dump it in the replacement cache, rather than completely throwing it
    // away. but don't let replacement_cache get too large.
    if(_parent->replacement_cache->size() < Kademlia::k)
      _parent->replacement_cache->insert(ninfo);
    else {
      KDEBUG(2) << "k_nodes::insert truncating " << Kademlia::printID(ninfo->id) << endl;
      _parent->kademlia()->flyweight.erase(_parent->kademlia()->flyweight.find(ninfo->id));
      Kademlia::pool->push(ninfo);
    }
  }

  checkrep(false);
}
// }}}
// {{{ k_nodes::clear
void
k_nodes::clear()
{
  nodes.clear();
  _nodes_by_id.clear();
}
// }}}
// {{{ k_nodes::erase
/*
 * pre: n is in the flyweight. n is contained in this k_nodes.
 * post: n is erased from this k_nodes
 */
void
k_nodes::erase(Kademlia::NodeID n)
{
  checkrep();
  assert(n);
  assert(_parent->kademlia()->flyweight.find(n) != _parent->kademlia()->flyweight.end());
  k_nodeinfo *ninfo = _parent->kademlia()->flyweight[n];
  ninfo->checkrep();
  assert(nodes.find(ninfo) != nodes.end() || _parent->replacement_cache->find(ninfo) != _parent->replacement_cache->end());
  assert(_nodes_by_id.find(ninfo) != nodes.end());

  nodes.erase(nodes.find(ninfo));
  _nodes_by_id.erase(_nodes_by_id.find(ninfo));

  checkrep();
}
// }}}
// {{{ k_nodes::contains
/*
 * pre: n is in the flyweight. n is contained in this k_nodes.
 * returns: whether or not this NodeID is in this set
 */
bool
k_nodes::contains(Kademlia::NodeID id) const
{
  checkrep();
  assert(id);
  assert(_parent->kademlia()->flyweight.find(id) != _parent->kademlia()->flyweight.end());
  k_nodeinfo *kinfo = _parent->kademlia()->flyweight[id];
  assert(kinfo);
  return (nodes.find(kinfo) != nodes.end());
}
// }}}
// {{{ k_nodes::checkrep
void
k_nodes::checkrep(bool rangecheck) const
{
  if(!Kademlia::docheckrep)
    return;

  assert(_parent);
  assert(_parent->nodes == this);
  assert(nodes.size() <= Kademlia::k);
  assert(nodes.size() == _nodes_by_id.size());
  assert(_parent->kademlia());
  assert(_parent->kademlia()->id());

  // own ID should never be in flyweight
  assert(_parent->kademlia()->flyweight.find(_parent->kademlia()->id()) == _parent->kademlia()->flyweight.end());

  // all nodes are in flyweight
  // all nodes are unique
  // all nodes are also in _nodes_by_id
  hash_map<Kademlia::NodeID, bool> haveseen;
  NODES_ITER(&nodes) {
    assert(_parent->kademlia()->flyweight.find((*i)->id) != _parent->kademlia()->flyweight.end());
    assert(haveseen.find((*i)->id) == haveseen.end());
    assert(_nodes_by_id.find(*i) != _nodes_by_id.end());
    haveseen[(*i)->id] = true;
  }

  // all nodes in _nodes_by_id are also in nodes
  haveseen.clear();
  for(set<k_nodeinfo*, Kademlia::idless>::const_iterator i = _nodes_by_id.begin(); i != _nodes_by_id.end(); ++i) {
    assert(nodes.find(*i) != nodes.end());
    assert(haveseen.find((*i)->id) == haveseen.end());
    haveseen[(*i)->id] = true;
  }

  if(!nodes.size())
    return;

  if(nodes.size() == 1) {
    k_nodeinfo *first = *nodes.begin();
    k_nodeinfo *last = *nodes.rbegin();
    assert(first == last);
    assert(first->id == last->id);
    return;
  }

  // own ID should never be in the range of values.
  // we would have returned already if size() <= 1
  // so first and last must be different.
  if(rangecheck) {
    k_nodeinfo *first = *_nodes_by_id.begin();
    k_nodeinfo *last = *_nodes_by_id.rbegin();
    assert(first != last);
    assert(!(_parent->kademlia()->id() >= first->id && _parent->kademlia()->id() <= last->id));
  }

  // oldest should be at the head, youngest at the tail
  // prev = first, cur = second
  set<k_nodeinfo*, Kademlia::older>::const_iterator prev, cur;
  cur = prev = nodes.begin();
  cur++;
  while(cur != nodes.end()) {
    assert((*prev)->lastts <= (*cur)->lastts);
    prev = cur++;
  }
}
// }}}

// {{{ k_nodeinfo::k_nodeinfo
k_nodeinfo::k_nodeinfo(NodeID id, IPAddress ip) : id(id), ip(ip)
{
  firstts = lastts = 0;
  checkrep();
}
// }}}
// {{{ k_nodeinfo::k_nodeinfo
k_nodeinfo::k_nodeinfo(k_nodeinfo *k) : id(k->id), ip(k->ip), firstts(k->firstts), lastts(k->lastts)
{
  checkrep();
}
// }}}
// {{{ k_nodeinfo::checkrep
void
k_nodeinfo::checkrep() const
{
  if(!Kademlia::docheckrep)
    return;

  assert(id);
  assert(ip);
  if(firstts > lastts)
    cout << "id = " << Kademlia::printID(id) << " firstts = " << firstts << " lastts = " << lastts << endl;
  assert(firstts <= lastts);
}
// }}}

// {{{ k_bucket::k_bucket
k_bucket::k_bucket(k_bucket *parent, Kademlia *k) : parent(parent), _kademlia(k)
{
  child[0] = child[1] = 0;
  nodes = 0;
  replacement_cache = 0;
  leaf = false;
}
// }}}
// {{{ k_bucket::~k_bucket
k_bucket::~k_bucket()
{
  if(leaf) {
    delete nodes;
    delete replacement_cache;
    return;
  }

  delete child[0];
  delete child[1];
}
// }}}
// {{{ k_bucket::traverse
void
k_bucket::traverse(k_traverser *traverser, Kademlia *k, string prefix, unsigned depth, unsigned leftright)
{
  Kademlia::NodeID _id = kademlia()->id();
  // if(!depth)
  KDEBUG(1) << "k_bucket::traverser for " << traverser->type() << ", prefix = " << prefix << endl;
  checkrep();

  if(!leaf) {
    if(!k->node()->alive() || !child[0])
      return;
    child[0]->traverse(traverser, k, prefix + "0", depth+1, 0);
    if(!k->node()->alive() || !child[1] || leaf)
      return;
    child[1]->traverse(traverser, k, prefix + "1", depth+1, 1);
    if(!k->node()->alive())
      return;
    checkrep();
    return;
  }


  if(!k->node()->alive())
    return;

  // we're a leaf
  traverser->execute(this, prefix, depth, leftright);

  checkrep();
}
// }}}
// {{{ k_bucket::find_node
void
k_bucket::find_node(Kademlia::NodeID key, set<k_nodeinfo*, Kademlia::closer> *v, unsigned depth)
{
  checkrep();

  Kademlia::NodeID _id = kademlia()->id();

  // recurse deeper in the right direction if we can
  if(!leaf) {
    unsigned leftmostbit = Kademlia::getbit(key, depth);
    KDEBUG(1) << "k_bucket::insert heading towards " << leftmostbit << endl;
    if(v->size() < Kademlia::k)
      child[leftmostbit]->find_node(key, v, depth+1);
    if(v->size() < Kademlia::k)
      child[leftmostbit^1]->find_node(key, v, depth+1);
    checkrep();
    return;
  }

  // collect stuff from the k-bucket itself
  NODES_ITER(&nodes->nodes) {
    pair<set<k_nodeinfo*, Kademlia::closer>::iterator, bool> rv = v->insert(Kademlia::pool->pop(*i));
    assert(rv.second);
    if(v->size() >= Kademlia::k) {
      checkrep();
      return;
    }
  }

  // XXX: collect stuff from replacement cache.  IS THIS OK?
  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = replacement_cache->begin(); i != replacement_cache->end(); ++i) {
    pair<set<k_nodeinfo*, Kademlia::closer>::iterator, bool> rv = v->insert(Kademlia::pool->pop(*i));
    assert(rv.second);
    if(v->size() >= Kademlia::k) {
      checkrep();
      return;
    }
  }

  checkrep();
}
// }}}
// {{{ k_bucket::insert
// pre: id is in flyweight, its timestamp (lastts) has been set.
// post: if id was already in k-bucket, its position is updated.
//       if id was not in k-bucket :
//              if k-bucket has space, id is added
//              if k-bucket is full, id is put in replacement cache
void
k_bucket::insert(Kademlia::NodeID id, bool touch, bool init_state, string prefix, unsigned depth)
{
  checkrep();
  assert(kademlia()->flyweight.find(id) != kademlia()->flyweight.end());

  // for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();

  KDEBUG(1) << "k_bucket::insert " << Kademlia::printbits(id) << ", prefix = " << prefix << endl;

  // recurse deeper if we can
  if(!leaf) {
    unsigned leftmostbit = Kademlia::getbit(id, depth);
    KDEBUG(1) << "k_bucket::insert heading towards " << leftmostbit << endl;
    child[leftmostbit]->insert(id, touch, init_state, prefix + (leftmostbit ? "1" : "0"), depth+1);
    checkrep();
    return;
  }

  // if not in k-bucket yet, or there's more space: insert it.
  if(nodes->contains(id) || !nodes->full()) {
    nodes->insert(id, touch);
    nodes->checkrep(false);
    return;
  }

  // we're full.  put in replacement_cache, remove old entry first.
  if(replacement_cache->size() < Kademlia::k) {
    k_nodeinfo *kinfo = kademlia()->flyweight[id];
    if(replacement_cache->find(kinfo) != replacement_cache->end())
      replacement_cache->erase(replacement_cache->find(kinfo));
    kinfo->lastts = now();
    replacement_cache->insert(kinfo);
  }

  checkrep();
}
// }}}
// {{{ k_bucket::erase
void
k_bucket::erase(Kademlia::NodeID id, string prefix, unsigned depth)
{
  checkrep();

  // recurse deeper
  if(!leaf) {
    unsigned leftmostbit = Kademlia::getbit(id, depth);
    child[leftmostbit]->erase(id, prefix + (leftmostbit ? "1" : "0"), depth+1);
    checkrep();
    return;
  }

  // if not in k-bucket itself, it must be in replacement_cache.  remove from
  // there.
  if(!nodes->contains(id)) {
    k_nodeinfo *ki = kademlia()->flyweight[id];
    set<k_nodeinfo*, Kademlia::younger>::const_iterator pos = replacement_cache->find(ki);
    if(pos != replacement_cache->end())
      replacement_cache->erase(replacement_cache->find(ki));
    checkrep();
    return;
  }

  // remove from k-bucket
  nodes->erase(id);

  // get latest from replacement_cache to nodes
  if(!replacement_cache->size()) {
    checkrep();
    return;
  }

  k_nodeinfo *ki = *(replacement_cache->begin());
  nodes->insert(ki->id, false);
  replacement_cache->erase(replacement_cache->find(ki));
  checkrep();
}
// }}}
// {{{ k_bucket::collapse
// transform back from node to leaf
void
k_bucket::collapse()
{
  Kademlia::NodeID _id = kademlia()->id();
  KDEBUG(2) << "k_bucket::collapse" << endl;
  checkrep();

  if(!leaf) {
    child[0]->collapse();
    child[1]->collapse();
  } else {
    nodes->clear();
    replacement_cache->clear();
  }

  checkrep();
}
// }}}
// {{{ k_bucket::checkrep
void
k_bucket::checkrep()
{
  if(!Kademlia::docheckrep)
    return;

  assert(_kademlia);

  if(!parent)
    assert(_kademlia->root() == this);
  else
    assert(!parent->leaf);

  if(!leaf) {
    assert(child[0] && child[1]);
    return;
  }


  // checkreps for leaf
  assert(nodes);
  assert(replacement_cache);
  nodes->checkrep();

  if(!replacement_cache->size())
    return;

  // all entrues are unique
  hash_map<Kademlia::NodeID, bool> haveseen;
  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = replacement_cache->begin(); i != replacement_cache->end(); ++i) {
    assert(haveseen.find((*i)->id) == haveseen.end());
    haveseen[(*i)->id] = true;
  }

  // no doubles in replacement_cache, none of them should be in nodes either,
  // and all entries should be sorted youngest first.
  set<k_nodeinfo*, Kademlia::younger>::const_iterator prev, cur;
  cur = prev = replacement_cache->begin();
  assert(!nodes->contains((*cur)->id));
  cur++;
  while(cur != replacement_cache->end()) {
    assert((*prev)->lastts >= (*cur)->lastts);
    assert(!nodes->contains((*cur)->id));
    prev = cur++;
  }

  // make sure every node in replacement_cache is referring to a data structure
  // in OUR flyweight
  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = replacement_cache->begin(); i != replacement_cache->end(); ++i) {
    bool found = false;
    for(hash_map<Kademlia::NodeID, k_nodeinfo*>::const_iterator j = kademlia()->flyweight.begin(); j != kademlia()->flyweight.end(); ++j)
      if(j->second == *i) {
        found = true;
        break;
      }
    assert(found);
  }
}
// }}}

// {{{ k_stabilizer::execute
void
k_stabilizer::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  // for KDEBUG purposes
  Kademlia *mykademlia = k->kademlia();
  Kademlia::NodeID _id = mykademlia->id();

  if(!k->leaf || !mykademlia->node()->alive())
    return;

  // return if any entry in this k-bucket is fresh
  for(set<k_nodeinfo*, Kademlia::older>::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
    // XXX: this is the sign of bad shit going on.  get out.
    // it doesn't really, really matter that we're not completing this round of
    // stabilization.  better than crashing the simulator, anyway.
    if(mykademlia->flyweight.find((*i)->id) == mykademlia->flyweight.end())
      return;

    if(now() - (*i)->lastts < Kademlia::refresh_rate)
      return;
  }

  // Oh.  stuff in this k-bucket is old, or we don't know anything. Lookup a
  // random key in this range.
  Kademlia::NodeID mask = 0;
  for(unsigned i=0; i<depth; i++)
    mask |= (((Kademlia::NodeID) 1) << (Kademlia::idsize-depth-i));

  Kademlia::NodeID random_key = _id & mask;
  KDEBUG(2) << "k_stabilizer: prefix = " << prefix << ", mask = " << Kademlia::printbits(mask) << ", random_key = " << Kademlia::printbits(random_key) << endl;

  // find first successor that we believe is alive.
  k_collect_closest getsuccessor(random_key);
  mykademlia->root()->traverse(&getsuccessor, mykademlia);
  k_nodeinfo *ki = 0;
  for(set<Kademlia::NodeID>::const_iterator i = getsuccessor.results.begin(); i != getsuccessor.results.end(); ++i) {
    if(mykademlia->flyweight.find(*i) == mykademlia->flyweight.end())
      continue;
    ki = mykademlia->flyweight[*i];
    break;
  }

  // we don't know a single soul in the universe. not even well-known node. this
  // can happen when stabilize() gets called right after we joined, but before
  // we got a reply back from well-known node.  just return.  this will get
  // better soon.
  if(!ki)
    return;

  // lookup the random key and update this k-bucket with what we learn
  Kademlia::lookup_args la(mykademlia->id(), mykademlia->node()->ip(), random_key);
  Kademlia::lookup_result lr;

  mykademlia->do_lookup(&la, &lr);
  if(!mykademlia->node()->alive())
    return;

  // update our k-buckets
  NODES_ITER(&lr.results) {
    KDEBUG(2) << "k_stabilizer: update_k_bucket " << Kademlia::printID((*i)->id) << ", ip = " << (*i)->ip << endl;
    mykademlia->update_k_bucket((*i)->id, (*i)->ip);
  }
}
// }}}
// {{{ k_stabilized::execute
void
k_stabilized::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  // for KDEBUG purposes
  Kademlia::NodeID _id = k->kademlia()->id();

  // if we know about nodes in this part of the ID space, great.
  if(!k->nodes->empty())
    return;

  KDEBUG(2) << "stabilized: " << prefix << " not present, depth = " << depth << ", prefix = " << prefix << ", leftright = " << leftright << endl;

  //
  // Node claims there is no node to satisfy this entry in the finger table.
  // Check whether that is true.
  //
  Kademlia::NodeID upper = 0, lower = 0;
  for(unsigned i=0; i<Kademlia::idsize; i++) {
    if(i < (depth-1)) {
      unsigned bit = Kademlia::getbit(_id, i);
      lower |= (((Kademlia::NodeID) bit) << Kademlia::idsize-i-1);
      upper |= (((Kademlia::NodeID) bit) << Kademlia::idsize-i-1);
    } else if(i == (depth-1) && leftright) {
      lower |= (((Kademlia::NodeID) leftright) << Kademlia::idsize-i-1);
      upper |= (((Kademlia::NodeID) leftright) << Kademlia::idsize-i-1);
    } else if(i > (depth-1))
      upper |= (((Kademlia::NodeID) 1) << Kademlia::idsize-i-1);
  }

  KDEBUG(2) << "stabilized: lower = " << Kademlia::printID(lower) << endl;
  KDEBUG(2) << "stabilized: upper = " << Kademlia::printID(upper) << endl;

  // yields the node with smallest id greater than lower
  vector<Kademlia::NodeID>::const_iterator it = upper_bound(_v->begin(), _v->end(), lower);

  // check that this is smaller than upper.  if so, then this node would
  // qualify for this entry in the finger table, so the node that says there
  // is no such is WRONG.
  if(it != _v->end() && *it <= upper) {
    KDEBUG(2) << "stabilized: prefix " << prefix << " on depth " << depth << " is invalid, but " << Kademlia::printID(*it) << " matches " << endl;
    _stabilized = false;
  }
  // assert(false);
}
// }}}
// {{{ k_finder::execute
void
k_finder::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  if(!k->leaf)
    return;

  k->checkrep();
  NODES_ITER(&k->nodes->nodes) {
    if((*i)->id == _n)
      _found++;
  }

  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = k->replacement_cache->begin(); i != k->replacement_cache->end(); ++i)
    if((*i)->id == _n)
      _found++;
}
// }}}
// {{{ k_dumper::execute
void
k_dumper::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  string spaces = "";
  for(unsigned i=0; i<depth; i++)
    spaces += "  ";
  cout << spaces << "prefix: " << prefix << ", depth " << depth << endl;
  NODES_ITER(&k->nodes->nodes) {
    cout << spaces << "  " << Kademlia::printbits((*i)->id) << ", lastts = " << (*i)->lastts << endl;
  }

  cout << spaces << "prefix: " << prefix << ", depth " << depth << ", replacement cache: " << endl;
  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = k->replacement_cache->begin(); i != k->replacement_cache->end(); ++i) {
    cout << spaces << "  " << Kademlia::printID((*i)->id) << ", lastts = " << (*i)->lastts << endl;
  }
}
// }}}
// {{{ k_check::execute
void
k_check::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  if(!k->leaf)
    return;

  k->checkrep();

  set<Protocol*> l = Network::Instance()->getallprotocols(k->kademlia()->proto_name());

  // go through all pointers in node
  NODES_ITER(&k->nodes->nodes) {
    for(set<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
      Kademlia *kad = (Kademlia*) *pos;
      if(kad->id() == k->kademlia()->id())
        continue;
      for(hash_map<Kademlia::NodeID, k_nodeinfo*>::const_iterator j = kad->flyweight.begin(); j != kad->flyweight.end(); ++j)
        assert(j->second != *i);
    }
  }

  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = k->replacement_cache->begin(); i != k->replacement_cache->end(); ++i) {
    for(set<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
      Kademlia *kad = (Kademlia*) *pos;
      if(kad->id() == k->kademlia()->id())
        continue;
      for(hash_map<Kademlia::NodeID, k_nodeinfo*>::const_iterator j = kad->flyweight.begin(); j != kad->flyweight.end(); ++j)
        assert(j->second != *i);
    }
  }
}
// }}}

// {{{ k_collect_closest::k_collect_closest
k_collect_closest::k_collect_closest(Kademlia::NodeID n) :
  k_traverser("k_collect_closest"), _node(n)
{
  DEBUG(2) << "k_collect_closest " << Kademlia::printbits(n) << endl;
  Kademlia::IDcloser::n = n;
  results.clear();
}
// }}}
// {{{ k_collect_closest::execute
void
k_collect_closest::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  k->checkrep();

  // for debugging purposes only
  Kademlia::NodeID _id = k->kademlia()->id();
  Kademlia::IDcloser::n = _node;
  KDEBUG(2) << "k_collect_closest: prefix = " << prefix << endl;

  if(!k->leaf)
    return;

  NODES_ITER(&k->nodes->nodes) {
    KDEBUG(2) << "k_collect_closest::execute: insert = " << Kademlia::printID((*i)->id) << ", lastts = " << (*i)->lastts << ", ip = " << (*i)->ip << " in resultset" << endl;
    results.insert((*i)->id);
  }

  KDEBUG(2) << "k_collect_closest::execute: resultset.size() = " << results.size() << endl;
}
// }}}
