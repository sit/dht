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
unsigned Kademlia::_nkademlias = 0;
Kademlia::k_nodeinfo_pool *Kademlia::pool = 0;

double Kademlia::_rpc_bytes = 0;
double Kademlia::_good_latency = 0;
double Kademlia::_good_hops = 0;
int Kademlia::_good_lookups = 0;
int Kademlia::_ok_failures = 0;  // # legitimate lookup failures
int Kademlia::_bad_failures = 0; // lookup failed, but node was live

Kademlia::NodeID *Kademlia::_all_kademlias = 0;
HashMap<Kademlia::NodeID, Kademlia*> *Kademlia::_nodeid2kademlia = 0;

#define NODES_ITER(x) for(set<k_nodeinfo, Kademlia::closer>::const_iterator i = (x)->begin(); i != (x)->end(); ++i)


// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(IPAddress i, Args a)
  : P2Protocol(i), _id(ConsistentHash::ip2chid(ip()))
{
  // KDEBUG(1) << "id: " << printID(_id) << ", ip: " << ip() << endl;
  if(getenv("P2PSIM_CHECKREP"))
    docheckrep = strcmp(getenv("P2PSIM_CHECKREP"), "0") ? true : false;

  if(!k) {
    k = a.nget<unsigned>("k", 20, 10);
    // KDEBUG(1) << "k = " << k << endl;
  }

  if(!alpha) {
    alpha = a.nget<unsigned>("alpha", 3, 10);
    // KDEBUG(1) << "alpha = " << alpha << endl;
  }

  if(!stabilize_timer) {
    stabilize_timer = a.nget<unsigned>("stabilize_timer", 10000, 10);
    // KDEBUG(1) << "stabilize_timer = " << stabilize_timer << endl;
  }

  if(!refresh_rate) {
    refresh_rate = a.nget<unsigned>("refresh_rate", 1000, 10);
    // KDEBUG(1) << "refresh_rate = " << refresh_rate << endl;
  }

  if(!Kademlia::pool)
    Kademlia::pool = New k_nodeinfo_pool();
  _me = k_nodeinfo(_id, ip());
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
    b->child[bit^1]->replacement_cache = New k_nodes(b->child[bit^1]);
    b->child[bit^1]->leaf = true;

    // nodeside
    b->child[bit] = New k_bucket(b, this);
    b = b->child[bit];
    b->leaf = false;

    if(i == Kademlia::idsize-2)
      break;
  }
  b->nodes = New k_nodes(b);
  b->replacement_cache = New k_nodes(b);
  b->leaf = true;

  _nkademlias++;
}

Kademlia::NodeID Kademlia::closer::n = 0;
// }}}
// {{{ Kademlia::~Kademlia
Kademlia::~Kademlia()
{
  // KDEBUG(1) << "Kademlia::~Kademlia" << endl;
  for(HashMap<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i)
    Kademlia::pool->push(i.value());
  flyweight.clear();

  delete _root;

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

  if(_all_kademlias) {
    delete _all_kademlias;
    _all_kademlias = 0;
  }

  if(_nodeid2kademlia) {
    delete _nodeid2kademlia;
    _nodeid2kademlia = 0;
  }


  if(--_nkademlias == 0) {
    delete pool;
    pool = 0;
  }
}
// }}}
// {{{ Kademlia::initstate
void 
Kademlia::initstate()
{
  const set<Node*> *l = Network::Instance()->getallnodes();
  // KDEBUG(1) << "Kademlia::initstate l->size = " << l->size() << endl;

  if(!_all_kademlias) {
    // KDEBUG(1) << "allocating all" << endl;
    _all_kademlias = New NodeID[l->size()];
    _nodeid2kademlia = New HashMap<NodeID, Kademlia*>;
    unsigned j = 0;
    for(set<Node*>::const_iterator i = l->begin(); i != l->end(); ++i) {
      Kademlia *k = (Kademlia *) *i;
      _all_kademlias[j++] = k->id();
      _nodeid2kademlia->insert(k->id(), k);
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

    // // KDEBUG(2) << " initstate lower = " << Kademlia::printbits(lower) << endl;
    // // KDEBUG(2) << " initstate upper = " << Kademlia::printbits(upper) << endl;

    // create a set of canditates
    vector<NodeID> candidates;
    for(unsigned i = 0; i < l->size(); i++) {
      if(_all_kademlias[i] >= lower && _all_kademlias[i] <= upper)
        candidates.push_back(_all_kademlias[i]);
    }

    // if candidate set size is less than k, then insert them all.
    if(candidates.size() <= Kademlia::k) {
      for(vector<NodeID>::const_iterator i = candidates.begin(); i != candidates.end(); ++i) {
        Kademlia *k = (*_nodeid2kademlia)[*i];
        // KDEBUG(2) << "initstate: inserting candidate = " << Kademlia::printID(k->id()) << endl;
        // assert(k);
        insert(k->id(), k->ip(), true);
      }
      continue;
    }

    // if candidate set is too large, pick k at random
    unsigned count = 0;
    while(count <= Kademlia::k && candidates.size()) {
      unsigned index = random() % candidates.size();
      Kademlia *k = (*_nodeid2kademlia)[candidates[index]];
      // assert(k);

      if(flyweight[k->id()])
        continue;

      insert(k->id(), k->ip(), true);
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
  // KDEBUG(1) << "Kademlia::join" << endl;
  if(_joined)
    return;

  IPAddress wkn = args->nget<IPAddress>("wellknown");
  // assert(wkn);
  // assert(alive());

  // I am the well-known node
  if(wkn == ip()) {
    // KDEBUG(2) << "Node " << printID(_id) << " is wellknown." << endl;
    delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
    _joined = true;
    return;
  }

join_restart:
  // lookup my own key with well known node.
  lookup_args la(_id, ip(), _id);
  lookup_result lr;
  // KDEBUG(2) << "join: lookup my id." << endl;
  record_stat(STAT_LOOKUP, 1, 0);
  bool b = doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  assert(b);
  record_stat(STAT_LOOKUP, lr.results.size(), 0);

  if(!alive())
    return;

  // put well known node in k-buckets
  // KDEBUG(2) << "join: lr.rid = " << printID(lr.rid) << endl;
  update_k_bucket(lr.rid, wkn);

  // put all nodes that wkn told us in k-buckets
  // KDEBUG(2) << "join: lr.results.size() = " << lr.results.size() << endl;
  for(set<k_nodeinfo, older>::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i) {
    // KDEBUG(2) << "join: lr.results iterator id = " << printID((*i)->id) << ", ip = " << (*i)->ip << endl;
    if(!flyweight[i->id] && i->id != _id) {
      // XXX: the touch is WRONG.  For all we know, the node is dead.
      insert(i->id, i->ip);
      touch(i->id);
    }
  }

  // get our ``successor'' and compute length
  // of prefix we have in common
  vector<k_nodeinfo*> successors;
  _root->find_node(_id, &successors, 1);
  NodeID succ_id = successors[0]->id;

  // KDEBUG(2) << "join: succ_id is " << printID(succ_id) << endl;
  unsigned cpl = common_prefix(_id, succ_id);

  // all entries further away than him need to be refreshed.
  // see section 2.3
  for(int i=cpl-1; i>=0; i--) {
    // XXX: should be random
    lookup_args la(_id, ip(), (_id ^ (((Kademlia::NodeID) 1)<<i)));
    lookup_result lr;

    // are we dead?  bye.
    if(!alive())
      return;

    // if we believe our successor died, then start again
    if(!flyweight[succ_id]) {
      // KDEBUG(2) << " restarting join" << endl;
      clear();
      goto join_restart;
    }
    
    // if the RPC failed, or the node is now dead, start over
    k_nodeinfo *ki = flyweight[succ_id];
    // KDEBUG(2) << "join: iteration " << i << ", ki->id is " << printID(ki->id) << ", ip = " << ki->ip << ", cpl = " << cpl << endl;
    record_stat(STAT_LOOKUP, 1, 0);
    // assert(ki->ip <= 1837);
    if(!doRPC(ki->ip, &Kademlia::do_lookup, &la, &lr)) {
      clear();
      goto join_restart;
    }
    record_stat(STAT_LOOKUP, lr.results.size(), 0);

    if(!alive())
      return;

    for(set<k_nodeinfo, older>::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
      if(!flyweight[i->id] && i->id != _id)
        insert(i->id, i->ip);
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
  // KDEBUG(1) << "Kademlia::crash" << endl;
  // assert(alive());
  _root->collapse();
  for(HashMap<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i; i++)
    Kademlia::pool->push(i.value());
  flyweight.clear();
  _joined = false;
}

// }}}
// {{{ Kademlia::clear
void
Kademlia::clear()
{
  // destroy k-buckets
  // KDEBUG(1) << "Kademlia::clear" << endl;
  for(HashMap<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i; i++)
    Kademlia::pool->push(i.value());
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
  Kademlia *k = (Kademlia*) Network::Instance()->getnode(key_ip);
  NodeID key = k->id();

  // KDEBUG(1) << "Kademlia::lookup: " << printID(key) << endl;
  // assert(alive());
  // assert(_nodeid2kademlia->find(key, 0));

  find_value_args fa(_id, ip(), key);
  find_value_result fr;

  Time before = now();
  find_value(&fa, &fr);

  bool alive_and_joined = (*_nodeid2kademlia)[key]->alive() &&
                          (*_nodeid2kademlia)[key]->_joined;

  // XXX: this shouldn't happen :(
  if(!fr.succ.id) {
    if(alive_and_joined)
      _bad_failures++;
    else
      _ok_failures++;
    return;
  }

  // get best match
  fr.succ.checkrep();

  // now ping that nod.
  ping_args pa(fr.succ.id, fr.succ.ip);
  ping_result pr;
  if(!doRPC(fr.succ.ip, &Kademlia::do_ping, &pa, &pr) && alive()) {
    // KDEBUG(2) << "Kademlia::lookup: ping RPC to " << Kademlia::printID(fr.succ->id) << " failed " << endl;
    if(flyweight[fr.succ.id])
      erase(fr.succ.id);
  }
  Time after = now();

  KDEBUG(0) << "lookup completed in " << after - before << "ms, using " << fr.hops << " hop(s)" << endl;
  
  // did we find that node?
  if(key == fr.succ.id){
    _good_lookups += 1;
    _good_latency += (after - before);
    _good_hops += fr.hops;
  } else if(alive_and_joined) {
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
  // KDEBUG(1) << "Kademlia::do_ping from " << printID(args->id) << endl;
  update_k_bucket(args->id, args->ip);
}

// }}}
// {{{ Kademlia::util for find_value and do_lookup
#define SEND_RPC(x, ARGS) {                                                             \
    find_node_args *fa = New find_node_args(_id, ip(), ARGS->key);                      \
    find_node_result *fr = New find_node_result;                                        \
    record_stat(STAT_FIND_VALUE, 1, 0);                                                 \
    unsigned rpc = asyncRPC(x.ip, &Kademlia::find_node, fa, fr);                        \
    callinfo *ci = New callinfo(x, fa, fr);                                             \
    rpcset->insert(rpc);                                                                \
    outstanding_rpcs->insert(rpc, ci);                                                  \
}

#define CREATE_REAPER(STAT) {                                                           \
      if(!outstanding_rpcs->size()) {                                                   \
        delete rpcset;                                                                  \
        delete outstanding_rpcs;                                                        \
        return;                                                                         \
      }                                                                                 \
      reap_info *ri = New reap_info();                                                  \
      ri->k = this;                                                                     \
      ri->rpcset = rpcset;                                                              \
      ri->outstanding_rpcs = outstanding_rpcs;                                          \
      ri->stat = STAT;                                                                  \
      ThreadManager::Instance()->create(Kademlia::reap, (void*) ri);                    \
      return;                                                                           \
}
// }}}
// {{{ Kademlia::find_value
void
Kademlia::find_value(find_value_args *fargs, find_value_result *fresult)
{
  // KDEBUG(1) << "Kademlia::find_value: node " << printID(fargs->id) << " does find_value for " << printID(fargs->key) << ", flyweight.size() = " << endl;
  // assert(alive());
  update_k_bucket(fargs->id, fargs->ip);
  fresult->rid = _id;

  // find alpha successors for this key
  closer::n = fargs->key;
  set<k_nodeinfo, closer> successors;
  vector<k_nodeinfo*> tmp;
  _root->find_node(fargs->key, &tmp);
  for(unsigned i=0; i<tmp.size(); i++)
    successors.insert(tmp[i]);

  // we can't do anything but return ourselves
  if(!successors.size()) {
    fresult->succ = _me;
    return;
  }

  // keep track of best answer so far
  fresult->succ = *successors.begin();

  // send out the first alpha RPCs
  HashMap<unsigned, callinfo*> *outstanding_rpcs = New HashMap<unsigned, callinfo*>;
  RPCSet *rpcset = New RPCSet;
  HashMap<NodeID, bool> last_rpcs;
  {
    unsigned alpha_counter = 0;
    NODES_ITER(&successors) {
      k_nodeinfo ki = *i;
      SEND_RPC(ki, fargs);
      successors.erase(ki);
      last_rpcs.insert(ki.id, true);
      if(++alpha_counter >= Kademlia::alpha)
        break;
    }
  }
  fresult->hops = 1;

  // now send out a new RPC for every single RPC that comes back
  unsigned useless_replies = 0;
  while(true) {
    bool ok;
    unsigned donerpc = rcvRPC(rpcset, ok);
    callinfo *ci = (*outstanding_rpcs)[donerpc];
    outstanding_rpcs->remove(donerpc);
    if(last_rpcs[ci->fr->rid])
      last_rpcs.remove(ci->fr->rid);

    k_nodeinfo ki;
    if(!alive()) {
      delete ci;
      CREATE_REAPER(STAT_FIND_VALUE); // returns
    }

    // node was dead
    closer::n = fargs->key;
    if(!ok) {
      if(flyweight[ci->ki.id])
        erase(ci->ki.id);
      delete ci;
      goto next_candidate;
    }

    // node was ok
    record_stat(STAT_FIND_VALUE, ci->fr->results.size(), 0);
    update_k_bucket(ci->fr->rid, ci->ki.ip);

    ki = ci->fr->results[0];
    if(ki.id == fargs->key) {
      fresult->succ = ki;
      delete ci;
      CREATE_REAPER(STAT_FIND_VALUE); // returns
    }

    // put all the ones better than what we knew so far in successors
    useless_replies++;
    closer::n = fargs->key;
    for(unsigned i=0; i<ci->fr->results.size(); i++) {
      k_nodeinfo ki = ci->fr->results[i];
      if(Kademlia::distance(ki.id, fargs->key) >= Kademlia::distance(fresult->succ.id, fargs->key))
        break;
      successors.insert(ki);
      useless_replies = 0;
    }
    delete ci;

    // if we improved, that's a hop
    if(useless_replies != 0)
      fresult->hops++;

    if(!successors.size() || (!last_rpcs.size() && useless_replies >= Kademlia::alpha)) {
      CREATE_REAPER(STAT_FIND_VALUE); // returns
    }

next_candidate:
    if(!successors.size()) {
      CREATE_REAPER(STAT_FIND_VALUE); // returns
    }
    k_nodeinfo front = *successors.begin();
    if(Kademlia::distance(front.id, fargs->key) < Kademlia::distance(fresult->succ.id, fargs->key))
      fresult->succ = front;
    SEND_RPC(front, fargs);
    if(last_rpcs.size() < (int) Kademlia::alpha)
      last_rpcs.insert(front.id, true);
    successors.erase(front);
  }
}

// }}}
// {{{ Kademlia::do_lookup
void
Kademlia::do_lookup(lookup_args *largs, lookup_result *lresult)
{
  // KDEBUG(1) << "Kademlia::do_lookup: node " << printID(largs->id) << " does lookup for " << printID(largs->key) << ", flyweight.size() = " << endl;
  // assert(alive());
  update_k_bucket(largs->id, largs->ip);
  lresult->rid = _id;

  // find successors of this key
  set<k_nodeinfo, closer> successors;
  vector<k_nodeinfo*> tmp;
  _root->find_node(largs->key, &tmp);
  for(unsigned i=0; i<tmp.size(); i++) {
    successors.insert(tmp[i]);
    lresult->results.insert(tmp[i]);
  }

  // we can't do anything but return ourselves
  if(!successors.size()) {
    lresult->results.insert(_me);
    return;
  }

  // keep track of worst-of-best
  NodeID worst = successors.rbegin()->id;

  // send out the first alpha RPCs
  HashMap<unsigned, callinfo*> *outstanding_rpcs = New HashMap<unsigned, callinfo*>;
  RPCSet *rpcset = New RPCSet;
  HashMap<NodeID, bool> last_rpcs;
  {
    unsigned alpha_counter = 0;
    NODES_ITER(&successors) {
      k_nodeinfo ki = *i;
      SEND_RPC(ki, largs);
      successors.erase(ki);
      last_rpcs.insert(ki.id, true);
      if(++alpha_counter >= Kademlia::alpha)
        break;
    }
  }
  lresult->hops = 1;

  unsigned useless_replies = 0;
  while(true) {
    bool ok;
    unsigned donerpc = rcvRPC(rpcset, ok);
    callinfo *ci = (*outstanding_rpcs)[donerpc];
    outstanding_rpcs->remove(donerpc);
    if(last_rpcs[ci->fr->rid])
      last_rpcs.remove(ci->fr->rid);

    if(!alive()) {
      while(lresult->results.size() > Kademlia::k)
        lresult->results.erase(*lresult->results.rbegin());
      delete ci;
      CREATE_REAPER(STAT_LOOKUP); // returns
    }

    // node was dead
    closer::n = largs->key;
    if(!ok) {
      if(flyweight[ci->ki.id])
        erase(ci->ki.id);
      delete ci;
      goto next_candidate;
    }

    // node was ok
    record_stat(STAT_LOOKUP, ci->fr->results.size(), 0);
    update_k_bucket(ci->fr->rid, ci->ki.ip);

    // put all the ones better than the worst we knew so far in successors
    closer::n = largs->key;
    useless_replies++;
    for(unsigned i=0; i<ci->fr->results.size(); i++) {
      k_nodeinfo ki = ci->fr->results[i];
      if(Kademlia::distance(ki.id, largs->key) >= Kademlia::distance(worst, largs->key))
        break;
      successors.insert(ki);
      lresult->results.insert(tmp[i]);
      useless_replies = 0;
    }
    delete ci;

    if(useless_replies != 0)
      lresult->hops++;

    // there's nobody left to ask. we're done.
    if(!successors.size())
      goto next_candidate;

    // the last alpha nodes that we sent an RPC did not change our view of the
    // world: blast RPCs to all remaining nodes
    if(!last_rpcs.size() && useless_replies >= Kademlia::alpha) {
      NODES_ITER(&successors) {
        k_nodeinfo ki = *i;
        SEND_RPC(ki, largs);
        successors.erase(ki);
      }
      continue;
    }

    // resize back to Kademlia::k
    while(successors.size() > Kademlia::k)
      successors.erase(*successors.rbegin());

    // need to update our worst-of-best?
    if(successors.size() &&
      (Kademlia::distance(successors.rbegin()->id, largs->key) <
       Kademlia::distance(worst, largs->key)))
    {
      worst = successors.rbegin()->id;
    }

next_candidate:
    if(!successors.size()) {
      while(lresult->results.size() > Kademlia::k)
        lresult->results.erase(*lresult->results.rbegin());
      CREATE_REAPER(STAT_LOOKUP); // returns
    }

    k_nodeinfo front = *successors.begin();
    SEND_RPC(front, largs);
    successors.erase(front);
    if(last_rpcs.size() < (int) Kademlia::alpha)
      last_rpcs.insert(front.id, true);
  }
}
// }}}
// {{{ Kademlia::find_node
// Kademlia's FIND_NODE.  Returns the best k from its own k-buckets
void
Kademlia::find_node(find_node_args *largs, find_node_result *lresult)
{
  // KDEBUG(2) << "find_node invoked by " << printID(largs->id) << ", looking for " << printID(largs->key) << ", calling thread = " << largs->tid << endl;
  // assert(alive());

  update_k_bucket(largs->id, largs->ip);
  lresult->rid = _id;

  // deal with the empty case
  if(!flyweight.size()) {
    // assert(p);
    // KDEBUG(2) << "find_node: tree is empty. returning myself, ip = " << ip() << endl;
    lresult->results.push_back(_me);
    return;
  }

  vector<k_nodeinfo*> tmpset;
  _root->find_node(largs->key, &tmpset);
  // KDEBUG(2) << "find_node: returning:" << endl;
  for(unsigned i=0; i<tmpset.size(); i++)
    lresult->results.push_back(tmpset[i]);
}


// }}}
// {{{ Kademlia::reschedule_stabilizer
void
Kademlia::reschedule_stabilizer(void *x)
{
  // KDEBUG(1) << "Kademlia::reschedule_stabilizer" << endl;
  if(!alive()) {
    // KDEBUG(2) << "Kademlia::reschedule_stabilizer returning because I'm dead." << endl;
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
  // KDEBUG(0) << "Kademlia::stabilize" << endl;
  // assert(alive());

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
// post: lastts for id is updated, update propagated to k-bucket.
void
Kademlia::touch(NodeID id)
{
  // KDEBUG(1) << "Kademlia::touch " << Kademlia::printID(id) << endl;

  // assert(alive());
  // assert(id);
  // assert(flyweight.find(id, 0));
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
  // KDEBUG(1) << "Kademlia::insert " << Kademlia::printID(id) << ", ip = " << ip << endl;
  static unsigned counter = 0;

  // assert(alive());
  // assert(id && ip);
  // assert(!flyweight.find(id, 0));

  k_nodeinfo *ni = Kademlia::pool->pop(id, ip);
  assert(ni);
  if(init_state)
    ni->lastts = counter++;
  flyweight.insert(id, ni);
  _root->insert(id, false, init_state);
}
// }}}
// {{{ Kademlia::erase
void
Kademlia::erase(NodeID id)
{
  // KDEBUG(1) << "Kademlia::erase " << Kademlia::printID(id) << endl;

  // assert(flyweight.find(id, 0));
  _root->erase(id);
  // KDEBUG(2) << "Kademlia::erase deleting id = " << printID(id) << ", ip = " << flyweight[id]->ip << endl;
  k_nodeinfo *ki = flyweight[id];
  flyweight.remove(id);
  Kademlia::pool->push(ki);
}
// }}}
// {{{ Kademlia::update_k_bucket
inline void
Kademlia::update_k_bucket(NodeID id, IPAddress ip)
{
  // KDEBUG(1) << "Kademlia::update_k_bucket" << endl;

  // update k-bucket
  if(id == _id)
    return;

  if(!flyweight[id]) {
    // KDEBUG(2) << "Kademlia::update_k_bucket says " << printID(id) << " doesn't exist yet" << endl;
    insert(id, ip);
  }
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
  // NodeID _id = ri->k->id();

  // cout << "Kademlia::reap" << endl;
  assert((int) ri->rpcset->size() == ri->outstanding_rpcs->size());

  while(ri->outstanding_rpcs->size()) {
    bool ok;
    unsigned donerpc = ri->k->rcvRPC(ri->rpcset, ok);
    callinfo *ci = ri->outstanding_rpcs->find(donerpc);

    // cout << "Kademlia::reap ok = " << ok << ", ki = " << endl;
    if(ok) {
      if(ri->k->alive())
        ri->k->update_k_bucket(ci->fr->rid, ci->ki.ip);
      ri->k->record_stat(ri->stat, ci->fr->results.size(), 0);
    } else if(ri->k->flyweight[ci->ki.id])
      if(ri->k->alive())
        ri->k->erase(ci->ki.id);

    ri->outstanding_rpcs->remove(donerpc);
    delete ci;
  }

  // ri->k->_riset.remove(ri);
  delete ri;
  // cout << "reaper done" << endl;
  threadexits(0);
}
// }}}

// {{{ k_nodeinfo_cmp
inline int
k_nodeinfo_cmp(const void *k1, const void *k2)
{
  k_nodeinfo *kx1, *kx2;
  kx1 = *((k_nodeinfo**) k1);
  kx2 = *((k_nodeinfo**) k2);
  return kx1->lastts - kx2->lastts;
}
// }}}
// {{{ k_nodes::k_nodes
k_nodes::k_nodes(k_bucket *parent) : _parent(parent)
{
  _map.clear();
  _redo = REBUILD;
  _nodes = 0;
}
// }}}
// {{{ k_nodes::~k_nodes
k_nodes::~k_nodes()
{
  if(_nodes)
    delete _nodes;
  _map.clear();
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
k_nodes::insert(Kademlia::NodeID n, bool touch = false)
{
//  Kademlia::NodeID _id = _parent->kademlia()->id();
  // KDEBUG(1) << "k_nodes::insert " << Kademlia::printID(n) << endl;
  checkrep();

  // assert(n);
  // assert(_parent->kademlia()->flyweight.find(n, 0));
  k_nodeinfo *ninfo = _parent->kademlia()->flyweight[n];
  // assert(ninfo);
  ninfo->checkrep();

  // if already in set, and we're not going to touch the timestamp, we're done.
  bool contained = contains(n);
  if(contained && !touch) {
    checkrep();
    return;
  }

  if(contained && touch && (ninfo->lastts != now())) {
    ninfo->lastts = now();
    _redo = _redo ? _redo : RESORT;
    checkrep();
    return;
  }

  // insert into set
  _map.insert(ninfo, true);
  // assert(_parent->kademlia()->flyweight.find_pair(ninfo->id)->value == ninfo);
  _redo = REBUILD;

  checkrep();
}
// }}}
// {{{ k_nodes::clear
void
k_nodes::clear()
{
  checkrep();

  _map.clear();
  if(_nodes)
    free(_nodes);
  _nodes = 0;
  _redo = REBUILD;

  checkrep();
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

  // assert(n);
  // assert(_parent->kademlia()->flyweight.find(n, 0));
  k_nodeinfo* ninfo = _parent->kademlia()->flyweight[n];
  ninfo->checkrep();
  // assert(_map.find(ninfo, false) || _parent->replacement_cache->contains(ninfo->id));

  _map.remove(ninfo);
  _redo = REBUILD;

  checkrep();
}
// }}}
// {{{ k_nodes::contains
bool
k_nodes::contains(Kademlia::NodeID n)
{
  checkrep();
  k_nodeinfo *ki = _parent->kademlia()->flyweight[n];
  // assert(ki);
  return _map.find(ki, false);
}
// }}}
// {{{ k_nodes::rebuild
void
k_nodes::rebuild()
{
  static unsigned _rebuild = 0;
  // Kademlia::NodeID _id = _parent->kademlia()->id();
  // KDEBUG(2) << "k_nodes::rebuild" << endl;

  if(_nodes)
    delete _nodes;
  _nodes = New k_nodeinfo*[_map.size()];

  // fill with entries from _map.
  unsigned j = 0;
  for(HashMap<k_nodeinfo*, bool>::iterator i = _map.begin(); i; i++)
    _nodes[j++] = i.key();
  _redo = RESORT;

  // once in a while throw entries beyond index k out of the set into the
  // replacement cache.  (don't do this if this *is* the replacement cache;
  // k_nodes also implements replacement cache).
  if(_rebuild++ < 100)
    return;

  bool i_am_replacement_cache = (this == _parent->replacement_cache);
  unsigned oldsize = _map.size();
  for(unsigned i=Kademlia::k; i<oldsize; i++) {
    k_nodeinfo *ki = _nodes[i];
    _map.remove(ki);
    if(i_am_replacement_cache) {
      _parent->kademlia()->flyweight.remove(ki->id);
      Kademlia::pool->push(ki);
    } else
      _parent->replacement_cache->insert(ki->id);
  }
  _rebuild = NOTHING;
  rebuild(); // after cleanup, rebuild _nodes again.
}
// }}}
// {{{ k_nodes::get 
inline k_nodeinfo*
k_nodes::get(unsigned i)
{
  // assert(i >= 0 && (int) i < _map.size());
  // Kademlia::NodeID _id = _parent->kademlia()->id();

  if(_redo == NOTHING)
    return _nodes[i];

  if(_redo == REBUILD)
    rebuild();

  if(_redo == RESORT) {
    if(_map.size() >= 2)
      qsort(_nodes, _map.size(), sizeof(k_nodeinfo*), &k_nodeinfo_cmp);
    _redo = NOTHING;
  }

  return _nodes[i];
}
// }}}
// {{{ k_nodes::checkrep
inline void
k_nodes::checkrep()
{
  if(!Kademlia::docheckrep)
    return;

  // Kademlia::NodeID _id = _parent->kademlia()->id();

  assert(_parent);
  assert(_parent->nodes == this || _parent->replacement_cache == this);
  assert(_parent->kademlia());
  assert(_parent->kademlia()->id());

  // own ID should never be in flyweight
  assert(!_parent->kademlia()->flyweight.find(_parent->kademlia()->id(), 0));

  if(!size())
    return;

  // all nodes are in flyweight
  // all nodes are unique
  // if we're replacement_cache, entry doesn't exist in nodes and vice versa
  k_nodes *other = _parent->nodes == this ? _parent->replacement_cache : this;
  HashMap<Kademlia::NodeID, bool> haveseen;
  for(unsigned i=0; i<size(); i++) {
    assert(_parent->kademlia()->flyweight.find(get(i)->id, 0));
    assert(_parent->kademlia()->flyweight.find_pair(get(i)->id)->value == get(i));
    assert(!haveseen.find(get(i)->id, false));
    assert(!other->contains(get(i)->id));
    haveseen.insert(get(i)->id, true);
  }

  if(size() == 1) {
    assert(get(0) == last());
    assert(get(0)->id == last()->id);
    return;
  }

  Time prev = get(0)->lastts;
  for(unsigned i=1; i<size(); i++) {
    assert(prev <= get(i)->lastts);
    prev = get(i)->lastts;
  }
}
// }}}

// {{{ k_nodeinfo::k_nodeinfo
k_nodeinfo::k_nodeinfo(NodeID id, IPAddress ip) : id(id), ip(ip)
{
  lastts = 0;
  checkrep();
}
// }}}
// {{{ k_nodeinfo::k_nodeinfo
k_nodeinfo::k_nodeinfo(k_nodeinfo *k) : id(k->id), ip(k->ip), lastts(k->lastts)
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
//  Kademlia::NodeID _id = kademlia()->id();
  // KDEBUG(1) << "k_bucket::traverser for " << traverser->type() << ", prefix = " << prefix << endl;
  checkrep();

  if(!leaf) {
    if(!k->alive() || !child[0])
      return;
    child[0]->traverse(traverser, k, prefix + "0", depth+1, 0);
    if(!k->alive() || !child[1] || leaf)
      return;
    child[1]->traverse(traverser, k, prefix + "1", depth+1, 1);
    if(!k->alive())
      return;
    checkrep();
    return;
  }


  if(!k->alive())
    return;

  // we're a leaf
  traverser->execute(this, prefix, depth, leftright);

  checkrep();
}
// }}}
// {{{ k_bucket::find_node
void
k_bucket::find_node(Kademlia::NodeID key, vector<k_nodeinfo*> *v,
    unsigned nhits, unsigned depth)
{
  checkrep();

//  // Kademlia::NodeID _id = kademlia()->id();
  Kademlia::closer::n = key;

  // recurse deeper in the right direction if we can
  if(!leaf) {
    unsigned leftmostbit = Kademlia::getbit(key, depth);
    if(v->size() < nhits)
      child[leftmostbit]->find_node(key, v, nhits, depth+1);
    if(v->size() < nhits)
      child[leftmostbit^1]->find_node(key, v, nhits, depth+1);
    checkrep();
    return;
  }

  // collect stuff from the k-bucket itself
  for(unsigned i=0; i<nodes->size(); i++) {
    v->push_back(nodes->get(i));
    if(v->size() >= nhits) {
      checkrep();
      return;
    }
  }

  // NB: optimization.
  // collect stuff from replacement cache.
  for(int i = replacement_cache->size()-1; i >= 0; i--) {
    v->push_back(replacement_cache->get(i));
    if(v->size() >= nhits) {
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
  // assert(kademlia()->flyweight.find(id, 0));

  // Kademlia::NodeID _id = kademlia()->id();
  // KDEBUG(1) << "k_bucket::insert " << Kademlia::printbits(id) << ", prefix = " << prefix << endl;

  // recurse deeper if we can
  if(!leaf) {
    unsigned leftmostbit = Kademlia::getbit(id, depth);
    // KDEBUG(1) << "k_bucket::insert heading towards " << leftmostbit << endl;
    child[leftmostbit]->insert(id, touch, init_state, prefix + (leftmostbit ? "1" : "0"), depth+1);
    checkrep();
    return;
  }

  // if not in k-bucket yet, or there's more space: insert it.
  if(nodes->contains(id) || !nodes->full()) {
    nodes->insert(id, touch);
    nodes->checkrep();
    return;
  }

  // we're full.  put in replacement_cache.
  replacement_cache->insert(id, touch);
  replacement_cache->checkrep();

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
    if(replacement_cache->contains(ki->id))
      replacement_cache->erase(ki->id);
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

  k_nodeinfo *ki = replacement_cache->last(); // youngest
  // assert(ki);
  // assert(ki->id);
  // assert(kademlia()->flyweight.find(ki->id, 0));
  nodes->insert(ki->id, false);
  replacement_cache->erase(ki->id);

  checkrep();
}
// }}}
// {{{ k_bucket::collapse
// transform back from node to leaf
void
k_bucket::collapse()
{
  // Kademlia::NodeID _id = kademlia()->id();
  // KDEBUG(2) << "k_bucket::collapse" << endl;
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

  if(!leaf) {
    assert(child[0] && child[1]);
    return;
  }


  // checkreps for leaf
  assert(nodes);
  assert(replacement_cache);
  nodes->checkrep();
  replacement_cache->checkrep();
}
// }}}

// {{{ k_stabilizer::execute
void
k_stabilizer::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  // for // KDEBUG purposes
  Kademlia *mykademlia = k->kademlia();
  Kademlia::NodeID _id = mykademlia->id();

  if(!k->leaf || !mykademlia->alive())
    return;

  // return if any entry in this k-bucket is fresh
  for(unsigned i = 0; i < k->nodes->size(); i++) {
    // XXX: this is the sign of bad shit going on.  get out.
    // it doesn't really, really matter that we're not completing this round of
    // stabilization.  better than crashing the simulator, anyway.
    k_nodeinfo *ki = k->nodes->get(i);
    if(!mykademlia->flyweight[ki->id])
      return;

    if(now() - ki->lastts < Kademlia::refresh_rate)
      return;
  }

  // Oh.  stuff in this k-bucket is old, or we don't know anything. Lookup a
  // random key in this range.
  Kademlia::NodeID mask = 0;
  for(unsigned i=0; i<depth; i++)
    mask |= (((Kademlia::NodeID) 1) << (Kademlia::idsize-depth-i));

  Kademlia::NodeID random_key = _id & mask;
  // KDEBUG(2) << "k_stabilizer: prefix = " << prefix << ", mask = " << Kademlia::printbits(mask) << ", random_key = " << Kademlia::printbits(random_key) << endl;

  // find first successor that we believe is alive.
  vector<k_nodeinfo*> successors;
  mykademlia->root()->find_node(random_key, &successors);
  k_nodeinfo *ki = 0;
  for(unsigned i=0; i<successors.size(); i++) {
    if(!mykademlia->flyweight[successors[i]->id])
      continue;
    ki = mykademlia->flyweight[successors[i]->id];
    break;
  }

  // we don't know a single soul in the universe. not even well-known node. this
  // can happen when stabilize() gets called right after we joined, but before
  // we got a reply back from well-known node.  just return.  this will get
  // better soon.
  if(!ki)
    return;

  // lookup the random key and update this k-bucket with what we learn
  Kademlia::lookup_args la(mykademlia->id(), mykademlia->ip(), random_key);
  Kademlia::lookup_result lr;

  mykademlia->do_lookup(&la, &lr);
  if(!mykademlia->alive())
    return;

  // update our k-buckets
  NODES_ITER(&lr.results) {
    // KDEBUG(2) << "k_stabilizer: update_k_bucket " << Kademlia::printID((*i)->id) << ", ip = " << (*i)->ip << endl;
    mykademlia->update_k_bucket(i->id, i->ip);
  }
}
// }}}
// {{{ k_stabilized::execute
void
k_stabilized::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  // for // KDEBUG purposes
  Kademlia::NodeID _id = k->kademlia()->id();

  // if we know about nodes in this part of the ID space, great.
  if(!k->nodes->empty())
    return;

  // KDEBUG(2) << "stabilized: " << prefix << " not present, depth = " << depth << ", prefix = " << prefix << ", leftright = " << leftright << endl;

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

  // KDEBUG(2) << "stabilized: lower = " << Kademlia::printID(lower) << endl;
  // KDEBUG(2) << "stabilized: upper = " << Kademlia::printID(upper) << endl;

  // yields the node with smallest id greater than lower
  vector<Kademlia::NodeID>::const_iterator it = upper_bound(_v->begin(), _v->end(), lower);

  // check that this is smaller than upper.  if so, then this node would
  // qualify for this entry in the finger table, so the node that says there
  // is no such is WRONG.
  if(it != _v->end() && *it <= upper) {
    // KDEBUG(2) << "stabilized: prefix " << prefix << " on depth " << depth << " is invalid, but " << Kademlia::printID(*it) << " matches " << endl;
    _stabilized = false;
  }
  // // assert(false);
}
// }}}
// {{{ k_finder::execute
void
k_finder::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  if(!k->leaf)
    return;

  k->checkrep();
  for(unsigned i = 0; i < k->nodes->size(); i++) {
    if(k->nodes->get(i)->id == _n)
      _found++;
  }

  for(int i = k->replacement_cache->size()-1; i >= 0; i--)
    if(k->replacement_cache->get(i)->id == _n)
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
  for(unsigned i = 0; i < k->nodes->size(); i++) {
    k_nodeinfo *ki = k->nodes->get(i);
    cout << spaces << "  " << Kademlia::printbits(ki->id) << ", lastts = " << ki->lastts << endl;
  }

  cout << spaces << "prefix: " << prefix << ", depth " << depth << ", replacement cache: " << endl;
  for(int i = k->replacement_cache->size()-1; i >= 0; i--) {
    cout << spaces << "  " << Kademlia::printID(k->replacement_cache->get(i)->id) << ", lastts = " << k->replacement_cache->get(i)->lastts << endl;
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

  const set<Node*> *l = Network::Instance()->getallnodes();

  // go through all pointers in node
  for(unsigned i = 0; i < k->nodes->size(); i++) {
    // k_nodeinfo *ki = k->nodes->get(i);
    for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
      Kademlia *kad = (Kademlia*) *pos;
      if(kad->id() == k->kademlia()->id())
        continue;
      /*
      for(HashMap<Kademlia::NodeID, k_nodeinfo*>::const_iterator j = kad->flyweight.begin(); j; j++)
          // assert(j.value() != ki);
      */
    }
  }

  for(int i = k->replacement_cache->size()-1; i >= 0; i--) {
    for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
      Kademlia *kad = (Kademlia*) *pos;
      if(kad->id() == k->kademlia()->id())
        continue;
      /*
      for(HashMap<Kademlia::NodeID, k_nodeinfo*>::const_iterator j = kad->flyweight.begin(); j; j++)
          // assert(j.value() != *i);
      */
    }
  }
}
// }}}

#include "p2psim/bighashmap.cc"
