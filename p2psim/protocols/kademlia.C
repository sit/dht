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

set<Kademlia::NodeID> *Kademlia::_all_kademlias = 0;
hash_map<Kademlia::NodeID, Kademlia*> *Kademlia::_nodeid2kademlia = 0;


// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(Node *n, Args a)
  : P2Protocol(n), _id(ConsistentHash::ip2chid(n->ip()))
{
  KDEBUG(1) << "ip: " << ip() << ", idsize = " << idsize << endl;
  if(getenv("P2PSIM_CHECKREP"))
    docheckrep = strcmp(getenv("P2PSIM_CHECKREP"), "0") ? true : false;

  if(!k)
    k = a.nget<unsigned>("k", 20, 10);
  if(!alpha)
    alpha = a.nget<unsigned>("alpha", 3, 10);
  if(!stabilize_timer)
    stabilize_timer = a.nget<unsigned>("stabilize_timer", 10000, 10);
  if(!refresh_rate)
    refresh_rate = a.nget<unsigned>("refresh_rate", 1000, 10);

  _me = New k_nodeinfo(_id, ip());
  char ptr[32]; sprintf(ptr, "%p", _me);
  KDEBUG(2) << "Kademlia::Kademlia: _me = " << ptr << endl;
  _root = New k_bucket(0, this);

  // init stats
  while (stat.size() < (uint) STAT_SIZE) {
    stat.push_back(0);
    num_msgs.push_back(0);
  }
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
  cout << "lookup " << stat[STAT_LOOKUP] << " " << num_msgs[STAT_LOOKUP] << endl;
}
// }}}
// {{{ Kademlia::initstate
void 
Kademlia::initstate(set<Protocol*> *l)
{
  KDEBUG(1) << "Kademlia::initstate" << endl;

  if(!_all_kademlias) {
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

  // // just bloody call insert on everyone
  // for(set<Protocol*>::const_iterator i = l->begin(); i != l->end(); ++i) {
  //   if((*i)->proto_name() != proto_name())
  //     continue;

  //   Kademlia *k = (Kademlia *) *i;
  //   if(k->id() == _id)
  //     continue;

  //   insert(k->id(), k->node()->ip(), true);
  //   touch(k->id());
  // }


  // KDEBUG(2) << "Kademlia::initstate DUMP for " << printID(id()) << endl;
  // k_dumper dump;
  // _root->traverse(&dump, this);


  // // now make sure every single one is in our tree
  // for(set<Protocol*>::const_iterator i = l->begin(); i != l->end(); ++i) {
  //   if((*i)->proto_name() != proto_name())
  //     continue;

  //   Kademlia *k = (Kademlia *) *i;
  //   if(k->id() == _id)
  //     continue;

  //   KDEBUG(2) << "Kademlia::initstate making sure that " << printID(k->id()) << " is in tree " << endl;
  //   assert(flyweight.find(k->id()) != flyweight.end());
  //   k_finder find(k->id());
  //   _root->traverse(&find, this);
  //   assert(find.found() == 1);
  // }
  //
  // Node claims there is no node to satisfy this entry in the finger table.
  // Check whether that is true.
  //
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

    // KDEBUG(2) << "id = " << printbits(_id) << " initstate: lower = " << Kademlia::printbits(lower) << endl;
    // KDEBUG(2) << "id = " << printbits(_id) << " initstate: upper = " << Kademlia::printbits(upper) << endl;

    // yields the node with smallest id greater than lower
    set<NodeID>::const_iterator it = upper_bound(_all_kademlias->begin(), _all_kademlias->end(), lower);

    // check that this is smaller than upper.  if so, then this node would
    // qualify for this entry in the finger table, so the node that says there
    // is no such is WRONG.
    if(it != _all_kademlias->end() && *it <= upper) {
      Kademlia *k = (*_nodeid2kademlia)[*it];
      // KDEBUG(2) << "id = " << printbits(_id) << " initstate: inserting = " << Kademlia::printbits(k->id()) << endl;
      assert(k);

      insert(k->id(), k->node()->ip(), true);
      touch(k->id());
    }
  }
  // assert(false);

  // remove ourselves from lid
  vector<NodeID> copylid;
  for(set<NodeID>::const_iterator i = _all_kademlias->begin(); i != _all_kademlias->end(); ++i)
    if(*i != id())
      copylid.push_back(*i);

  k_stabilized stab(&copylid);
  _root->traverse(&stab, this);
  assert(stab.stabilized());
}

// }}}
// {{{ Kademlia::join
void
Kademlia::join(Args *args)
{
  KDEBUG(1) << "Kademlia::join" << endl;

  IPAddress wkn = args->nget<IPAddress>("wellknown");
  assert(wkn);
  assert(node()->alive());

  // I am the well-known node
  if(wkn == ip()) {
    KDEBUG(2) << "Node " << printID(_id) << " is wellknown." << endl;
    delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
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
  for(nodeinfo_set::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i) {
    char ptr[32]; sprintf(ptr, "%p", (*i));
    KDEBUG(2) << "join: lr.results iterator *i = " << ptr << ", id = " << printID((*i)->id) << ", ip = " << (*i)->ip << endl;
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

    // if we believe our successor died, then start all over again
    if(flyweight.find(succ_id) == flyweight.end()) {
succ_died:
      if(!node()->alive())
        return;

      KDEBUG(2) << " restarting join" << endl;
      for(hash_map<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i) {
        char ptr[32]; sprintf(ptr, "%p", (*i).second);
        KDEBUG(2) << "Kademlia::join restart, deleting " << ptr << endl;
        delete (*i).second;
      }
      flyweight.clear();
      _root->collapse();
      goto join_restart;
    }
    
    // if the RPC failed, or the node is now dead, start over
    k_nodeinfo *ki = flyweight[succ_id];
    char ptr[32]; sprintf(ptr, "%p", ki);
    KDEBUG(2) << "join: iteration " << i << ", ki = " << ptr << ", ki->id is " << printID(ki->id) << ", ip = " << ki->ip << ", cpl = " << cpl << ", ptr = " << ptr << endl;
    record_stat(STAT_LOOKUP, 1, 0);
    if(!doRPC(ki->ip, &Kademlia::do_lookup, &la, &lr) || !node()->alive())
      goto succ_died;
    record_stat(STAT_LOOKUP, lr.results.size(), 0);

    for(nodeinfo_set::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
      if(flyweight.find((*i)->id) == flyweight.end() && (*i)->id != _id)
        insert((*i)->id, (*i)->ip);
        //  ... but not touch.  we didn't actually talk to the node.
  }

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
  for(hash_map<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i) {
    char ptr[32]; sprintf(ptr, "%p", (*i).second);
    KDEBUG(2) << "Kademlia::crash deleting " << ptr << endl;
    delete (*i).second;
  }

  // prepare for coming back up
  flyweight.clear();
  _root->collapse();
}

// }}}
// {{{ Kademlia::lookup
void
Kademlia::lookup(Args *args)
{
  NodeID key = args->nget<long long>("key", 0, 16);
  KDEBUG(1) << "Kademlia::lookup: " << printID(key) << endl;
  assert(node()->alive());

  lookup_args la(_id, ip(), key, true);
  lookup_result lr;

  Time before = now();
  do_lookup(&la, &lr);

  // get best match
  k_nodeinfo *ki = *lr.results.begin();
  assert(lr.results.size());
  char ptr[32]; sprintf(ptr, "%p", ki);
  KDEBUG(2) << "Kademlia::lookup, best result = " << ptr << endl;
  assert(ki);
  ki->checkrep();

  // now ping that node
  ping_args pa(ki->id, ki->ip);
  ping_result pr;
  record_stat(STAT_PING, 1, 0); // we send our ID
  assert(ki->ip > 0 && ki->ip <= 1024);
  if(!doRPC(ki->ip, &Kademlia::do_ping, &pa, &pr) && node()->alive()) {
    KDEBUG(2) << "Kademlia::lookup: ping RPC to " << Kademlia::printID(ki->id) << " failed " << endl;
    if(flyweight.find(ki->id) != flyweight.end())
      erase(ki->id);
  }
  record_stat(STAT_PING, 0, 0);

  Time after = now();
  KDEBUG(2) << "lookup: " << printID(key) << " is on " << Kademlia::printID(lr.rid) << endl;
  cout << "latency " << after - before << endl;
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
// {{{ Kademlia::do_lookup
void
Kademlia::do_lookup(lookup_args *largs, lookup_result *lresult)
{
  KDEBUG(1) << "Kademlia::do_lookup: node " << printID(largs->id) << " does lookup for " << printID(largs->key) << ", flyweight.size() = " << flyweight.size() << endl;
  assert(node()->alive());

  update_k_bucket(largs->id, largs->ip);

  // get a list of nodes sorted on distance to largs->key
  k_collect_closest successors(largs->key);
  _root->traverse(&successors, this);

  // we can't do anything but return ourselves
  if(!successors.results.size()) {
    KDEBUG(2) << "do_lookup: my tree is empty; returning myself." << endl;
    k_nodeinfo *me = New k_nodeinfo(_me);
    char ptr[32]; sprintf(ptr, "%p", me);
    KDEBUG(2) << "me = " << Kademlia::printID(_me->id) << ", ptr = " << ptr << endl;
    lresult->results.insert(me);
    lresult->rid = _id;
    return;
  }

  // initialize result set with the successors of largs->key
  closer::n = largs->key;
  for(set<NodeID, IDcloser>::const_iterator i = successors.results.begin(); i != successors.results.end(); ++i) {
    assert(flyweight.find(*i) != flyweight.end());
    k_nodeinfo *ki = flyweight[*i];
    k_nodeinfo *newki = New k_nodeinfo(ki);
    assert(newki);
    char ptr[32]; sprintf(ptr, "%p", newki);
    KDEBUG(2) << "do_lookup: initializing resultset, adding = " << Kademlia::printID(newki->id) << ", ptr = " << ptr << endl;
    lresult->results.insert(newki);
  }

  // also insert ourselves.  this can't really hurt, because it will be thrown
  // out of the set anyway, if it wasn't part of the answer.  this avoids
  // returning an empty set.
  k_nodeinfo *me = New k_nodeinfo(_me);
  char ptr[32]; sprintf(ptr, "%p", me);
  KDEBUG(2) << "insert me = " << Kademlia::printID(_me->id) << ", ptr = " << ptr << " to be safe " << endl;
  lresult->results.insert(me);

  // asked: one entry for each node we sent an RPC to
  // replied: one entry for each node we got a reply from
  // outstanding_rpcs: keyed by rpc-token, gets all info associated with RPC
  // rpcset: set of outstanding rpcs
  hash_map<NodeID, bool> asked;
  hash_map<NodeID, bool> replied;
  hash_map<unsigned, callinfo*> *outstanding_rpcs = New hash_map<unsigned, callinfo*>;
  assert(outstanding_rpcs);
  RPCSet *rpcset = New RPCSet;
  assert(rpcset);

  //
  // - stop if we've had an answer from the best k nodes in the result set
  // - find best alpha unqueried nodes in result set
  // - send an RPC to those alpha nodes
  // - wait for an RPC to come back
  // - update results set
  //
  while(true) {
    KDEBUG(2) << "do_lookup: top of the loop. outstaning rpcs = " << outstanding_rpcs->size() << endl;

    // stop if we've had an answer from the best k nodes we know of.
    // also mark the fact whether we asked everyone already
    unsigned k_counter = 0;
    bool we_are_done = false;
    bool asked_all = true;
    for(set<k_nodeinfo*, closer>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i) {
      KDEBUG(2) << "do_lookup: finished? considering result " << printID((*i)->id) << endl;
      if(asked.find((*i)->id) == replied.end())
        asked_all = false;

      if(replied.find((*i)->id) == replied.end()) {
        KDEBUG(2) << "do_lookup: finished? haven't gotten a reply from " << printID((*i)->id) << " yet, so no." << endl;
        break;
      }

      if(++k_counter >= Kademlia::k) {
        KDEBUG(2) << "do_lookup: finished? we've reached Kademlia::k.  so, yes." << endl;
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

    // we_are_done: we've had an answer from the best k nodes we know of
    if(we_are_done) {
      reap_info *ri = New reap_info();
      ri->k = this;
      ri->rpcset = rpcset;
      ri->outstanding_rpcs = outstanding_rpcs;
      KDEBUG(2) << "do_lookup: we_are_done, reaper has the following crap." << endl;
      for(hash_map<unsigned, callinfo*>::const_iterator i = outstanding_rpcs->begin(); i != outstanding_rpcs->end(); ++i) {
        char ptr[32]; sprintf(ptr, "%p", i->second);
        char ptr2[32]; sprintf(ptr2, "%p", i->second->ki);
        KDEBUG(2) << "do_lookup: callinfo = " << ptr << ", callinfo->ki = " << ptr2 << ", callinfo->ki->id = " << printID(i->second->ki->id) << endl;
      }

      ThreadManager::Instance()->create(Kademlia::reap, (void*) ri);
      break;
    }

    //
    // pick alpha nodes from the k best guys in the resultset, provided we
    // didn't already ask them, and, to the best of our knowledge, they're still
    // alive.
    //
    k_nodeinfo* toask[Kademlia::alpha];
    for(unsigned i=0; i<Kademlia::alpha; i++)
      toask[i] = 0;

    k_counter = 0;
    unsigned j = 0;
    for(set<k_nodeinfo*, closer>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i) {
      if(asked.find((*i)->id) == asked.end()) {
        KDEBUG(2) << "do_lookup: setting toask[" << j << "] to " << printID((*i)->id) << endl;
        toask[j++] = *i;
      }
      if(++k_counter >= Kademlia::k || j >= Kademlia::alpha)
        break;
    };


    //
    // send an RPC to all selected alpha nodes
    //
    for(unsigned i=0; i<Kademlia::alpha; i++) {
      if(toask[i] == 0)
        break;

      char ptr[32]; sprintf(ptr, "%p", toask[i]);
      KDEBUG(2) << "do_lookup: toask[ " << i << "] = " << ptr << ", id = " << printID(toask[i]->id) << ", ip = " << toask[i]->ip << endl;


      lookup_args *la = New lookup_args(_id, ip(), largs->id);
      lookup_result *lr = New lookup_result;
      assert(la && lr);
      assert(toask[i]);
      assert(toask[i]->ip <= 1024 && toask[i]->ip > 0);

      record_stat(STAT_LOOKUP, 1, 0);
      KDEBUG(2) << "do_lookup: asyncRPC to " << printID(toask[i]->id) << ", ip = " << toask[i]->ip << ", toask[" << i << "] = " << ptr << endl;
      unsigned rpc = asyncRPC(toask[i]->ip, &Kademlia::find_node, la, lr);
      callinfo *ci = New callinfo(New k_nodeinfo(toask[i]), la, lr);
      assert(ci);
      assert(rpc);
      rpcset->insert(rpc);
      asked[toask[i]->id] = true;

      // record this outstanding RPC
      assert(ci);
      (*outstanding_rpcs)[rpc] = ci;
    }

    //
    // now block on outstanding_rpcs
    //
receive_rpc:
    if(outstanding_rpcs->size()) {
      KDEBUG(2) << "do_lookup: thread " << threadid() << " going into rcvRPC, outstanding = " << outstanding_rpcs->size() << endl;

      bool ok;
      unsigned donerpc = rcvRPC(rpcset, ok);
      callinfo *ci = (*outstanding_rpcs)[donerpc];
      assert(ci);
      replied[ci->ki->id] = true;
      outstanding_rpcs->erase(outstanding_rpcs->find(donerpc));

      // if the node is dead, then remove this guy from our flyweight and, if he
      // was in our results, from the results.  but since we didn't make any
      // progress at all, we should give rcvRPC another chance.
      closer::n = largs->key;
      if(!ok) {
        KDEBUG(2) << "do_lookup: RPC to " << Kademlia::printID(ci->ki->id) << " failed" << endl;
        if(flyweight.find(ci->ki->id) != flyweight.end())
          erase(ci->ki->id);
        if(lresult->results.find(ci->ki) != lresult->results.end())
          lresult->results.erase(lresult->results.find(ci->ki));
        char ptr[32]; sprintf(ptr, "%p", ci);
        KDEBUG(2) << "do_lookup: RPC to " << Kademlia::printID(ci->ki->id) << " failed, deleting ci = " << ptr << endl;
        delete ci;
        goto receive_rpc;
      }

      //
      // RPC was ok
      //
      record_stat(STAT_LOOKUP, ci->lr->results.size(), 0);
      update_k_bucket(ci->lr->rid, ci->ki->ip);

      // put results that this node tells us in our results set
      for(nodeinfo_set::const_iterator i = ci->lr->results.begin(); i != ci->lr->results.end(); ++i) {
        char ptr[32]; sprintf(ptr, "%p", (*i));
        KDEBUG(2) << "do_lookup: RETURNED RESULT from " << Kademlia::printID(ci->lr->rid) << " for rcvRPC entry id = " << printID((*i)->id) << ", ip = " << (*i)->ip << ", ptr = " << ptr << endl;
        k_nodeinfo *n = New k_nodeinfo(*i);
        assert(n);
        sprintf(ptr, "%p", n);
        KDEBUG(2) << "do_lookup: RETURNED RESULT from " << Kademlia::printID(ci->lr->rid) << ", inserting as id = " << printID(n->id) << ", ip = " << n->ip << ", ptr = " << ptr << endl;
        pair<set<k_nodeinfo*, closer>::iterator, bool> rv = lresult->results.insert(n);
        if(!rv.second) {
          char ptr[32]; sprintf(ptr, "%p", n);
          KDEBUG(2) << "do_lookup: already in set, deleting " << ptr << endl;
          delete n;
        }
      }
      char ptr[32]; sprintf(ptr, "%p", ci);
      KDEBUG(2) << "do_lookup: bottom of loop, ci = " << ptr << endl;
      delete ci;
    }
  }

  // destroy all entries in results that are not part of the best k
  while(lresult->results.size() > Kademlia::k) {
    k_nodeinfo *i = *lresult->results.rbegin();
    char ptr[32]; sprintf(ptr, "%p", i);
    lresult->results.erase(lresult->results.find(i));
    KDEBUG(2) << "do_lookup: deleting in truncate id = " << printID(i->id) << ", ip = " << i->ip << ", ptr = " << ptr << endl;
    delete i;
  }

  KDEBUG(2) << "do_lookup: results that I'm returning to " << printID(largs->id) << " who was looking key " << printID(largs->key) << endl;
  for(set<k_nodeinfo*, closer>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i) {
    char ptr[32]; sprintf(ptr, "%p", (*i));
    KDEBUG(2) << "do_lookup: result: id = " << printID((*i)->id) << ", lastts = " << (*i)->lastts << ", ip = " << (*i)->ip << ", ptr = " << ptr << endl;
  }

  // put ourselves as replier
  lresult->rid = _id;
}

// }}}
// {{{ Kademlia::do_lookup_wrapper

// wrapper around do_lookup(lookup_args *largs, lookup_result *lresult)
// we're sending a do_lookup RPC to node ki asking him to lookup key.
//
void
Kademlia::do_lookup_wrapper(k_nodeinfo *ki, NodeID key)
{
  lookup_args la(_id, ip(), key, true);
  lookup_result lr;

  assert(node()->alive());

  assert(ki->ip);
  assert(ki->ip > 0 && ki->ip <= 1024);
  record_stat(STAT_LOOKUP, 1, 0);
  if(!doRPC(ki->ip, &Kademlia::do_lookup, &la, &lr) && node()->alive()) {
    KDEBUG(2) << "do_lookup_wrapper: RPC to " << Kademlia::printID(ki->id) << " failed " << endl;
    if(flyweight.find(ki->id) != flyweight.end())
      erase(ki->id);
    return;
  }
  record_stat(STAT_LOOKUP, lr.results.size(), 0);

  // caller not interested in result.  delete entries.
  if(!node()->alive())
    return;

  // update our k-buckets
  for(set<k_nodeinfo*, closer>::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
    update_k_bucket((*i)->id, (*i)->ip);
}

// }}}
// {{{ Kademlia::find_node
// Kademlia's FIND_NODE.  Returns the best k from its own k-buckets
void
Kademlia::find_node(lookup_args *largs, lookup_result *lresult)
{
  char ptr[32]; sprintf(ptr, "%p", lresult);
  KDEBUG(2) << "find_node invoked by " << Kademlia::printID(largs->id) << ", calling thread = " << largs->tid << ", inserting reply into lresult = " << ptr << endl;
  assert(node()->alive());

  update_k_bucket(largs->id, largs->ip);
  lresult->rid = _id;
  closer::n = largs->key;

  // deal with the empty case
  if(!flyweight.size()) {
    k_nodeinfo *p = New k_nodeinfo(_id, ip());
    assert(p);
    sprintf(ptr, "%p", p);
    KDEBUG(2) << "find_node: tree is empty. returning myself, ip = " << ip() << ", ptr = " << ptr << endl;
    pair<set<k_nodeinfo*, closer>::iterator, bool> rv = lresult->results.insert(p);
    if(!rv.second) {
      KDEBUG(2) << "find_node: insert failed, deleting ptr = " << ptr << endl;
      delete p;
    }
    return;
  }

  // fill result vector
  k_collect_closest successors(largs->key);
  _root->traverse(&successors, this);
  for(set<NodeID, Kademlia::closer>::const_iterator i = successors.results.begin(); i != successors.results.end(); ++i) {
    KDEBUG(2) << "find_node: going to find " << printID(*i) << " in flyweight" << endl;
    k_nodeinfo *p = New k_nodeinfo(flyweight[*i]);
    assert(p);
    sprintf(ptr, "%p", p);
    KDEBUG(2) << "find_node: adding, id = " << Kademlia::printID(*i) << ", p->id = " << Kademlia::printID(p->id) << " ip = " << p->ip << ", lastts = " << p->lastts << ", ptr = " << ptr << " to reply" << endl;
    pair<set<k_nodeinfo*, closer>::iterator, bool> rv = lresult->results.insert(p);
    if(!rv.second) {
      KDEBUG(2) << "find_node: insert failed (2), deleting ptr = " << ptr << endl;
      delete p;
    }
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

  k_nodeinfo *ni = New k_nodeinfo(id, ip);
  char ptr[32]; sprintf(ptr, "%p", ni);
  KDEBUG(2) << "Kademlia::insert " << Kademlia::printID(id) << ", ip = " << ip << ", ptr = " << ptr << endl;
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
  char ptr[32]; sprintf(ptr, "%p", flyweight[id]);
  KDEBUG(2) << "Kademlia::erase deleting id = " << printID(id) << ", ip = " << flyweight[id]->ip << ", ptr = " << ptr << endl;
  delete flyweight[id];
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
  assert(stat.size() > (unsigned) type);
  stat[type] += 20 + 4*num_ids + num_else;
  num_msgs[type]++;
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

    char ptr[32]; sprintf(ptr, "%p", (ci->ki));
    KDEBUG(2) << "Kademlia::reap ok = " << ok << ", ki = " << ptr << endl;
    if(ok)
      ri->k->update_k_bucket(ci->lr->rid, ci->ki->ip);
    else if(ri->k->flyweight.find(ci->ki->id) != ri->k->flyweight.end())
      ri->k->erase(ci->ki->id);

    ri->outstanding_rpcs->erase(ri->outstanding_rpcs->find(donerpc));
    sprintf(ptr, "%p", ci);
    KDEBUG(2) << "Kademlia::reap deleting ci = " << ptr << endl;
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

  // if already in set, and we're nog going to touch the timestamp, we're done.
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
    char ptr[32]; sprintf(ptr, "%p", ninfo);
    KDEBUG(2) << "k_nodes::insert truncating " << Kademlia::printID(ninfo->id) << endl;
    nodes.erase(nodes.find(ninfo));
    _nodes_by_id.erase(_nodes_by_id.find(ninfo));
    // dump it in the replacement cache, rather than completely throwing it
    // away. but don't let replacement_cache get too large.
    if(_parent->replacement_cache->size() < Kademlia::k)
      _parent->replacement_cache->insert(ninfo);
    else {
      KDEBUG(2) << "k_nodes::insert truncating " << Kademlia::printID(ninfo->id) << ", deleting " << ptr << endl;
      _parent->kademlia()->flyweight.erase(_parent->kademlia()->flyweight.find(ninfo->id));
      delete ninfo;
    }
  }

  checkrep(false);
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
// {{{ k_nodes::inrange
/*
 * pre: -
 * returns: whether or not this NodeID is within the range in nodes.
 */
bool
k_nodes::inrange(Kademlia::NodeID id) const
{
  // for KDEBUG purposes
  // Kademlia::NodeID _id = _parent->kademlia()->id();

  checkrep(false);

  if(nodes.size() <= 1)
    return false;
  k_nodeinfo *first = *_nodes_by_id.begin();
  k_nodeinfo *last = *_nodes_by_id.rbegin();
  // KDEBUG(2) <<  "k_nodes::inrange " << Kademlia::printID(id) << " between " << Kademlia::printID(first->id) << " and " << Kademlia::printID(last->id) << endl;
  return(id >= first->id && id <= last->id);
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
  for(nodeset_t::const_iterator i = nodes.begin(); i != nodes.end(); ++i) {
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
k_bucket::k_bucket(k_bucket *parent, Kademlia *k) : leaf(true), parent(parent)
{
  if(k) {
    _kademlia = k;
    _kademlia->setroot(this);
  } else
    _kademlia = parent->_kademlia;

  child[0] = child[1] = 0;
  nodes = New k_nodes(this);
  assert(nodes);
  replacement_cache = New set<k_nodeinfo*, Kademlia::younger>;
  checkrep();
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
  // Kademlia::NodeID _id = kademlia()->id();
  // if(!depth)
    // KDEBUG(1) << "k_nodes::traverser for " << traverser->type() << endl;
  checkrep();

  if(!leaf) {
    if(!k->node()->alive() || !child[0])
      return;
    child[0]->traverse(traverser, k, prefix + "0", depth+1, 0);
    if(!k->node()->alive() || !child[1])
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
  k_nodeinfo *kinfo = kademlia()->flyweight[id];

  if(depth == 0)
    KDEBUG(2) << "k_bucket::insert: id = " << Kademlia::printID(id) << ", ip = " << kinfo->ip << ", prefix = " << prefix << endl;

  unsigned leftmostbit = Kademlia::getbit(id, depth);
  // KDEBUG(2) << "insert: leftmostbit = " << leftmostbit << ", depth = " << depth << endl;

  //
  // NON-LEAF: RECURSE DEEPER
  //
  if(!leaf) {
    // KDEBUG(2) << "k_bucket::insert: child[" << leftmostbit << "] exists, descending" << endl;
    return child[leftmostbit]->insert(id, touch, init_state, prefix + (leftmostbit ? "1" : "0"), depth+1);
  }


  //
  // LEAF
  //
  // if this node is already in the k-bucket, update timestamp.
  // if this node is not in the k-bucket, and there's still space, add it.
  if(nodes->contains(id) || !nodes->full()) {
    // KDEBUG(2) << "k_bucket::insert nodes contains " << Kademlia::printID(id) << " OR is not full." << endl;
    assert(!nodes->inrange(kademlia()->id()));
    nodes->insert(id, touch);
    nodes->checkrep(false);
    if(nodes->inrange(kademlia()->id())) {
      divide(depth);
      checkrep();
    }
    return;
  }

  // when we're initializing, we're trying to insert every id in everyone's
  // k-buckets, so no need to put it in the replacement cache
  if(init_state) {
    checkrep();
    return;
  }

  // we're full already, just put in replacement cache, and truncate to
  // Kademlia::k
  //
  // XXX: this is wrong.  we haven't heard from this guy yet.
  // do we need a ping?
  if(replacement_cache->find(kinfo) != replacement_cache->end()) {
    // KDEBUG(2) << "k_bucket::insert removing " << Kademlia::printID(id) << " from replacement first." << endl;
    replacement_cache->erase(replacement_cache->find(kinfo));
  }

  kinfo->lastts = now();
  // KDEBUG(2) << "k_bucket::insert adding " << Kademlia::printID(id) << " to replacement cache." << endl;
  replacement_cache->insert(kinfo);
  // XXX: we can't really truncate the replacement cache.  it will break the
  // invariant that everything in the flyweight has to be in a k-bucket

  checkrep();
}
// }}}
// {{{ k_bucket::erase
void
k_bucket::erase(Kademlia::NodeID id, string prefix, unsigned depth)
{
  checkrep();

  // for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();
  k_nodeinfo *kinfo = kademlia()->flyweight[id];

  if(depth == 0)
    KDEBUG(2) << "k_bucket::erase: node = " << Kademlia::printID(id) << ", ip = " << kinfo->ip << ", prefix = " << prefix << endl;

  unsigned leftmostbit = Kademlia::getbit(id, depth);
  // unsigned myleftmostbit = Kademlia::getbit(kademlia()->id(), depth);
  KDEBUG(2) << "k_bucket::erase: leftmostbit = " << leftmostbit << ", depth = " << depth << endl;

  //
  // NODE WITH CHILD: recurse deeper.
  //
  if(!leaf) {
    // KDEBUG(2) << "k_bucket::erase: _child[" << leftmostbit << "] exists, descending" << endl;
    child[leftmostbit]->erase(id, prefix + (leftmostbit ? "1" : "0"), depth+1);
    checkrep();
    return;
  }

  //
  // this is a leaf.  erase the node.
  //
  if(nodes->contains(id))
    nodes->erase(id);
  else {
    // XXX: is true, but crashes...
    // assert(replacement_cache->find(kinfo) != replacement_cache->end());
    replacement_cache->erase(replacement_cache->find(kinfo));
    return;
  }

  //
  // get front guy from replacement cache
  //
  if(replacement_cache->size()) {
    k_nodeinfo *ki = *(replacement_cache->begin());
    nodes->insert(ki->id, false); // don't touch.  we're just moving it ove.r
    replacement_cache->erase(replacement_cache->find(ki));
  }

  //
  // Now if the range in this k-bucket includes my own ID, then split it,
  // otherwise just return the peer_t we just inserted.
  //
  if(!nodes->inrange(kademlia()->id())) {
    checkrep();
    return;
  }

  // divide contents of nodes over children
  divide(depth);
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
    delete child[0];
    delete child[1];
  } else {
    delete nodes;
    delete replacement_cache;
  }

  nodes = New k_nodes(this);
  assert(nodes);
  replacement_cache = New set<k_nodeinfo*, Kademlia::younger>;
  assert(replacement_cache);

  leaf = true;
  checkrep();
}
// }}}
//{{{ k_bucket::divide
// pre: own ID is in range of nodes
// post: this is turned into a non-leaf, contents divided over children.
void
k_bucket::divide(unsigned depth)
{
  assert(leaf);

  // for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();
  KDEBUG(2) << "k_bucket::divide" << endl;

  // can't do full checkrep because inrange() is true
  assert(nodes->inrange(kademlia()->id()));

  // we are transforming from leaf to node.  allocate the children
  leaf = false;
  child[0] = New k_bucket(this);
  child[1] = New k_bucket(this);
  assert(child[0]);
  assert(child[1]);

  // divide k-bucket
  for(k_nodes::nodeset_t::const_iterator i = nodes->nodes.begin(); i != nodes->nodes.end(); ++i) {
    unsigned bit = Kademlia::getbit((*i)->id, depth);
    child[bit]->nodes->insert((*i)->id, false);
  }

  // divide the replacement cache
  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = replacement_cache->begin(); i != replacement_cache->end(); ++i) {
    unsigned bit = Kademlia::getbit((*i)->id, depth);
    child[bit]->nodes->insert((*i)->id, false);
  }

  if(child[0]->nodes->inrange(kademlia()->id()))
    child[0]->divide(depth+1);
  if(child[1]->nodes->inrange(kademlia()->id()))
    child[1]->divide(depth+1);

  delete replacement_cache;
  delete nodes;

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

  if(!mykademlia->node()->alive())
    return;

  // return if any entry in this k-bucket is fresh
  for(set<k_nodeinfo*, Kademlia::older>::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
    // XXX: this is the sign of bad shit going on.  get out.
    // it doesn't really, really matter that we're not completing this round of
    // stabilization.  better than crashing the simulator, anyway.
    if(mykademlia->flyweight.find((*i)->id) == mykademlia->flyweight.end())
      return;

    Time lastts = mykademlia->flyweight[(*i)->id]->lastts;
    if(now() - lastts < Kademlia::refresh_rate)
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

  char ptr[32]; sprintf(ptr, "%p", ki);
  // KDEBUG(2) << "k_stabilizer: random lookup for " << Kademlia::printID(random_key) << ", sending to " << Kademlia::printID(ki->id) << ", ip = " << ki->ip << ", ptr = " << ptr << endl;

  // lookup the random key and update this k-bucket with what we learn
  mykademlia->do_lookup_wrapper(ki, random_key);
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
  k->checkrep();
  for(Kademlia::nodeinfo_set::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i)
    if((*i)->id == _n)
      _found++;

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
  for(Kademlia::nodeinfo_set::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
    char ptr[32]; sprintf(ptr, "%p", (*i));
    cout << spaces << "  " << Kademlia::printID((*i)->id) << ", lastts = " << (*i)->lastts << ", ptr = " << ptr << endl;
  }

  cout << spaces << "prefix: " << prefix << ", depth " << depth << ", replacement cache: " << endl;
  for(set<k_nodeinfo*, Kademlia::younger>::const_iterator i = k->replacement_cache->begin(); i != k->replacement_cache->end(); ++i) {
    char ptr[32]; sprintf(ptr, "%p", (*i));
    cout << spaces << "  " << Kademlia::printID((*i)->id) << ", lastts = " << (*i)->lastts << ", ptr = " << ptr << endl;
  }
}
// }}}
// {{{ k_delete::execute
void
k_delete::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  delete k;
}
// }}}
// {{{ k_check::execute
void
k_check::execute(k_bucket *k, string prefix, unsigned depth, unsigned leftright)
{
  k->checkrep();

  set<Protocol*> l = Network::Instance()->getallprotocols(k->kademlia()->proto_name());

  // go through all pointers in node
  for(Kademlia::nodeinfo_set::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
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

  for(Kademlia::nodeinfo_set::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
    char ptr[32]; sprintf(ptr, "%p", (*i));
    KDEBUG(2) << "k_collect_closest::execute: insert = " << Kademlia::printID((*i)->id) << ", lastts = " << (*i)->lastts << ", ip = " << (*i)->ip << ", ptr = " << ptr << " in resultset" << endl;
    results.insert((*i)->id);
  }

  KDEBUG(2) << "k_collect_closest::execute: resultset.size() = " << results.size() << endl;
}
// }}}
