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
#include <deque>
using namespace std;

unsigned Kademlia::k = 0;
unsigned Kademlia::alpha = 0;
unsigned Kademlia::debugcounter = 1;
unsigned Kademlia::controlmsg = 0;
unsigned Kademlia::joined = 0;
unsigned Kademlia::stabilize_timer = 0;
unsigned Kademlia::refresh_rate = 0;


// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(Node *n, Args a)
  : P2Protocol(n), _id(ConsistentHash::ip2chid(n->ip()))
{
  KDEBUG(1) << "ip: " << ip() << ", idsize = " << idsize << endl;

  if(!k)
    k = a.nget<unsigned>("k", 20, 10);
  if(!alpha)
    alpha = a.nget<unsigned>("alpha", 3, 10);
  if(!stabilize_timer)
    stabilize_timer = a.nget<unsigned>("stabilize_timer", 10000, 10);
  if(!refresh_rate)
    refresh_rate = a.nget<unsigned>("refresh_rate", 1000, 10);

  _me = New k_nodeinfo(_id, ip());
  _root = New k_bucket_leaf(this);
}

Kademlia::NodeID Kademlia::closer::n = 0;
// }}}
// {{{ Kademlia::join
void
Kademlia::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
  assert(wkn);

  // I am the well-known node
  if(wkn == ip()) {
    KDEBUG(1) << "Node " << printID(_id) << " is wellknown." << endl;
    joined++;
    cout << joined << ": " << Kademlia::printbits(_id) << " joined" << endl;
    delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
    return;
  }

  // lookup my own key with well known node.
  lookup_args la(_id, ip(), _id);
  lookup_result lr;
  KDEBUG(1) << "join: lookup my id." << endl;
  bool b = doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  assert(b);

  // put well known node in k-buckets and all results.
  KDEBUG(1) << "join: lr.rid = " << lr.rid << endl;
  insert(lr.rid, wkn);
  touch(lr.rid);
  for(nodeinfo_set::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
    if(flyweight.find((*i)->id) == flyweight.end())
      insert((*i)->id, (*i)->ip);

  // get our ``successor'' and compute length
  // of prefix we have in common
  k_collect_closest getsuccessor(_id, 1);
  _root->traverse(&getsuccessor);
  k_nodeinfo *ki = *getsuccessor.results.begin();

  unsigned cpl = common_prefix(_id, ki->id);
  KDEBUG(2) << "join: successor is " << printbits(ki->id) << ", cpl = " << cpl << endl;

  // all entries further away than him need to be refreshed.
  // see section 2.3
  for(int i=cpl-1; i>=0; i--) {
    // XXX: should be random
    lookup_args la(_id, ip(), (_id ^ (1<<i)));
    lookup_result lr;
    if(!doRPC(ki->ip, &Kademlia::do_lookup, &la, &lr))
      erase(ki->id);
    for(nodeinfo_set::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
      insert((*i)->id, (*i)->ip);
  }

  joined++;
  cout << joined << ": " << Kademlia::printbits(_id) << " joined" << endl;

  delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::lookup
void
Kademlia::lookup(Args *args)
{
  NodeID key = args->nget<long long>("key", 0, 16);
  KDEBUG(1) << "lookup: " << printID(key) << endl;

  lookup_args la(_id, ip(), key, true);
  lookup_result lr;

  Time before = now();
  do_lookup(&la, &lr);
  Time after = now();
  KDEBUG(1) << "lookup: " << printID(key) << " is on " << Kademlia::printbits(lr.rid) << endl;
  cout << "latency = " << after - before << endl;
}

// }}}
// {{{ Kademlia::do_lookup
void
Kademlia::do_lookup(lookup_args *largs, lookup_result *lresult)
{
  unsigned outstanding = 0;
  RPCSet rpcset;
  hash_map<unsigned, callinfo*> resultmap;
  hash_map<NodeID, bool> asked;
  lookup_args *la = 0;
  lookup_result *lr = 0;
  unsigned rpc = 0;
  deque<k_nodeinfo*> tasks;
  set<k_nodeinfo*, closer> *results = New set<k_nodeinfo*, closer>;

  // store caller id and ip
  NodeID callerID = largs->id;
  IPAddress callerIP = largs->ip;

  KDEBUG(2) << "Node " << printbits(callerID) << " does lookup for " << printID(largs->key) << ", thread = " << threadid() << endl;

  // get a list of nodes sorted on distance to largs->key
  k_collect_closest successors(largs->key);
  _root->traverse(&successors);

  // we can't do anything but return ourselves
  if(!successors.results.size()) {
    KDEBUG(2) << "do_lookup: my tree is empty; returning myself." << endl;
    KDEBUG(2) << "_me = " << Kademlia::printbits(_me->id) << endl;
    results->insert(_me);
    goto done;
  }

  // keep a map of which nodes we already asked
  for(set<k_nodeinfo*>::const_iterator i=results->begin(); i != results->end(); ++i) {
    KDEBUG(2) << "do_lookup: results entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
    asked[(*i)->id] = false;
    tasks.push_back(*i);
  }

  // keep a map of which nodes we already asked
  for(deque<k_nodeinfo*>::const_iterator i=tasks.begin(); i != tasks.end(); ++i)
    KDEBUG(2) << "do_lookup: tasks entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;

  // issue new RPCs
  while(true) {
    // find the first that we haven't asked yet.
    k_nodeinfo *toask = 0;
    if(tasks.size()) {
      toask = tasks.front();
      tasks.pop_front();
    }

    KDEBUG(2) << "do_lookup: top of the loop, outstanding = " << outstanding << endl;

    // we're done.
    if(!toask && !outstanding) {
      KDEBUG(2) << "do_lookup: nobody to ask, none outstanding. goodbye." << endl;
      break;
    }


    // there's a guy we didn't ask yet, and there's less than alpha outstanding
    // RPCs: send out another one.
    if(toask && outstanding < Kademlia::alpha) {
      KDEBUG(2) << "do_lookup: front task id = " << printbits(toask->id) << ", ip = " << toask->ip << endl;
      la = New lookup_args(_id, ip(), largs->id);
      lr = New lookup_result;
      assert(la && lr);
      assert(toask);
      assert(toask->ip <= 512 && toask->ip > 0);
      KDEBUG(2) << "do_lookup: thread " << threadid() << " doing find_node asyncRPC to ip=" << toask->ip << ", " << Kademlia::printbits(toask->id) << endl;
      if(la->controlmsg)
        controlmsg++;
      rpc = asyncRPC(toask->ip, &Kademlia::find_node, la, lr);
      KDEBUG(2) << "do_lookup: thread " << threadid() << " came back from find_node asyncRPC to " << toask->ip << ", " << Kademlia::printbits(toask->id) << endl;
      assert(rpc);
      rpcset.insert(rpc);
      callinfo *ci = New callinfo(toask->ip, la, lr);
      assert(ci);
      resultmap[rpc] = ci;
      asked[toask->id] = true;

      // don't block yet if there's more RPCs we can send
      if(++outstanding < alpha)
        continue;
    }


    // at this point we have the full number of outstanding RPCs, so block on
    // rcvRPC, but receive as many as we can while we're at it.  Use select() to
    // not block beyond the first rcvRPC.
    while(outstanding) {
      KDEBUG(2) << "do_lookup: thread " << threadid() << " going into rcvRPC, outstanding = " << outstanding << endl;
      KDEBUG(2) << "do_lookup: OUTSTANDING" << endl;
      for(hash_map<unsigned, callinfo*>::const_iterator i = resultmap.begin(); i != resultmap.end(); ++i) {
        KDEBUG(2) << "outstanding ip = " << i->second->ip << endl;
      }

      bool ok;
      unsigned donerpc = rcvRPC(&rpcset, ok);
      outstanding--;
      assert(donerpc);
      callinfo *ci = resultmap[donerpc];
      KDEBUG(2) << "do_lookup: still working for " << Kademlia::printbits(largs->id) << ", looking for " << Kademlia::printbits(largs->key) << ", thread = " << threadid() << endl;
      KDEBUG(2) << "do_lookup: after rcvRPC " << threadid() << " ci->ip = " << ci->ip << ", ci->rid = " << Kademlia::printbits(ci->lr->rid) << " outstanding = " << outstanding << endl;
      resultmap.erase(donerpc);

      // remove if RPC failed
      if(!ok) {
        erase(ci->lr->rid);
        KDEBUG(2) << "do_lookup: RPC failed" << endl;
        continue;
      }

      // update our own k-buckets
      touch(ci->lr->rid);

      closer::n = largs->key;
      set<k_nodeinfo*, closer> *newresults = New set<k_nodeinfo*, closer>;
      assert(newresults);

      for(nodeinfo_set::const_iterator i=ci->lr->results.begin(); i != ci->lr->results.end(); ++i) {
        KDEBUG(2) << "do_lookup: RETURNED RESULT for rcvRPC entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
        newresults->insert(*i);
      }

      for(set<k_nodeinfo*>::const_iterator i=results->begin(); i != results->end(); ++i) {
        KDEBUG(2) << "do_lookup: STORED RESULT for rcvRPC entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
        newresults->insert(*i);
      }

      KDEBUG(2) << "do_lookup: newresults->size() after sort = " << newresults->size() << endl;
      for(set<k_nodeinfo*, closer>::const_iterator i=newresults->begin(); i != newresults->end(); ++i) {
        KDEBUG(2) << "do_lookup: MERGED RESULT for rcvRPC entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
      }
      // EqualNodes en(largs->key);
      // newresults->erase(unique(newresults->begin(), newresults->end(), en), newresults->end());
      // KDEBUG(2) << "do_lookup: newresults->size() after unique = " << newresults->size() << endl;

      // cut off all entries larger than the first _k
      while(newresults->size() >= Kademlia::k) {
        k_nodeinfo *ki = *newresults->rbegin();
        newresults->erase(ki);
      }
      delete results;
      results = newresults;

      for(set<k_nodeinfo*, closer>::const_iterator i=results->begin(); i != results->end(); ++i) {
        KDEBUG(2) << "do_lookup: AFTER CUT RESULT for rcvRPC entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
      }

      // mark new nodes as not yet asked
      for(set<k_nodeinfo*, closer>::const_iterator i=results->begin(); i != results->end(); ++i)
        if(asked.find((*i)->id) == asked.end()) {
          asked[(*i)->id] = false;
          tasks.push_front(*i);
        }
      delete ci;
    }
  }

done:
  assert(!resultmap.size());

  KDEBUG(2) << "do_lookup, thread " << threadid() << ": done, was working for " << Kademlia::printbits(largs->id) << ", looking for " << Kademlia::printbits(largs->key) << endl;

  // this is the final answer
  // XXX: no longer sorting on close, but now on timestamp
  for(set<k_nodeinfo*, closer>::const_iterator i=results->begin(); i != results->end(); ++i) {
    (*i)->checkrep();
    lresult->results.insert(*i);
  }

  unsigned k = 0;

  for(set<k_nodeinfo*>::const_iterator i = lresult->results.begin(); i != lresult->results.end(); ++i)
    KDEBUG(2) << "do_lookup reply [" << k++ << "] : " << printbits((*i)->id) << endl;
  delete results;

  KDEBUG(2) << "do_lookup: replying to " << Kademlia::printbits(callerID) << endl;

  // put ourselves as replier
  lresult->rid = _id;

  // put the caller in the tree
  if(flyweight.find(callerID) == flyweight.end())
    insert(callerID, callerIP);
  touch(callerID);
}

// }}}
// {{{ Kademlia::do_lookup_wrapper

// wrapper around do_lookup(lookup_args *largs, lookup_result *lresult)
// we're sending a do_lookup RPC to node ki asking him to lookup key.
//
void
Kademlia::do_lookup_wrapper(k_nodeinfo *ki, NodeID key, set<k_nodeinfo*> *v)
{
  lookup_args la(_id, ip(), key, true);
  lookup_result lr;

  assert(ki->ip);
  controlmsg++;
  if(!doRPC(ki->ip, &Kademlia::do_lookup, &la, &lr)) {
    erase(ki->id);
    assert(false);
    return;
  }
  touch(ki->id);

  // caller not interested in result
  if(!v)
    return;

  for(nodeinfo_set::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
    v->insert(*i);
}

// }}}
// {{{ Kademlia::find_node
// Kademlia's FIND_NODE.  Returns the best k from its own k-buckets
void
Kademlia::find_node(lookup_args *largs, lookup_result *lresult)
{
  KDEBUG(2) << "find_node invoked by " << Kademlia::printbits(largs->id) << ", thread = " << threadid() << endl;

  // deal with the empty case
  if(!flyweight.size()) {
    KDEBUG(3) << "find_node: tree is empty. returning myself, ip = " << ip() << endl;
    // XXX: why a new one?
    k_nodeinfo *p = New k_nodeinfo(_id, ip());
    lresult->results.insert(p);
  } else {
    // fill result vector
    k_collect_closest successors(largs->key);
    _root->traverse(&successors);
    for(set<k_nodeinfo*, Kademlia::closer>::const_iterator i = successors.results.begin(); i != successors.results.end(); ++i)
      lresult->results.insert(*i);
  }

  // XXX: touch?
  insert(largs->id, largs->ip);
  lresult->rid = _id;
}

// }}}
// {{{ Kademlia::reschedule_stabilizer
void
Kademlia::reschedule_stabilizer(void *x)
{
  KDEBUG(1) << "reschedule_stabilizer" << endl;
  stabilize();
  delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::stabilize
void 
Kademlia::stabilize()
{
  KDEBUG(1) << "stabilize" << endl;

  k_stabilizer stab;
  _root->traverse(&stab);

  KDEBUG(1) << "notifyObservers" << endl;
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
  _root->traverse(&stab);
  return stab.stabilized();
}

// }}}
// {{{ Kademlia::init_state
// NASTY HACK to stabilize faster
void 
Kademlia::init_state(list<Protocol*> l)
{
  // just bloody call insert on everyone
  for(list<Protocol*>::const_iterator i = l.begin(); i != l.end(); ++i) {
    Kademlia *k = (Kademlia *) *i;
    insert(k->id(), k->node()->ip(), true);
  }
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
  assert(id);
  assert(flyweight.find(id) != flyweight.end());
  KDEBUG(1) << "Kademlia::touch " << Kademlia::printbits(id) << endl;


  // k_dumper dump;
  // _root->traverse(&dump);

  k_finder find(id);
  _root->traverse(&find);
  assert(find.found() == 1);

  if(!flyweight[id]->firstts)
    flyweight[id]->firstts = now();
  _root->insert(id);
}

// }}}
// {{{ Kademlia::insert
// pre: id and ip are valid, id is not yet in this flyweight
// post: id->ip mapping in flyweight, and k-bucket
//
void
Kademlia::insert(NodeID id, IPAddress ip, bool init_state)
{
  assert(id && ip);
  assert(flyweight.find(id) == flyweight.end());

  k_nodeinfo *ni = New k_nodeinfo(id, ip);
  assert(ni);
  flyweight[id] = ni;
  _root->insert(id, init_state);
}
// }}}
// {{{ Kademlia::erase
void
Kademlia::erase(NodeID id)
{
  assert(flyweight.find(id) != flyweight.end());
  _root->erase(id);
  delete flyweight[id];
  flyweight.erase(id);
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

// {{{ k_nodes::k_nodes
k_nodes::k_nodes(k_bucket_leaf *parent) : _parent(parent)
{
  assert(_parent);
  nodes.clear();
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
k_nodes::insert(Kademlia::NodeID n)
{
  checkrep(false);

  Kademlia::NodeID _id = _parent->kademlia()->id();
  KDEBUG(1) << "k_nodes::insert " << Kademlia::printbits(n) << endl;

  assert(n);
  assert(_parent->kademlia()->flyweight.find(n) != _parent->kademlia()->flyweight.end());
  k_nodeinfo *ninfo = _parent->kademlia()->flyweight[n];
  assert(ninfo);
  ninfo->checkrep();

  // if already in set: remove it since its position will change
  nodeset_t::iterator pos = nodes.find(ninfo);
  if(pos != nodes.end()) {
    KDEBUG(1) << "k_nodes::insert found " << Kademlia::printbits(n) << ", removing..." << endl;
    nodes.erase(pos);
    _nodes_by_id.erase(ninfo);
  }

  // update timestamp
  ninfo->lastts = now();
  if(!(nodes.insert(ninfo).second && _nodes_by_id.insert(ninfo).second))
    assert(false);

  if(nodes.size() > Kademlia::k) {
    k_nodeinfo *ninfo = *nodes.rbegin();
    nodes.erase(ninfo);
    _nodes_by_id.erase(ninfo);
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
  cout << "erase in" << endl;
  checkrep();
  assert(n);
  assert(_parent->kademlia()->flyweight.find(n) != _parent->kademlia()->flyweight.end());
  k_nodeinfo *ninfo = _parent->kademlia()->flyweight[n];
  ninfo->checkrep();
  assert(nodes.find(ninfo) != nodes.end());
  assert(_nodes_by_id.find(ninfo) != nodes.end());

  nodes.erase(ninfo);
  _nodes_by_id.erase(ninfo);

  checkrep();
  cout << "erase out" << endl;
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
  // XXX: for KDEBUG purposes
  Kademlia::NodeID _id = _parent->kademlia()->id();

  checkrep(false);

  if(nodes.size() <= 1)
    return false;
  k_nodeinfo *first = *_nodes_by_id.begin();
  k_nodeinfo *last = *_nodes_by_id.rbegin();
  KDEBUG(4) <<  "k_nodes::inrange " << Kademlia::printbits(id) << " between " << Kademlia::printbits(first->id) << " and " << Kademlia::printbits(last->id) << endl;
  return(id >= first->id && id <= last->id);
}
// }}}
// {{{ k_nodes::checkrep
void
k_nodes::checkrep(bool rangecheck) const
{
  assert(_parent);
  assert(_parent->nodes == this);
  assert(nodes.size() <= Kademlia::k);
  assert(nodes.size() == _nodes_by_id.size());
  assert(_parent->kademlia());
  assert(_parent->kademlia()->id());

  // all nodes are in flyweight
  // all nodes are unique
  // all nodes are also in _nodes_by_id
  hash_map<Kademlia::NodeID, bool> haveseen;
  for(set<k_nodeinfo*, Kademlia::older>::const_iterator i = nodes.begin(); i != nodes.end(); ++i) {
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
}
// }}}
// {{{ k_nodeinfo::checkrep
void
k_nodeinfo::checkrep() const
{
  assert(id);
  assert(ip);
  assert(firstts <= lastts);
}
// }}}

// {{{ k_bucket_node::k_bucket_node
k_bucket_node::k_bucket_node(k_bucket *parent) : k_bucket(parent, false)
{
  child[0] = New k_bucket_leaf(this);
  child[1] = New k_bucket_leaf(this);
  checkrep();
}
// }}}
// {{{ k_bucket_node::k_bucket_node
k_bucket_node::k_bucket_node(Kademlia *k) : k_bucket(0, false, k)
{
  child[0] = New k_bucket_leaf(this);
  child[1] = New k_bucket_leaf(this);
  checkrep();
}
// }}}
// {{{ k_bucket_node::checkrep
void
k_bucket_node::checkrep() const
{
  assert(child[0] && child[1]);
  assert(!leaf);
  k_bucket::checkrep();
}
// }}}

// {{{ k_bucket_leaf::k_bucket_leaf
k_bucket_leaf::k_bucket_leaf(k_bucket *parent) : k_bucket(parent, true)
{
  nodes = New k_nodes(this);
  replacement_cache = New set<k_nodeinfo*, Kademlia::younger>;
  assert(replacement_cache);
  checkrep();
}
// }}}
// {{{ k_bucket_leaf::k_bucket_leaf
k_bucket_leaf::k_bucket_leaf(Kademlia *k) : k_bucket(0, true, k)
{
  nodes = New k_nodes(this);
  nodes->checkrep();
  replacement_cache = New set<k_nodeinfo*, Kademlia::younger>;
  assert(replacement_cache);
  checkrep();
}
// }}}
// {{{ k_bucket_leaf::divide
// pre: own ID is in range of nodes
// post: this is replaced with a k_bucket_node, contents divided over children.
//       this is deleted.
k_bucket_node*
k_bucket_leaf::divide(unsigned depth)
{
  // XXX: for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();

  // can't do full checkrep because inrange() is true
  assert(nodes->inrange(kademlia()->id()));

  // now divide contents into separate buckets
  // XXX: we have to ping these guys?

  // Now we are a leaf, but we need to replace ourselves with a node rather than
  // a leaf.
  k_bucket_node *newnode = 0;
  if(parent) {
    newnode = New k_bucket_node(parent);
    k_bucket **pointertome = &(((k_bucket_node*)parent)->child[(((k_bucket_node*)parent)->child[0] == this ? 0 : 1)]);
    *pointertome = newnode;
  } else {
    assert(kademlia()->root() == this);
    newnode = New k_bucket_node(kademlia());
  }

  // create both children
  KDEBUG(4) <<  "k_bucket_leaf::divide creating subchildren" << endl;

  for(k_nodes::nodeset_t::const_iterator i = nodes->nodes.begin(); i != nodes->nodes.end(); ++i) {
    unsigned bit = Kademlia::getbit((*i)->id, depth);
    KDEBUG(4) <<  "k_bucket_leaf::divide on depth " << depth << ": pushed entry " << Kademlia::printbits((*i)->id) << " to side " << bit << endl;
    ((k_bucket_leaf*)newnode->child[bit])->nodes->insert((*i)->id);
  }

  if(((k_bucket_leaf*)newnode->child[0])->nodes->inrange(kademlia()->id()))
    ((k_bucket_leaf*)newnode->child[0])->divide(depth+1);
  if(((k_bucket_leaf*)newnode->child[1])->nodes->inrange(kademlia()->id()))
    ((k_bucket_leaf*)newnode->child[1])->divide(depth+1);

  // XXX: what to do with replacement cache?!
  delete this;
  newnode->checkrep();
  return newnode;
}
// }}}
// {{{ k_bucket_leaf::checkrep
void
k_bucket_leaf::checkrep() const
{
  assert(leaf);
  assert(nodes);
  assert(replacement_cache);
  nodes->checkrep();

  // no doubles in replacement_cache, none of them should be in nodes either,
  // and all entries should be sorted youngest first.
  hash_map<Kademlia::NodeID, bool> haveseen;
  set<k_nodeinfo*, Kademlia::younger>::const_iterator prev, cur;

  if(!replacement_cache->size())
    return;

  cur = prev = replacement_cache->begin();
  cur++;
  while(cur != replacement_cache->end()) {
    assert((*prev)->lastts >= (*cur)->lastts);
    assert(!nodes->contains((*cur)->id));
    assert(haveseen.find((*cur)->id) == haveseen.end());
    prev = cur++;
  }

  k_bucket::checkrep();

}
// }}}

// {{{ k_bucket::k_bucket
k_bucket::k_bucket(k_bucket *parent, bool leaf, Kademlia *k) : leaf(leaf), parent(parent)
{
  if(k) {
    _kademlia = k;
    _kademlia->setroot(this);
  } else
    _kademlia = parent->_kademlia;
  checkrep();
}
// }}}
// {{{ k_bucket::traverse
void
k_bucket::traverse(k_traverser *traverser, string prefix, unsigned depth)
{
  checkrep();

  // XXX: for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();
  if(!depth)
    KDEBUG(1) << "k_nodes::traverser for " << traverser->type() << endl;

  if(!leaf) {
    ((k_bucket_node*) this)->child[0]->traverse(traverser, prefix + "0", depth+1);
    ((k_bucket_node*) this)->child[1]->traverse(traverser, prefix + "1", depth+1);
    checkrep();
    return;
  }

  traverser->execute((k_bucket_leaf*) this, prefix, depth);

  // no checkrep.  the divide() may have killed us.
}
// }}}
// {{{ k_bucket::insert
// pre: id is in flyweight, its timestamp (lastts) has been set.
// post: if id was already in k-bucket, its position is updated.
//       if id was not in k-bucket :
//              if k-bucket has space, id is added
//              if k-bucket is full, id is put in replacement cache
void
k_bucket::insert(Kademlia::NodeID id, bool init_state, string prefix, unsigned depth)
{
  checkrep();
  assert(kademlia()->flyweight.find(id) != kademlia()->flyweight.end());

  // XXX: for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();
  k_nodeinfo *kinfo = kademlia()->flyweight[id];

  if(depth == 0)
    KDEBUG(4) << "insert: id = " << Kademlia::printbits(id) << ", ip = " << kinfo->ip << ", prefix = " << prefix << endl;

  unsigned leftmostbit = Kademlia::getbit(id, depth);
  KDEBUG(4) << "insert: leftmostbit = " << leftmostbit << ", depth = " << depth << endl;

  //
  // NON-LEAF: RECURSE DEEPER
  //
  if(!leaf) {
    KDEBUG(4) << "insert: child[" << leftmostbit << "] exists, descending" << endl;
    return ((k_bucket_node *) this)->child[leftmostbit]->insert(id, init_state, prefix + (leftmostbit ? "1" : "0"), depth+1);
  }


  //
  // LEAF
  //
  // if this node is already in the k-bucket, update timestamp.
  // if this node is not in the k-bucket, and there's still space, add it.
  k_nodes *nodes = ((k_bucket_leaf*)this)->nodes;
  if(nodes->contains(id) || !nodes->full()) {
    assert(!nodes->inrange(kademlia()->id()));
    nodes->insert(id);
    nodes->checkrep(false);
    if(nodes->inrange(kademlia()->id())) {
      k_bucket_node *newnode = ((k_bucket_leaf*)this)->divide(depth);
      newnode->checkrep();
    }
    return;
  }

  // when we're initializing, we're trying to insert every id in everyone's
  // k-buckets, so no need to put it in the replacement cache
  if(init_state) {
    checkrep();
    return;
  }

  // we're full already, just put in replacement cache.
  ((k_bucket_leaf*)this)->replacement_cache->insert(kinfo);

  checkrep();
}
// }}}
// {{{ k_bucket::erase
void
k_bucket::erase(Kademlia::NodeID id, string prefix, unsigned depth)
{
  checkrep();

  // XXX: for KDEBUG
  Kademlia::NodeID _id = kademlia()->id();
  IPAddress ip = kademlia()->flyweight[id]->ip;

  if(depth == 0)
    KDEBUG(4) << "erase: node = " << Kademlia::printbits(id) << ", ip = " << ip << ", prefix = " << prefix << endl;

  unsigned leftmostbit = Kademlia::getbit(id, depth);
  // unsigned myleftmostbit = Kademlia::getbit(kademlia()->id(), depth);
  KDEBUG(4) << "erase: leftmostbit = " << leftmostbit << ", depth = " << depth << endl;

  //
  // NODE WITH CHILD: recurse deeper.
  //
  if(!leaf) {
    KDEBUG(4) << "insert: _child[" << leftmostbit << "] exists, descending" << endl;
    ((k_bucket_node *) this)->child[leftmostbit]->erase(id, prefix + (leftmostbit ? "1" : "0"), depth+1);
    checkrep();
    return;
  }

  //
  // this is a leaf.  erase the node.
  //
  k_nodes *nodes = ((k_bucket_leaf*)this)->nodes;
  nodes->erase(id);

  //
  // get front guy from replacement cache
  //
  set<k_nodeinfo*, Kademlia::younger> *replacement_cache = ((k_bucket_leaf*)this)->replacement_cache;
  if(replacement_cache->size()) {
    k_nodeinfo *ki = *(replacement_cache->begin());
    nodes->insert(ki->id);
    replacement_cache->erase(ki);
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
  k_bucket_node *newnode = ((k_bucket_leaf*)this)->divide(depth);

  // can't call checkrep on this since divide() deleted it.
  newnode->checkrep();
}
// }}}
// {{{ k_bucket::checkrep
void
k_bucket::checkrep() const
{
  assert(_kademlia);

  if(!parent)
    assert(_kademlia->root() == this);
  else
    assert(!parent->leaf);
}
// }}}

// {{{ k_stabilizer::execute
void
k_stabilizer::execute(k_bucket_leaf *k, string prefix, unsigned depth)
{
  // XXX: for KDEBUG purposes
  Kademlia *mykademlia = k->kademlia();
  Kademlia::NodeID _id = mykademlia->id();

  if(k->nodes->nodes.size()) {
    // make safe copy of node IDs.
    vector<Kademlia::NodeID> idvec;
    for(set<k_nodeinfo*, Kademlia::older>::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
      assert(*i);
      idvec.push_back((*i)->id);
    }
    
    for(vector<Kademlia::NodeID>::const_iterator i = idvec.begin(); i != idvec.end(); ++i) {
      assert(mykademlia->flyweight.find(*i) != mykademlia->flyweight.end());
      Time lastts = mykademlia->flyweight[(*i)]->lastts;
      if(now() - lastts < Kademlia::refresh_rate)
        continue;

      // find the closest node to the ID we're looking for
      k_collect_closest getsuccessor(*i, 1);
      mykademlia->root()->traverse(&getsuccessor);
      k_nodeinfo *ki = *getsuccessor.results.begin();

      KDEBUG(1) << "threadid = " << threadid() << " stabilize: lookup for " << Kademlia::printbits(*i) << ", on " << Kademlia::printbits(ki->id) << endl;
      // will add it to the tree
      mykademlia->do_lookup_wrapper(ki, *i);
      KDEBUG(1) << "k_stabilizer: lookup for " << Kademlia::printbits(*i) << " returned" << endl;
    }
    return;
  }

  // we have no information on this part of the ID space.
  // lookup a random key
  Kademlia::NodeID mask = 0;
  for(unsigned i=0; i<depth; i++)
    mask |= (((Kademlia::NodeID) 1) << (Kademlia::idsize-depth-i));

  KDEBUG(1) << "k_stabilizer: mask = " << Kademlia::printbits(mask) << endl;
  Kademlia::NodeID random_key = _id & mask;

  k_collect_closest getsuccessor(random_key);
  mykademlia->root()->traverse(&getsuccessor);
  for(k_collect_closest::resultset_t::const_iterator i = getsuccessor.results.begin(); i != getsuccessor.results.end(); ++i) {
    KDEBUG(1) << "k_stabilizer: random lookup for " << Kademlia::printbits(random_key) << ", sending to " << Kademlia::printbits((*i)->id) << endl;
    mykademlia->do_lookup_wrapper((*i), random_key);
  }
}
// }}}

// {{{ k_stabilized::execute
void
k_stabilized::execute(k_bucket_leaf *k, string prefix, unsigned depth)
{
  // XXX: for debugging purposes only
  Kademlia::NodeID _id = k->kademlia()->id();

  // 
  // example node ID 01001011
  //
  // look at second entry in finger table should yield a node ID
  // less than 01001001, but larger than 01001000
  // (we preserved prefix)
  //
  // for fourth entry in finger table should
  // less than 01000111, larger than 01000000
  //

  // if we know about nodes in this part of the ID space, great.
  if(!k->nodes->empty())
    return;

  KDEBUG(2) << "stabilized: " << prefix << " not present, depth = " << depth << ", prefix = " << prefix << endl;
  Kademlia::NodeID lower_mask = 0;

  //
  // Node claims there is no node to satisfy this entry in the finger table.
  // Check whether that is true.
  //

  // On every iteration we add another bit.  lower_mask looks like 111...000,
  // but we use it as 000...111 by ~-ing it.
  for(unsigned i=0; i<depth; i++)
    lower_mask |= (((Kademlia::NodeID)1) << (Kademlia::idsize-i-1));
  KDEBUG(4) << "stabilized: lower_mask on depth " << depth << " = " << Kademlia::printbits(lower_mask) << endl;

  // flip the bit, and turn all bits to the right of the flipped bit into
  // zeroes.
  Kademlia::NodeID lower = _id ^ (((Kademlia::NodeID) 1) << (Kademlia::idsize-depth-1));
  KDEBUG(4) << "stabilized: lower before mask = " << Kademlia::printbits(lower) << endl;
  lower &= lower_mask;
  KDEBUG(4) << "stabilized: lower after mask = " << Kademlia::printbits(lower) << endl;

  // upper bound is the id with one bit flipped and all bits to the right of
  // that turned into ones.
  Kademlia::NodeID upper = lower | ~lower_mask;

  KDEBUG(4) << "stabilized: lower = " << Kademlia::printbits(lower) << endl;
  KDEBUG(4) << "stabilized: upper = " << Kademlia::printbits(upper) << endl;

  // yields the node with smallest id greater than lower
  vector<Kademlia::NodeID>::const_iterator it = upper_bound(_v->begin(), _v->end(), lower);

  // check that this is smaller than upper.  if so, then this node would
  // qualify for this entry in the finger table, so the node that says there
  // is no such is WRONG.
  if(it != _v->end() && *it <= upper) {
    KDEBUG(4) << "stabilized: prefix " << prefix << " on depth " << depth << " is invalid, but " << Kademlia::printbits(*it) << " matches " << endl;
    KDEBUG(4) << "stabilized: lowermask = " << Kademlia::printbits(lower_mask) << endl;
    KDEBUG(4) << "stabilized: ~lowermask = " << Kademlia::printbits(~lower_mask) << endl;
    KDEBUG(4) << "stabilized: lower = " << Kademlia::printbits(lower) << endl;
    KDEBUG(4) << "stabilized: upper = " << Kademlia::printbits(upper) << endl;
    _stabilized = false;
  }
  // assert(false);
}
// }}}

// {{{ k_finder::execute
void
k_finder::execute(k_bucket_leaf *k, string prefix, unsigned depth)
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
k_dumper::execute(k_bucket_leaf *k, string prefix, unsigned depth)
{
  string spaces = "";
  for(unsigned i=0; i<depth; i++)
    spaces += "  ";
  cout << spaces << "prefix: " << prefix << endl;
  for(Kademlia::nodeinfo_set::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i)
    cout << spaces << "  " << Kademlia::printbits((*i)->id) << ", lastts = " << (*i)->lastts << endl;
}
// }}}

// {{{ k_collect_closest::k_collect_closest
k_collect_closest::k_collect_closest(Kademlia::NodeID n, unsigned best) :
  k_traverser("k_collect_closest"), _node(n) , _best(best)
{
  Kademlia::closer::n = n;
  _lowest = (Kademlia::NodeID) -1;
  results.clear();
  checkrep();
}
// }}}
// {{{ k_collect_closest::execute
void
k_collect_closest::execute(k_bucket_leaf *k, string prefix, unsigned depth)
{
  checkrep();
  k->checkrep();

  for(Kademlia::nodeinfo_set::const_iterator i = k->nodes->nodes.begin(); i != k->nodes->nodes.end(); ++i) {
    // skip this entry if it's worse than the worst we have so far
    // XXX: proximity?
    // otherwise add it to resultset
    //
    // NB: _lowest is not updated in every iteration, so it may be too low, but
    // that's ok as long as it isn't too high.  it's just an optimization.
    if(results.size() >= _best && Kademlia::distance((*i)->id, _node) >= _lowest)
      continue;
    results.insert(*i);
  }

  // prune the resultset
  while(results.size() > _best) {
    k_nodeinfo *ki = *results.rbegin();
    results.erase(ki);
  }

  if(!results.size())
    return;

  k_nodeinfo *last = *results.rbegin();
  assert(last);
  _lowest = Kademlia::distance(last->id, _node);

  checkrep();
}
// }}}
// {{{ k_collect_closest::checkrep
void
k_collect_closest::checkrep()
{
  assert(_best);
  assert(results.size() <= _best);
  if(results.size() == 0)
    assert(_lowest == (Kademlia::NodeID) -1);
  else {
    k_nodeinfo *last = *results.rbegin();
    assert(last);
    assert(_lowest == Kademlia::distance(last->id, _node));
  }
}
// }}}
