// {{{ headers
#include "kademlia.h"
#include "packet.h"
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "../utils/skiplist.h"
#define TMG_DMALLOC
#include "tmgdmalloc.h"
#include "p2psim.h"
using namespace std;

#define STABLE_TIMER 500 //use a["stabtimer"] to set stabilization timer
#define KADEMLIA_REFRESH 1000

unsigned kdebugcounter = 1;
unsigned Kademlia::_k = 0;
unsigned Kademlia::_alpha = 0;

unsigned k_bucket::_k = 0;
unsigned k_bucket_tree::_k = 0;
k_bucket_tree::NodeID k_bucket_tree::DistCompare::_key;

//
// Notes
//
// FIND_VALUE, STORE
// Augmenting k-buckets with round trip times so as to choose the best alpha.
// Paper isn't clear about second step in lookup.  Can we send another alpha for
//    EACH reply?  In other words: can we ever have more than alpha outstanding
//    RPCs?
// Expire keys after 24 hours.  Republish if necessary.
// FIND_VALUE should immediately return when value comes in.
// Initiator should store key on closest node that did not return the key/value
//     pair.
// Over-caching avoidance policy
// Refreshing buckets???
// Efficient key republishing
//
// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(Node *n, Args a)
  : DHTProtocol(n), _id(ConsistentHash::ip2chid(n->ip()) & 0x0000ffff)
{
  KDEBUG(1) << "ip: " << ip() << endl;
  _values.clear();

  if(!_k)
    _k = a.nget<unsigned>("k", 20, 10);
  if(!_alpha)
    _alpha = a.nget<unsigned>("alpha", 3, 10);

  // precompute masks
  // if(!_rightmasks[0]) {
  //   NodeID mask = 0;
  //   _rightmasks[0] = (NodeID) -1;
  //   for(unsigned i=1; i<idsize; i++) {
  //     mask |= (1<<(i-1));
  //     _rightmasks[i] = ~mask;
  //   }
  // }
  _tree = New(k_bucket_tree, this);
  assert(_tree);
}

// }}}
// {{{ Kademlia::~Kademlia
Kademlia::~Kademlia()
{
}

// }}}
// {{{ Kademlia::join
void
Kademlia::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
  if(!_wkn)
    _wkn = wkn;

  if(wkn == ip()) {
    KDEBUG(1) << "Node " << printID(_id) << " is wellknown." << endl;
    return;
  }

  // lookup my own key with well known node.
  lookup_args la(_id, ip(), _id);
  lookup_result lr;
  KDEBUG(1) << "join: lookup my id.  included ip = " << la.ip << endl;
  bool b = doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  assert(b);

  // put well known node in k-buckets and all results.
  _tree->insert(lr.rid, wkn);
  _tree->insert(&(lr.results));

  // get our ``successor'' and compute length
  // of prefix we have in common
  peer_t *p = lr.results[0];
  unsigned cpl = common_prefix(_id, p->id);
  KDEBUG(2) << "join: successor is " << printbits(p->id) << ", cpl = " << cpl << endl;

  // all entries further away than him need to be refreshed.
  // see section 2.3
  for(int i=cpl-1; i>=0; i--) {
    // XXX: should be random
    lookup_args la(_id, ip(), (_id ^ (1<<i)));
    lookup_result lr;
    if(!doRPC(p->ip, &Kademlia::do_lookup, &la, &lr))
      _tree->erase(p->id);
    _tree->insert(&(lr.results));
  }

  // now get the keys from our successor
  transfer_args ta(_id, ip());
  transfer_result tr;
  tr.values = &_values;
  KDEBUG(2) << "join: Node " << printbits(_id) << " initiating transfer from " << printbits(p->id) << endl;
  if(!doRPC(p->ip, &Kademlia::do_transfer, &ta, &tr))
    _tree->erase(p->id);

  delaycb(STABLE_TIMER, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::leave
void
Kademlia::leave(Args*)
{
  cout << "Kademlia leave" << endl;
}

// }}}
// {{{ Kademlia::insert
void
Kademlia::insert(Args *args)
{
  insert_args ia;
  insert_result ir;

  ia.id = _id;
  ia.ip = ip();

  ia.key = args->nget<NodeID>("key");
  ia.val = args->nget<Value>("val");

  KDEBUG(2) << "insert " << printID(ia.key) << ":" << ia.val << endl;
  do_insert(&ia, &ir);
}

// }}}
// {{{ Kademlia::do_insert
void
Kademlia::do_insert(insert_args *iargs, insert_result *iresult)
{
  // insert caller into tree
  _tree->insert(iargs->id, iargs->ip);

  lookup_args la(_id, ip(), iargs->key);
  lookup_result lr;
  do_lookup(&la, &lr);

  // if we are the successor for this key, we store it.
  peer_t *p = lr.results[0];
  if(p->id == _id) {
    KDEBUG(2) << "Node " << printID(_id) << " storing " << printID(iargs->key) << ":" << iargs->val << "." << endl;
    _values[iargs->key] = iargs->val;
    return;
  }

  // we're not the one to insert it.  forward it.
  if(!doRPC(p->ip, &Kademlia::do_insert, iargs, iresult))
    _tree->erase(p->id);
}

// }}}
// {{{ Kademlia::lookup
void
Kademlia::lookup(Args *args)
{
  KDEBUG(1) << "Kademlia lookup" << endl;
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


  // store caller id and ip
  NodeID callerID = largs->id;
  IPAddress callerIP = largs->ip;

  KDEBUG(2) << "Node " << printbits(callerID) << " does lookup for " << printID(largs->key) << endl;

  // fill it with the best that i know of
  vector<peer_t*> *results = New(vector<peer_t*>);
  assert(results);
  _tree->get(largs->key, results);

  // we can't do anything but return ourselves
  if(!results->size()) {
    KDEBUG(2) << "do_lookup: my tree is empty; returning myself." << endl;
    peer_t *p = New(peer_t, _id, ip());
    lresult->results.push_back(p);
    goto done;
  }

  // keep a map of which nodes we already asked
  for(vector<peer_t*>::const_iterator i=results->begin(); i != results->end(); ++i) {
    KDEBUG(2) << "do_lookup: results entry id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
    asked[(*i)->id] = false;
  }

  // issue new RPCs
  while(true) {
    // find the first that we haven't asked yet.
    peer_t *toask = 0;
    assert(toask == 0);
    for(vector<peer_t*>::const_iterator i=results->begin(); i != results->end(); ++i) {
      KDEBUG(2) << "do_lookup: considering for asyncRPC id = " << printbits((*i)->id) << ", ip = " << (*i)->ip << endl;
      if(!asked[(*i)->id]) {
        toask = *i;
        KDEBUG(2) << "do_lookup: taken" << endl;
        break;
      }
      KDEBUG(2) << "do_lookup: already considered" << endl;
    }

    // we're done.
    if(!toask && !outstanding)
      break;

    // there's a guy we didn't ask yet, and there's less than alpha outstanding
    // RPCs: send out another one.
    if(toask && outstanding < _alpha) {
      la = New(lookup_args, _id, ip(), largs->id);
      lr = New(lookup_result);
      assert(la && lr);
      KDEBUG(2) << "do_lookup: going into asyncRPC" << endl;
      assert(toask);
      assert(toask->ip <= 512 && toask->ip > 0);
      rpc = asyncRPC(toask->ip, &Kademlia::find_node, la, lr);
      KDEBUG(2) << "do_lookup: returning from asyncRPC" << endl;
      assert(rpc);
      rpcset.insert(rpc);
      resultmap[rpc] = New(callinfo, toask->ip, la, lr);
      asked[toask->id] = true;

      // don't block yet if there's more RPCs we can send
      if(++outstanding < _alpha)
        continue;
    }


    // at this point we have the full number of outstanding RPCs, so block on
    // rcvRPC, but receive as many as we can while we're at it.  Use select() to
    // not block beyond the first rcvRPC.
    do {
      KDEBUG(2) << "do_lookup: going into rcvRPC" << endl;
      unsigned donerpc = rcvRPC(&rpcset);
      KDEBUG(2) << "do_lookup: rcvRPC returned" << endl;
      outstanding--;
      assert(donerpc);
      callinfo *ci = resultmap[donerpc];
      resultmap.erase(donerpc);

      // update our own k-buckets
      _tree->insert(ci->lr->rid, ci->ip);

      // merge both tables and cut out everything after the first k.
      SortNodes sn(largs->key);
      vector<peer_t*> *newresults = New(vector<peer_t*>, results->size() + ci->lr->results.size());
      assert(newresults);
      merge(results->begin(), results->end(), 
            ci->lr->results.begin(), ci->lr->results.end(),
            newresults->begin(), sn);

      // cut off all entries larger than the first _k
      if(_k < newresults->size())
        newresults->resize(_k);
      Delete(results);
      results = newresults;

      // mark new nodes as not yet asked
      for(vector<peer_t*>::const_iterator i=results->begin(); i != results->end(); ++i)
        if(asked.find((*i)->id) == asked.end())
          asked[(*i)->id] = false;
      Delete(ci);
    } while(select(&rpcset));
  }

done:
  assert(!resultmap.size());

  KDEBUG(2) << "do_lookup: done" << endl;
  // this is the answer
  lresult->results = *results;
  Delete(results);

  // put the caller in the tree
  _tree->insert(callerID, callerIP);
}

// }}}
// {{{ Kademlia::do_lookup_wrapper

// wrapper around do_lookup(lookup_args *largs, lookup_result *lresult)
// if use_ip == 0, use the well-known node
void
Kademlia::do_lookup_wrapper(peer_t *p, Kademlia::NodeID key, 
    vector<peer_t*> *v)
{
  lookup_args la(_id, ip(), key);
  lookup_result lr;

  if(!doRPC(p->ip ? p->ip : _wkn, &Kademlia::do_lookup, &la, &lr))
    _tree->erase(p->id);

  if(v)
    copy(lr.results.begin(), lr.results.end(), v->begin());
}

// }}}
// {{{ Kademlia::find_node
// Kademlia's FIND_NODE.  Returns the best k from its own k-buckets
void
Kademlia::find_node(lookup_args *largs, lookup_result *lresult)
{
  // deal with the empty case
  if(_tree->empty()) {
    KDEBUG(3) << "do_lookup: tree is empty. returning myself, ip = " << ip() << endl;
    peer_t *p = New(peer_t, _id, ip());
    lresult->results.push_back(p);
    goto done;
  }

  // fill result vector
  _tree->get(largs->key, &lresult->results);

done:
  _tree->insert(largs->id, largs->ip);
  lresult->rid = _id;
}

// }}}
// {{{ Kademlia::do_ping
void
Kademlia::do_ping(ping_args *pargs, ping_result *presult)
{
  // insert caller into our tree
  _tree->insert(pargs->id, pargs->ip);
}

// }}}
// {{{ Kademlia::do_ping_wrapper
bool
Kademlia::do_ping_wrapper(peer_t *p)
{
  assert(p->ip);
  ping_args pa(_id, ip());
  ping_result pr;

  if(!doRPC(p->ip, &Kademlia::do_ping, &pa, &pr)) {
    _tree->erase(p->id);
    return false;
  }
  return true;
}

// }}}
// {{{ Kademlia::stabilized
bool
Kademlia::stabilized(vector<NodeID> lid)
{
  // __tmg_dmalloc_stats();
  return _tree->stabilized(lid);
}

// }}}
// {{{ Kademlia::stabilize
void 
Kademlia::stabilize()
{
  _tree->stabilize();
}

// }}}
// {{{ Kademlia::reschedule_stabilizer
void
Kademlia::reschedule_stabilizer(void *x)
{
  // if stabilize blah.
  KDEBUG(1) << "reschedule_stabilizer" << endl;
  stabilize();
  delaycb(STABLE_TIMER, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::do_transfer
//
// XXX: this is scary because we're deleting nodes before the other guy has
// them.  what if the reply fails?
void
Kademlia::do_transfer(transfer_args *targs, transfer_result *tresult)
{
  // insert caller into our tree
  _tree->insert(targs->id, targs->ip);

  KDEBUG(2) << "handle_transfer to node " << printbits(targs->id) << "\n";
  if(!_values.size())
    return;

  // copy all key/value pairs that targs->id is supposed to have
  for(hash_map<NodeID, Value>::const_iterator pos = _values.begin(); pos != _values.end(); ++pos) {
    if(distance(pos->first, _id) < distance(pos->first, targs->id))
      continue;
    tresult->values->insert(*pos);
    _values.erase(pos->first);
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
  return (n & (1<<((sizeof(NodeID)*8)-i-1))) ? 1 : 0;
}

// }}}
// {{{ Kademlia::printbits
string
Kademlia::printbits(NodeID id)
{
  char buf[128];

  unsigned j=0;
  for(int i=idsize-1; i>=0; i--)
    sprintf(&(buf[j++]), "%u", (id >> i) & 0x1);
  // sprintf(&(buf[j]), ":%llx", id);
  sprintf(&(buf[j]), ":%hx", id);

  return string(buf);
}

// }}}
// {{{ Kademlia::printID
string
Kademlia::printID(NodeID id)
{
  char buf[128];
  sprintf(buf, "%x", id);
  return string(buf);
}

// }}}
// {{{ Kademlia::distance
Kademlia::NodeID
Kademlia::distance(Kademlia::NodeID from, Kademlia::NodeID to)
{
  return from ^ to;
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
// {{{ Kademlia::crash
void
Kademlia::crash(Args*)
{
  cout << "Kademlia crash" << endl;
}

// }}}
// {{{ Kademlia::dump
void
Kademlia::dump()
{
  if(!verbose)
    return;

  cout << "*** DUMP FOR " << printbits(_id) << endl;
  cout << "   *** -------------------------- ***" << endl;
  _tree->dump();
  cout << "   *** -------------------------- ***" << endl;
}

// }}}

// {{{ k_bucket_tree::k_bucket_tree
k_bucket_tree::k_bucket_tree(Kademlia *k) : _self(k)
{
  _nodes.clear();
  _root = New(k_bucket, _self, this);

  _id = _self->id(); // for KDEBUG purposes only
  if(!_k)
    _k = Kademlia::k();
}

// }}}
// {{{ k_bucket_tree::~k_bucket_tree
k_bucket_tree::~k_bucket_tree()
{
  Delete(_root);
}

// }}}
// {{{ k_bucket_tree::insert (pair)
void
k_bucket_tree::insert(NodeID id, IPAddress ip)
{
  if(id == _id)
    return;

  peer_t *p = 0;
  if((p = _root->insert(id, ip)))
    _nodes[id] = p;
}

// }}}
// {{{ k_bucket_tree::insert (vector)
void
k_bucket_tree::insert(vector<peer_t*> *v)
{
  for(unsigned j=0; j<v->size(); j++)
    if((*v)[j]->id != _id)
      _root->insert((*v)[j]->id, (*v)[j]->ip);
}

// }}}
// {{{ k_bucket_tree::erase
void
k_bucket_tree::erase(NodeID id)
{
  KDEBUG(2) << "k_bucket_tree::erase: id = " << Kademlia::printbits(id) << endl;
  _nodes.erase(id);
  _self->dump();
}

// }}}
// {{{ k_bucket_tree::stabilize
void 
k_bucket_tree::stabilize()
{
  _root->stabilize();
}

// }}}
// {{{ k_bucket_tree::stabilized
bool
k_bucket_tree::stabilized(vector<NodeID> lid)
{
  return _root->stabilized(lid);
}

// }}}
// {{{ k_bucket_tree::get
void
k_bucket_tree::get(NodeID key, vector<peer_t*> *v, unsigned nbest)
{
  k_bucket_tree::DistCompare::_key = key;
  skiplist<best_entry, NodeID, &best_entry::dist, &best_entry::_sortlink, DistCompare> best;

  // printf("------ k_bucket_tree::get START ---------\n");

  unsigned best_entries = 0;
  for(hash_map<NodeID, peer_t*>::const_iterator i = _nodes.begin(); i != _nodes.end(); i++) {
    // if best still has space OR
    // if this entry is better than the worst entry, then add new if it doesn't
    // exist, or add to existing.
    NodeID dist = Kademlia::distance(i->second->id, key);
    if(best_entries < nbest || dist < best.last()->dist) {
      struct best_entry *be = 0;
      if(!(be = best.search(dist))) {
        be = New(best_entry);
        be->dist = dist;
        bool b = best.insert(be);
        assert(b);
      }
      be->peers.push_back(i->second);
      best_entries++;
    }

    if(best_entries > nbest) {
      assert(best_entries == nbest+1);
      struct best_entry *be = best.last();
      be->peers.pop_back();
      if(!be->peers.size()) {
        best.remove(be->dist);
        Delete(be);
      }
      best_entries--;
    }
  }

  // copy the results into the vector
  best_entry *next = 0;
  for(best_entry *cur = best.first(); cur; cur = next) {
    next = best.next(cur);
    for(vector<peer_t*>::const_iterator j = cur->peers.begin(); j != cur->peers.end(); ++j)
      v->push_back(*j);
    Delete(cur);
  }

#if 0
  // XXX heinously inefficient, but who cares for now.
  vector<peer_t*> tmp;
  KDEBUG(2) << "k_bucket_tree::get putting crap in tmp" << endl;
  for(hash_map<NodeID, peer_t*>::const_iterator i = _nodes.begin(); i != _nodes.end(); i++) {
    KDEBUG(2) << "k_bucket_tree::get calling assert for id = " << Kademlia::printbits(i->first) << endl;
    assert(i->second);
    tmp.push_back(i->second);
  }

  KDEBUG(2) << "k_bucket_tree::get tmp BEFORE sort" << endl;
  for(vector<peer_t*>::const_iterator i = tmp.begin(); i != tmp.end(); ++i)
    KDEBUG(2) << "k_bucket_tree: [" << j++ << "] " << Kademlia::printbits((*i)->id) << " -> " << (*i)->ip << endl;


  SortNodes sn(key);
  sort(tmp.begin(), tmp.end(), sn);

  j = 0;
  KDEBUG(2) << "k_bucket_tree::get tmp AFTER sort" << endl;
  for(vector<peer_t*>::const_iterator i = tmp.begin(); i != tmp.end(); ++i)
    KDEBUG(2) << "k_bucket_tree: [" << j++ << "] " << Kademlia::printbits((*i)->id) << " -> " << (*i)->ip << ", dist = " << Kademlia::printbits((*i)->id ^ key) << endl;

  j = 0;
  for(vector<peer_t*>::const_iterator i = tmp.begin(); i != tmp.end() && j<_k; ++i, ++j) {
    KDEBUG(2) << "k_bucket_tree::get [" << j << "] " << Kademlia::printbits((*i)->id) << " -> " << (*i)->ip << ", dist = " << Kademlia::printbits((*i)->id ^ key) << endl;
    v->push_back(*i);
  }
  // _root->get(key, v);
#endif
}


// }}}
// {{{ k_bucket_tree::random_node
peer_t*
k_bucket_tree::random_node()
{
  unsigned r = 1 + (unsigned)(((float) _nodes.size())*rand() / (RAND_MAX+1.0));
  return _nodes[r];
}

// }}}

// {{{ k_bucket::k_bucket
k_bucket::k_bucket(Kademlia *k, k_bucket_tree *root) : _leaf(false), _self(k), _root(root)
{
  _child[0] = _child[1] = 0;
  _id = _self->id(); // for KDEBUG purposes only

  typedef set<peer_t*, SortedByLastTime> Xset; // tmg dmalloc doesn't like comma's
  _nodes = New(Xset);
  _nodes->clear();
  if(!_k)
    _k = Kademlia::k();
  assert(_nodes);
}

// }}}
// {{{ k_bucket::~k_bucket
// depth-first delete
k_bucket::~k_bucket()
{
  if(_child[0]) {
    Delete(_child[0]);
  } if(_child[1]) {
    Delete(_child[1]);
  }

  if(_nodes) {
    for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
      Delete(*it);
    }
    Delete(_nodes);
  }
}

// }}}
// {{{ k_bucket::insert
peer_t*
k_bucket::insert(Kademlia::NodeID node, IPAddress ip, string prefix, unsigned depth, k_bucket *root)
{
  if(!root)
    root = this; // i.e. Kademlia::_root

  if(depth == 0)
    KDEBUG(4) << "insert: node = " << Kademlia::printbits(node) << ", ip = " << ip << ", prefix = " << prefix << endl;

  unsigned leftmostbit = Kademlia::getbit(node, depth);
  unsigned myleftmostbit = Kademlia::getbit(_self->id(), depth);
  KDEBUG(4) << "insert: leftmostbit = " << leftmostbit << ", depth = " << depth << endl;

  //
  // NON-ENDLEAF NODE WITH CHILD
  //
  if(_child[leftmostbit]) {
    assert(!_leaf);
    KDEBUG(4) << "insert: _child[" << leftmostbit << "] exists, descending" << endl;
    return _child[leftmostbit]->insert(node, ip, prefix + (leftmostbit ? "1" : "0"), depth+1, root);
  }


  //
  // TRY TO INSERT AT THE NODE WE END UP AT THROUGH RECURSION
  //

  // if this thing is already in the array, move to the right
  // place and bail out
  for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
    if((*it)->id != node)
      continue;

    _nodes->erase(*it);
    (*it)->lastts = now();
    _nodes->insert(*it);
    KDEBUG(4) <<  "insert: node " << Kademlia::printbits(node) << " already in tree" << endl;
    return *it;
  }

  // if not full, just add the new id.
  if(_nodes->size() < _k) {
    KDEBUG(4) <<  "insert: added on level " << depth << endl;
    peer_t *p = New(peer_t, node, ip, now());
    assert(p);
    _nodes->insert(p);
    return p;
  }

  //
  // _nodes ARRAY IS FULL.  PING THE LEAST-RECENTLY SEEN NODE.
  //
  assert(_nodes->size() == _k);
  assert(_child[0] == 0);
  assert(_child[1] == 0);

  // ping the least-recently seen node.  if that one is
  // OK, then don't do anything.
  set<peer_t*>::const_iterator least_recent = _nodes->begin();
  assert(*least_recent);
  if(_self->do_ping_wrapper(*least_recent))
    return 0;

  // evict the dead node
  _nodes->erase(least_recent);
  _root->erase((*least_recent)->id);

  // insert the new one
  peer_t *p = New(peer_t, node, ip, now());
  assert(p);
  _nodes->insert(p);

  //
  // Now if the range in this k-bucket includes the node own ID, then split it,
  // otherwise just return the peer_t we just inserted.
  //
  if(!(node >= (*_nodes->begin())->id && node <= (*_nodes->end())->id &&
        (*_nodes->begin())->id != (*_nodes->end())->id))
    return p;

  assert(!_leaf);
  // create both children
  KDEBUG(4) <<  "insert: not a leaf.  creating subchildren" << endl;
  _child[0] = New(k_bucket, _self, _root);
  _child[1] = New(k_bucket, _self, _root);
  _child[myleftmostbit ^ 1]->_leaf = true;
  KDEBUG(4) <<  "insert: subchild " << (myleftmostbit ^ 1) << " is a leaf on depth " << depth << endl;

  // now divide contents into separate buckets
  // XXX: we have to ping these guys?
  for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
    assert(*it);
    unsigned bit = Kademlia::getbit((*it)->id, depth);
    KDEBUG(4) <<  "insert: pushed entry " << Kademlia::printbits((*it)->id) << " to side " << bit << endl;
    _child[bit]->_nodes->insert(*it);
  }
  Delete(_nodes);
  _nodes = 0;

  // now insert at the right child
  KDEBUG(4) <<  "insert: after split, calling insert for prefix " << (prefix + (leftmostbit ? "1" : "0")) << " to depth " << (depth+1) << endl;
  return _child[leftmostbit]->insert(node, ip, prefix + (leftmostbit ? "1" : "0"), depth+1, root);
}

// }}}
// {{{ k_bucket::stabilize
void
k_bucket::stabilize(string prefix, unsigned depth)
{
  // go through tree depth-first and refresh buckets in the leaves of the tree.
  if(_child[0]) {
    assert(!_nodes);
    assert(_child[1]);
    _child[0]->stabilize(prefix + "0", depth+1);
    _child[1]->stabilize(prefix + "1", depth+1);
    return;
  }

  assert(_nodes);
  if(_nodes->size()) {
    // make a temporary copy
    set<peer_t*, SortedByLastTime> tmpcopy(*_nodes);
    for(set<peer_t*>::const_iterator it = tmpcopy.begin(); it != tmpcopy.end(); ++it) {
      if(now() - (*it)->lastts < KADEMLIA_REFRESH)
        continue;

      // find the closest node to the ID we're looking for
      vector<peer_t*> *best = New(vector<peer_t*>);
      _root->get((*it)->id, best);
      KDEBUG(1) << "stabilize: lookup for " << Kademlia::printbits((*it)->id) << endl;
      _self->do_lookup_wrapper((*best)[0], (*it)->id);
      Delete(best);
    }
    return;
  }

  // now lookup a random key in this range
  NodeID mask = 0;
  for(unsigned i=0; i<depth; i++)
    mask |= (1<<(Kademlia::idsize-depth-i));

  KDEBUG(1) << "stabilize: mask = " << Kademlia::printbits(mask) << endl;
  NodeID random_key = _self->id() & mask;

  vector<peer_t*> *best = New(vector<peer_t*>);
  _root->get(random_key, best);
  for(vector<peer_t*>::const_iterator i = best->begin(); i != best->end(); ++i) {
    KDEBUG(1) << "stabilize: random lookup for " << Kademlia::printbits(random_key) << endl;
    _self->do_lookup_wrapper((*i), random_key);
  }
  Delete(best);

  // NB: the lookup itself will add it to the tree!
}

// }}}
// {{{ k_bucket::stabilized
bool
k_bucket::stabilized(vector<NodeID> lid, string prefix, unsigned depth)
{
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
  if(_child[0]) {
    assert(_child[1]);
    return _child[0]->stabilized(lid, prefix + "0", depth+1) &&
           _child[1]->stabilized(lid, prefix + "1", depth+1);
  }

  if(!_nodes || _nodes->size())
    return true;


  KDEBUG(2) << "stabilized: " << prefix << " not present, depth = " << depth << ", prefix = " << prefix << endl;
  NodeID lower_mask = 0;

  //
  // Node claims there is no node to satisfy this entry in the finger table.
  // Check whether that is true.
  //

  // On every iteration we add another bit.  lower_mask looks like 111...000,
  // but we use it as 000...111 by ~-ing it.
  for(unsigned i=0; i<depth; i++)
    lower_mask |= (1<<(Kademlia::idsize-i-1));
  KDEBUG(4) << "stabilized: lower_mask on depth " << depth << " = " << Kademlia::printbits(lower_mask) << endl;

  // flip the bit, and turn all bits to the right of the flipped bit into
  // zeroes.
  NodeID lower = _id ^ (1<<(Kademlia::idsize-depth-1));
  KDEBUG(4) << "stabilized: lower before mask = " << Kademlia::printbits(lower) << endl;
  lower &= lower_mask;
  KDEBUG(4) << "stabilized: lower after mask = " << Kademlia::printbits(lower) << endl;

  // upper bound is the id with one bit flipped and all bits to the right of
  // that turned into ones.
  NodeID upper = lower | ~lower_mask;

  KDEBUG(4) << "stabilized: lower = " << Kademlia::printbits(lower) << endl;
  KDEBUG(4) << "stabilized: upper = " << Kademlia::printbits(upper) << endl;

  // yields the node with smallest id greater than lower
  vector<NodeID>::const_iterator it = upper_bound(lid.begin(), lid.end(), lower);

  // check that this is smaller than upper.  if so, then this node would
  // qualify for this entry in the finger table, so the node that says there
  // is no such is WRONG.
  if(it != lid.end() && *it <= upper) {
    KDEBUG(4) << "stabilized: entry " << depth << " is invalid, but " << Kademlia::printbits(*it) << " matches " << endl;
    KDEBUG(4) << "stabilized: lowermask = " << Kademlia::printbits(lower_mask) << endl;
    KDEBUG(4) << "stabilized: ~lowermask = " << Kademlia::printbits(~lower_mask) << endl;
    KDEBUG(4) << "stabilized: lower = " << Kademlia::printbits(lower) << endl;
    KDEBUG(4) << "stabilized: upper = " << Kademlia::printbits(upper) << endl;
    return false;
  }

  threadexitsall(0);
  return true;
}

// }}}
// {{{ k_bucket::get

//
// Return the k closest entries to
//
// void
// k_bucket::get(NodeID key, vector<peer_t*> *v, unsigned depth)
// {
//   // descend into the tree
//   unsigned b = Kademlia::getbit(key, depth);
//   KDEBUG(3) << "do_lookup: bit " << depth << " of key is " << b << endl;
//   if(_child[b]) {
//     KDEBUG(3) << "do_lookup: descending further down from depth " << depth << endl;
//     _child[b]->get(key, v, depth+1);
//     if(v->size() >= _k)
//       return;
//   }
// 
//   KDEBUG(3) << "do_lookup: found deepest level on depth = " << depth << endl;
// 
//   if(!_nodes || !_nodes->size()) {
//     KDEBUG(3) << "do_lookup: returning 0 from depth = " << depth << endl;
//     // XXX
//     return;
//   }
// 
//   // XXX: use alpha
//   NodeID bestdist = (NodeID) -1;
//   for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
//     NodeID dist = Kademlia::distance(key, (*it)->id);
//     if(dist < bestdist) {
//       bestdist = dist;
//       p = *it;
//     }
//   }
//   assert(p);
//   KDEBUG(3) << "do_lookup: returning best = " << Kademlia::printbits(p->id) << " from depth = " << depth << endl;
//   return p;
// }

// }}}
// {{{ k_bucket::dump

void
k_bucket::dump(string prefix, unsigned depth)
{
  if(_child[0]) {
    assert(_child[1]);
    _child[0]->dump(prefix + "0", depth+1);
    _child[1]->dump(prefix + "1", depth+1);
    return;
  }

  unsigned i = 0;
  for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
    if(*it)
      cout << "   *** " << prefix << " [" << i++ << "] : " << Kademlia::printbits((*it)->id) << ", firstts = " << (*it)->firstts << ", lastts = " << (*it)->lastts << endl;
  }
}

// }}}

// {{{ trash
#if 0
// {{{ Kademlia::flipbitandmaskright
//
// flips the i-th bit and zeroes out the ones right of it.
//
// for example:
//
//   n = 00110110, i = 3
//                     |
//                     v
//       flip bit: 00111110
//           mask: 11111000
//         result: 00111000
//
inline
Kademlia::NodeID
Kademlia::flipbitandmaskright(NodeID n, unsigned i)
{
  assert((i >= 0) && (i < (8*sizeof(NodeID))));
  return ((n ^ (1<<i)) & _rightmasks[i]);
}

// }}}
// {{{ Kademlia::maskright
inline
Kademlia::NodeID
Kademlia::maskright(NodeID n, unsigned i)
{
  return n & _rightmasks[i];
}

// }}}
// {{{ Kademlia::get_closest
peer_t*
Kademlia::get_closest(vector<peer_t*> *v, NodeID id)
{
  // insert all results in our k-buckets and find the NodeID closest to our own.
  NodeID closestDist = (NodeID) -1;
  peer_t *closestP = 0;
  for(unsigned i=0; i<v->size(); i++) {
    NodeID xid = (*v)[i]->id;
    if(distance(id, xid) < closestDist) {
      closestDist = distance(id, xid);
      closestP = (*v)[i];
    }
  }

  assert(closestP);
  return closestP;
}

// }}}
// {{{ Kademlia::do_lookup remainders
//   //
//   // XXX: do_lookup not correct yet.
//   //
//   vector<peer_t*> *bestset = 0;
//   peer_t *p = 0;
// 
//   // get the best fitting entry in the tree
//   bestset = new vector<peer_t*>;
//   assert(bestset);
// 
//   _tree->get(largs->key, bestset);
//   assert(bestset->size());
// 
//   p = (*bestset)[0];
//   assert(p);
// 
//   // if we are closer than the closest one, then we are the best match.
//   KDEBUG(3) << "do_lookup: closest node = " << printbits(p->id) << endl;
//   if(!p->ip || distance(_id, largs->key) < distance(p->id, largs->key)) {
//     KDEBUG(3) << "do_lookup: i am the best match" << endl;
//     lresult->results.push_back(new peer_t(_id, ip()));
//     goto done;
//   }
// 
//   // recursive lookup
//   KDEBUG(3) << "do_lookup: recursive lookup to " << printbits(p->id) << " at ip = " << p->ip << endl;
//   largs->id = _id;
//   largs->ip = ip();
//   if(!doRPC(p->ip, &Kademlia::do_lookup, largs, lresult))
//     _tree->erase(p->id);
// 
// done:
//   // set correct return data and insert caller into our tree
//   lresult->rid = _id;
//   _tree->insert(callerID, callerIP);
//   Delete(bestset);
// }}}
#endif
/// }}}
