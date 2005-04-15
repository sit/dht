// {{{ headers
/*
 * Copyright (c) 2003-2005 Thomer M. Gil
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

Kademlia::use_replacement_cache_t Kademlia::use_replacement_cache = FULL;
unsigned Kademlia::debugcounter = 1;
unsigned Kademlia::_nkademlias = 0;
Kademlia::k_nodeinfo_pool *Kademlia::pool = 0;

bool Kademlia::docheckrep = false;

unsigned Kademlia::k = 0;
unsigned Kademlia::k_tell = 0;
unsigned Kademlia::alpha = 0;
unsigned Kademlia::stabilize_timer = 0;
unsigned Kademlia::refresh_rate = 0;
unsigned Kademlia::erase_count = 0;
Time Kademlia::max_lookup_time = 0;
bool Kademlia::learn_stabilize_only = false;
bool Kademlia::force_stabilization = false;
bool Kademlia::death_notification = false;

long long unsigned Kademlia::_rpc_bytes = 0;
long unsigned Kademlia::_good_rpcs = 0;
long unsigned Kademlia::_bad_rpcs = 0;
long unsigned Kademlia::_ok_by_reaper = 0;
long unsigned Kademlia::_timeouts_by_reaper = 0;

long unsigned Kademlia::_good_lookups = 0;
long unsigned Kademlia::_good_attempts = 0;
long unsigned Kademlia::_bad_attempts = 0;
long unsigned Kademlia::_lookup_dead_node = 0;
long unsigned Kademlia::_ok_failures = 0;
long unsigned Kademlia::_bad_failures = 0;

Time Kademlia::_good_total_latency = 0;
Time Kademlia::_good_lookup_latency = 0;
Time Kademlia::_good_ping_latency = 0;
long unsigned Kademlia::_good_timeouts = 0;

long unsigned Kademlia::_good_hops = 0;
Time Kademlia::_good_hop_latency = 0;

Time Kademlia::_bad_lookup_latency = 0;
long unsigned Kademlia::_bad_timeouts = 0;
long unsigned Kademlia::_bad_hops = 0;
Time Kademlia::_bad_hop_latency = 0;
Time Kademlia::_default_timeout = 0;

unsigned Kademlia::_to_multiplier = 0;
unsigned Kademlia::_to_cheat = 0;


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

  use_replacement_cache = a.nget<use_replacement_cache_t>("rcache", ENABLED, 10);
  max_lookup_time = a.nget<Time>("maxlookuptime", 4000, 10);
  learn_stabilize_only = a.nget<unsigned>("stablearn_only", 0, 10) == 1 ? true : false;
  force_stabilization = a.nget<unsigned>("force_stab", 0, 10) == 1 ? true : false;
  death_notification = 
    a.nget<unsigned>("death_notify", 1, 10) == 1 ? true : false;

  if(!k) {
    k = a.nget<unsigned>("k", 20, 10);
  }

  if(!k_tell) {
    k_tell = a.nget<unsigned>("k_tell", k, 10);
  }

  if(!alpha) {
    alpha = a.nget<unsigned>("alpha", 3, 10);
  }

  if(!stabilize_timer) {
    refresh_rate = stabilize_timer = a.nget<unsigned>("stabilize_timer", 10000, 10);
  }

  if(!erase_count) {
    erase_count = a.nget<unsigned>("erase_count", 5, 10);
  }

  if(!_default_timeout) {
    _default_timeout = a.nget<Time>("default_timeout", 1000, 10);
  }

  if (!_to_multiplier) {
    _to_multiplier = a.nget<unsigned>("timeout_multiplier",3,10);
  }
  _to_cheat = a.nget<unsigned>("timeout_cheat",1,10);

  if(!Kademlia::pool)
    Kademlia::pool = New k_nodeinfo_pool();
  _me = k_nodeinfo(_id, ip());
  _joined = false;

  init_k_bucket_tree();
  _nkademlias++;
}

Kademlia::NodeID Kademlia::closer::n = 0;
Kademlia::NodeID Kademlia::closerRTT::n = 0;
// }}}
// {{{ Kademlia::~Kademlia
Kademlia::~Kademlia()
{
  // KDEBUG(1) << "Kademlia::~Kademlia" << endl;
  for(HashMap<NodeID, k_nodeinfo*>::iterator i = flyweight.begin(); i != flyweight.end(); ++i)
    Kademlia::pool->push(i.value());
  flyweight.clear();

  delete _root;

  if(ip() == 1) {
    printf("\
#  1: k\n\
#  2: k_tell\n\
#  3: alpha\n\
#  4: stabilize_timer\n\
#  5: refresh_timer\n\
#  6: learn\n\
#  7: rcache\n\
#\n\
#  8: total number of bytes for RPCs\n\
#  9: number of RPCs sent in lookup (good)\n\
# 10: number of RPCs sent in lookup (bad)\n\
# 11: number of good      RPCs reaped by reaper\n\
# 12: number of timed out RPCs reaped by reaper\n\
#\n\
# 13: number of good lookups\n\
# 14: number of attempts for good lookups\n\
# 15: number of attempts for bad lookups\n\
# 16: number of good lookups, but node was dead\n\
# 17: number of bad lookups, node was dead\n\
# 18: number of bad lookups, node was alive\n\
#\n\
# 19: avg total lookup latency (good)\n\
# 20: avg pure lookup latency (good)\n\
# 21: avg pure ping latency (good)\n\
# 22: number of timeouts suffered during lookup (good)\n\
#\n\
# 23: avg number of hops (good)\n\
# 24: avg latency per hop (good)\n\
#\n\
# 25: avg pure lookup latency (bad)\n\
# 26: number of timeouts suffered during lookup (bad)\n\
# 27: avg number of hops (bad)\n\
# 28: avg latency per hop (bad)\n\
#\n\
%u %u %u %u %u %u %u      %llu %lu %lu %lu %lu     %lu %lu %lu %lu %lu %lu    %.2f %.2f %.2f %lu    %.2f %.2f   %.2f %lu %.2f %.2f\n",
        Kademlia::k,
        Kademlia::k_tell,
        Kademlia::alpha,
        Kademlia::stabilize_timer,
        Kademlia::refresh_rate,
        Kademlia::learn_stabilize_only ? 1 : 0,
        Kademlia::use_replacement_cache,

        _rpc_bytes,
        _good_rpcs,
        _bad_rpcs,
        _ok_by_reaper,
        _timeouts_by_reaper,

        _good_lookups,
        _good_attempts,
        _bad_attempts,
        _lookup_dead_node,
        _ok_failures,
        _bad_failures,

        (double) _good_total_latency / _good_lookups,
        (double) _good_lookup_latency / _good_lookups,
        (double) _good_ping_latency / _good_lookups,
        _good_timeouts,

        (double) _good_hops / _good_lookups,
        (double) _good_hop_latency / _good_hops,

        (double) _bad_lookup_latency / _bad_attempts,
        _bad_timeouts,
        (double) _bad_hops / _bad_attempts,
        (double) _bad_hop_latency / _bad_attempts);


    print_stats();
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

void
Kademlia::init_k_bucket_tree()
{
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
}

// jy
Time
Kademlia::timeout(IPAddress dst)
{
  if (_to_cheat)
    return _to_multiplier * 2 * Network::Instance()->gettopology()->latency(_me.ip,dst);
  else
    return Kademlia::_default_timeout;
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
        insert(k->id(), k->ip(), 0, 0, true);
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

      insert(k->id(), k->ip(), 0, 0, true);
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
  if(_joined)
    return;

  IPAddress wkn = args->nget<IPAddress>("wellknown");

  // pick a new ID
  //KDEBUG(1) << "Kademlia::join ip " << ip() << " id " << printID(_id) << " now " << now() << endl;
  assert(!_root);
  _nodeid2kademlia->remove(_id);
  _id = ConsistentHash::ip2chid(ip());
  _me = k_nodeinfo(_id, ip());
  assert(!_nodeid2kademlia->find_pair(_id));
  _nodeid2kademlia->insert(_id, this);
  assert(!_root);
  init_k_bucket_tree();

  // I am the well-known node
  if(wkn == ip()) {
    if(stabilize_timer)
      delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
    _joined = true;
    return;
  }

join_restart:
  // lookup my own key with well known node.
  lookup_args la(_id, ip(), _id);
  la.stattype = Kademlia::STAT_JOIN;
  lookup_result lr;
  bool b = false;
  Time before = 0;
  do {
    record_stat(STAT_JOIN, 1, 0);
    before = now();
    b = doRPC(wkn, &Kademlia::do_lookup, &la, &lr, timeout(wkn));
    record_stat(STAT_JOIN, lr.results.size(), 0);
  } while(!b);

  if(!alive())
    return;

  // put well known node in k-buckets
  // KDEBUG(2) << "join: lr.rid = " << printID(lr.rid) << endl;
  update_k_bucket(lr.rid, wkn, now() - before);

  // put all nodes that wkn told us in k-buckets
  // NB: it's questionable whether we can do a touch here.  we haven't
  // actually talked to the node.
  NODES_ITER(&lr.results) {
    if(!flyweight[i->id] && i->id != _id) {
      insert(i->id, i->ip, 0, i->timeouts);
      touch(i->id);
    }
  }

  // get our ``successor'' and compute length of prefix we have in common
  vector<k_nodeinfo*> successors;
  _root->find_node(_id, &successors);
  closer::n = _id;
  sort(successors.begin(), successors.end(), closer());
  NodeID succ_id = successors[0]->id;

  //KDEBUG(2) << "join: succ ip is " << successors[0]->ip << " id is " << printID(succ_id) << endl;
  unsigned cpl = common_prefix(_id, succ_id);

  // all entries further away than him need to be refreshed.  this is similar to
  // stabilization.  flip a bit and set the rest of the ID to something random.
  for(unsigned i=idsize-cpl+1; i<idsize; i++) {
    NodeID random_key = _id ^ (((Kademlia::NodeID) 1) << i);
    for(int j=i-1; j>=0; j--)
      random_key ^= (((NodeID) random() & 0x1) << j);

    lookup_args la(_id, ip(), random_key);
    la.stattype = Kademlia::STAT_JOIN;
    lookup_result lr;

    // if we now believe our successor died, then start again
    if(!flyweight[succ_id]) {
      clear();
      goto join_restart;
    }

    // do a refresh on that bucket
    do_lookup(&la, &lr);

    if(!alive())
      return;

    // fill our finger table
    NODES_ITER(&lr.results)
      if(!flyweight[i->id] && i->id != _id)
        insert(i->id, i->ip, 0, i->timeouts);
        //  ... but not touch.  we didn't actually talk to the node.
  }

  _joined = true;

  if(stabilize_timer)
    delaycb(stabilize_timer, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::crash
void
Kademlia::crash(Args *args)
{
  // destroy k-buckets
  //KDEBUG(1) << "Kademlia::crash ip " << ip() << endl;
  // assert(alive());
  //_root->collapse();
  //jy
  delete _root;
  _root = NULL;
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
  if(!_joined || !alive())
    return;

  IPAddress key_ip = args->nget<NodeID>("key");
  if(!Network::Instance()->getnode(key_ip)->alive())
    return;

  // find node with this IP
  Kademlia *k = (Kademlia*) Network::Instance()->getnode(key_ip);
  NodeID key = k->id();
  //KDEBUG(0) << "Kademlia::lookup: ip " << key_ip << " id " << printID(key) << endl;

  lookup_wrapper_args *lwa = New lookup_wrapper_args();
  lwa->ipkey = key_ip;
  lwa->key = key;
  lwa->starttime = now();
  lwa->attempts = 0;

  lookup_wrapper(lwa);
}

// }}}
// {{{ Kademlia::lookup_wrapper
void
Kademlia::lookup_wrapper(lookup_wrapper_args *args)
{
  static unsigned outcounter = 0;

  if(!alive()) {
    delete args;
    return;
  }

  find_value_args fa(_id, ip(), args->key);
  find_value_result fr;

  find_value(&fa, &fr);
  Time after = now();
  args->attempts++;

  // if we found the node, ping it.
  if(args->key == fr.succ.id) {
    ping_args pa(fr.succ.id, fr.succ.ip);
    ping_result pr;
    Time pingbegin = now();
    // assert(_nodeid2kademlia[fr.succ.id]->ip() == fr.succ.ip);
    if(!doRPC(fr.succ.ip, &Kademlia::do_ping, &pa, &pr, timeout(fr.succ.ip)) && alive()) {
      if(collect_stat()) _lookup_dead_node++;
      if(flyweight[fr.succ.id] && !Kademlia::learn_stabilize_only)
        erase(fr.succ.id);
      fr.timeouts++;
    }
    after = now();

    record_lookup_stat(ip(), fr.succ.ip, after - args->starttime, true, true,
		       fr.hops, fr.timeouts, 0);
    if(outcounter++ >= 1000) {
      KDEBUG(0) <<  pingbegin - args->starttime << "ms lookup (" << args->attempts << "a, " << fr.hops << "h, " << fr.rpcs << "r, " << fr.timeouts << "t), " << after - pingbegin << "ms ping, " << after - args->starttime << "ms total." << endl;
      outcounter = 0;
    }
    // cout << pingbegin - args->starttime << endl;

    if(collect_stat()) {
      _good_lookups++;
      _good_attempts += args->attempts;
      _good_total_latency += (after - args->starttime);
      _good_lookup_latency += (pingbegin - args->starttime);
      _good_ping_latency += (after - pingbegin);

      _good_hops += fr.hops;
      _good_timeouts += fr.timeouts;
      _good_rpcs += fr.rpcs;
      _good_hop_latency += fr.latency;
    }
    delete args;
    return;
  }


  //node crashed and some other nodes rejoined into its old master node,
  //deleting the crashed node's entry, this cannot be relied upon to 
  //determine node liveliness
  //bool alive_and_joined = (*_nodeid2kademlia)[args->key]->alive() &&
  //                        (*_nodeid2kademlia)[args->key]->_joined;
  bool alive_and_joined = Network::Instance()->alive(args->ipkey)&&
                          ((Kademlia*)Network::Instance()->getnode(args->ipkey))->_joined;
  IPAddress target_ip = args->ipkey;

  // we're out of time.
  if(now() - args->starttime > Kademlia::max_lookup_time) {
    if(alive_and_joined) {
      if(collect_stat()) _bad_failures++;
      record_lookup_stat(ip(), target_ip, after - args->starttime, false, false, fr.hops, fr.timeouts, 0);
    } else {
      if(alive_and_joined) _ok_failures++;
      record_lookup_stat(ip(), target_ip, after - args->starttime, true, false, fr.hops, fr.timeouts, 0);
    }
    if(collect_stat()) {
      _bad_attempts += args->attempts;
      _bad_lookup_latency += (after - args->starttime);
      _bad_hops += fr.hops;
      _bad_timeouts += fr.timeouts;
      _bad_rpcs += fr.rpcs;
      _bad_hop_latency += fr.latency;
    }

    delete args;
    return;
  }

  // try again in a bit.
  delaycb(100, &Kademlia::lookup_wrapper, args);
}

// }}}
// {{{ Kademlia::do_ping
void
Kademlia::do_ping(ping_args *args, ping_result *result)
{
  // put the caller in the tree, but never ourselves
  // KDEBUG(1) << "Kademlia::do_ping from " << printID(args->id) << endl;
  if(!Kademlia::learn_stabilize_only)
    update_k_bucket(args->id, args->ip);
}

// }}}
// {{{ Kademlia::util for find_value and do_lookup
#define SEND_RPC(x, ARGS, RESULT, HOPS, WHICH_ALPHA) {                                  \
    find_node_args *fa = New find_node_args(_id, ip(), ARGS->key);                      \
    fa->stattype = ARGS->stattype;                                                      \
    find_node_result *fr = New find_node_result;                                        \
    fr->hops = HOPS;                                                                    \
    fr->which_alpha = WHICH_ALPHA;                                                      \
    record_stat(ARGS->stattype, 1, 0);                                                  \
    unsigned rpc = asyncRPC(x.ip, &Kademlia::find_node, fa, fr, timeout(x.ip)); \
    callinfo *ci = New callinfo(x, fa, fr);                                             \
    ci->before = now();                                                                 \
    rpcset->insert(rpc);                                                                \
    outstanding_rpcs->insert(rpc, ci);                                                  \
}

#define CREATE_REAPER(STAT) {                                                           \
      if(!outstanding_rpcs->size() && !is_dead->size() ) {                              \
        delete rpcset;                                                                  \
        delete outstanding_rpcs;                                                        \
        if(Kademlia::death_notification) {                                              \
          for( HashMap<NodeID, vector<IPAddress> *>::iterator i=who_told_me->begin(); i != who_told_me->end(); ++i ) {                                                 \
            vector<IPAddress> *v = i.value();                                           \
            if( v != NULL ) delete v;                                                   \
          }                                                                             \
        }                                                                               \
        delete who_told_me;                                                             \
        delete is_dead;                                                                 \
        return;                                                                         \
      }                                                                                 \
      reap_info *ri = New reap_info();                                                  \
      ri->k = this;                                                                     \
      ri->rpcset = rpcset;                                                              \
      ri->outstanding_rpcs = outstanding_rpcs;                                          \
      ri->who_told_me = who_told_me;                                                    \
      ri->is_dead = is_dead;                                                            \
      ri->stat = STAT;                                                                  \
      ThreadManager::Instance()->create(Kademlia::reap, (void*) ri);                    \
      return;                                                                           \
}
// }}}
// {{{ Kademlia::find_value
void
Kademlia::find_value(find_value_args *fargs, find_value_result *fresult)
{
  //KDEBUG(0) << "Kademlia::find_value: node ip " << fargs->ip << " id " << printID(fargs->id) << " does find_value for " << printID(fargs->key) << endl;
    // assert(alive());
  HashMap<NodeID, bool> asked;
  HashMap<NodeID, unsigned> hops;
  HashMap<unsigned, unsigned> timeouts;
  update_k_bucket(fargs->id, fargs->ip);
  fresult->rid = _id;
  fresult->hops = 0;

  // find alpha successors for this key
  closer::n = fargs->key;
  closerRTT::n = fargs->key;
  set<k_nodeinfo, closerRTT> successors;
  vector<k_nodeinfo*> tmp;
  _root->find_node(fargs->key, &tmp);
  for(unsigned i=0; i<tmp.size(); i++) {
    successors.insert((*tmp[i]));
    hops.insert(tmp[i]->id, 0);
  }

  // we can't do anything but return ourselves
  if(!successors.size()) {
    fresult->succ = _me;
    return;
  }

  // return if we're done already
  fresult->succ = *successors.begin();
  if(fresult->succ.id == fargs->key)
    return;

  // send out the first alpha RPCs
  HashMap<unsigned, callinfo*> *outstanding_rpcs = New HashMap<unsigned, callinfo*>;
  RPCSet *rpcset = New RPCSet;
  {
    unsigned a = 0;
    for(set<k_nodeinfo, closer>::const_iterator i=successors.begin(); i != successors.end() && a < Kademlia::alpha; ++i, ++a) {
      k_nodeinfo ki = *i;
      SEND_RPC(ki, fargs, fresult, hops[ki.id], a);
      fresult->rpcs++;
      asked.insert(ki.id, true);
      successors.erase(ki);
      timeouts.insert(a, 0);
    }
  }

  // data structures for death notifications:
  HashMap<NodeID, vector<IPAddress> * > *who_told_me = 
    New HashMap<NodeID, vector<IPAddress> * >;
  HashMap<NodeID, bool> *is_dead = New HashMap<NodeID, bool>;

  // now send out a new RPC for every single RPC that comes back
  unsigned useless_replies = 0;
  NodeID last_before_merge = ~fargs->key;
  unsigned last_returned_alpha;
  while(true) {
    bool ok;
    unsigned donerpc = rcvRPC(rpcset, ok);
    callinfo *ci = (*outstanding_rpcs)[donerpc];
    outstanding_rpcs->remove(donerpc);
    bool improved = false;
    last_returned_alpha = ci->fr->which_alpha;

    k_nodeinfo ki;
    if(!alive()) {
      stat_type st = ci->fa->stattype;
      delete ci;
      CREATE_REAPER(st); // returns
    }

    // node was dead
    closer::n = fargs->key;
    closerRTT::n = fargs->key;
    if(!ok) {
      if(flyweight[ci->ki.id] && !Kademlia::learn_stabilize_only)
        erase(ci->ki.id);
      if(Kademlia::death_notification) {
	// inform whoever sent me this person that they are dead.
	is_dead->insert( ci->ki.id, true );
      }
      timeouts.insert(last_returned_alpha, timeouts[last_returned_alpha]+1);
      delete ci;
      if(!successors.size() && outstanding_rpcs->size()==0) {
        CREATE_REAPER(ci->fa->stattype); // returns
      }
      goto next_candidate;
    }

    // node was ok
    assert(ci->fr->results.size() <= Kademlia::k_tell);
    record_stat(ci->fa->stattype, ci->fr->results.size(), 0);
    if(!Kademlia::learn_stabilize_only)
      update_k_bucket(ci->ki.id, ci->ki.ip, now() - ci->before);

    // put all the ones better than what we knew so far in successors, provided
    // we haven't asked them already.
    if(successors.size())
      last_before_merge = successors.rbegin()->id;

    for(unsigned i=0; i<ci->fr->results.size(); i++) {
      k_nodeinfo ki = ci->fr->results[i];
      if( Kademlia::death_notification ) {
	if( (*who_told_me)[ki.id] == NULL ) {
	  who_told_me->insert( ki.id, New vector<IPAddress> );
	}
	((*who_told_me)[ki.id])->push_back( ci->ki.ip );
      }
      if(asked.find_pair(ki.id))
        continue;
      successors.insert(ki);
      if(!hops.find_pair(ki.id))
        hops.insert(ki.id, ci->fr->hops+1);
    }

    // cut out elements beyond index k
    while(successors.size() > Kademlia::k)
      successors.erase(*successors.rbegin());

    // found the key
    ki = *successors.begin();
    if(ki.id == fargs->key) {
      fresult->hops = hops[ki.id];
      fresult->succ = ki;
      fresult->timeouts = timeouts[last_returned_alpha];
      stat_type st = ci->fa->stattype;
      delete ci;
      CREATE_REAPER(st); // returns
    }

    // if our standing hasn't improved,
    improved = (distance(successors.begin()->id, fargs->key) < distance(fresult->succ.id, fargs->key));
    useless_replies = improved ? 0 : useless_replies++;

    // if the last alpha RPCs didn't yield anything better, give up.  I don't
    // think this should ever happen.  It means we went down a dead end alpha
    // times.
    //
    // XXX: Thomer completely pulled this out of his ass.
    if(!successors.size() || useless_replies > Kademlia::alpha) {
      stat_type st = ci->fa->stattype;
      delete ci;
      CREATE_REAPER(st); // returns
    }
    delete ci;

next_candidate:
    // assert((unsigned) outstanding_rpcs->size() == Kademlia::alpha - 1);
    // XXX: wrong.  messes up last_alpha_rpc
    while((outstanding_rpcs->size() < (int) Kademlia::alpha) && successors.size()) {
      k_nodeinfo front = *successors.begin();
      if(Kademlia::distance(front.id, fargs->key) < Kademlia::distance(fresult->succ.id, fargs->key))
        fresult->succ = front;

      SEND_RPC(front, fargs, fresult, hops[front.id], timeouts[last_returned_alpha]);

      fresult->rpcs++;
      asked.insert(front.id, true);
      successors.erase(front);
    }
  }

}

// }}}
// {{{ Kademlia::do_lookup
//
// only called for stabilization, so always learn
void
Kademlia::do_lookup(lookup_args *largs, lookup_result *lresult)
{
  //KDEBUG(1) << "Kademlia::do_lookup: node " << printID(largs->id) << " does lookup for " << printID(largs->key) << ", flyweight.size() = " << endl;
  // assert(alive());
  lresult->rid = _id;

  // find successors of this key
  closer::n = largs->key;
  set<k_nodeinfo, closer> successors;
  vector<k_nodeinfo*> tmp;
  _root->find_node(largs->key, &tmp);
  for(unsigned i=0; i<tmp.size(); i++) {
    successors.insert(tmp[i]);
    lresult->results.insert(tmp[i]);
  }

  // unsigned j = 0;
  // NODES_ITER(&lresult->results) {
  //   KDEBUG(0) << "start: lresults[" << j++ << "] = " << printID(i->id) << endl;
  // }

  // we can't do anything but return ourselves
  if(!successors.size()) {
    // KDEBUG(0) << "no successors, exiting" << endl;
    lresult->results.insert(_me);
    return;
  }

  // keep track of worst-of-best
  NodeID worst = lresult->results.rbegin()->id;
  // KDEBUG(0) << "worst = " << printID(worst) << endl;

  // send out the first alpha RPCs
  HashMap<unsigned, callinfo*> *outstanding_rpcs = New HashMap<unsigned, callinfo*>;
  RPCSet *rpcset = New RPCSet;
  {
    unsigned a = 0;
    for(set<k_nodeinfo, closer>::const_iterator i=successors.begin(); i != successors.end() && a < Kademlia::alpha; ++i, ++a) {
      k_nodeinfo ki = *i;
      SEND_RPC(ki, largs, lresult, 0, 0);
      // KDEBUG(0) << "SEND_RPC (initial) to " << printID(ki.id) << endl;
      successors.erase(ki);
    }
  }

  // j = 0;
  // NODES_ITER(&successors) {
  //   KDEBUG(0) << "after initial SEND_RPC successors[" << j++ << "] = " << printID(i->id) << endl;
  // }

  // data structures for death notifications:
  HashMap<NodeID, vector<IPAddress> * > *who_told_me = 
    New HashMap<NodeID, vector<IPAddress> * >;
  HashMap<NodeID, bool> *is_dead = New HashMap<NodeID, bool>;

  // send an RPC back for each incoming reply.
  HashMap<NodeID, bool> replied;
  while(true) {
    bool ok;
    unsigned donerpc = rcvRPC(rpcset, ok);
    callinfo *ci = (*outstanding_rpcs)[donerpc];
    outstanding_rpcs->remove(donerpc);
    replied.insert(ci->ki.id, true);

    if(!alive()) {
      delete ci;
      // KDEBUG(0) << "rcvRPC, not alive, bye" << endl;
      CREATE_REAPER(largs->stattype); // returns
    }

    // node was dead
    closer::n = largs->key;
    if(!ok) {
      // KDEBUG(0) << "!ok" << endl;
      if(flyweight[ci->ki.id] && (!Kademlia::learn_stabilize_only ||
                                   largs->stattype == STAT_STABILIZE ||
                                   largs->stattype == STAT_LOOKUP))
        erase(ci->ki.id);
      if(Kademlia::death_notification) {
	// inform whoever sent me this person that they are dead.
	is_dead->insert( ci->ki.id, true );
      }
      delete ci;

      // no more RPCs to send, but more RPCs to wait for
      if(!successors.size() && outstanding_rpcs->size()) {
        // KDEBUG(0) << "!ok, no more successors, but more outstanding rpcs" << endl;
        continue;
      }

      // no more RPCs to send and no outstanding RPCs.
      if(!successors.size() && !outstanding_rpcs->size()) {
        // KDEBUG(0) << "!ok, no more successors and no outstanding_rpcs" << endl;
        // truncate to right size
        while(lresult->results.size() > Kademlia::k)
          lresult->results.erase(*lresult->results.rbegin());
        // unsigned j = 0;
        // NODES_ITER(&lresult->results) {
        //   KDEBUG(0) << "CREATE_REAPER lresults[" << j++ << "] = " << printID(i->id) << endl;
        // }
        CREATE_REAPER(largs->stattype); // returns
      }

      if(successors.size())
        goto next_candidate;

      // we've handled all the cases, right?
      assert(false);
    }

    // node was ok
    // KDEBUG(0) << "good reply" << endl;
    record_stat(largs->stattype, ci->fr->results.size(), 0);
    if(!Kademlia::learn_stabilize_only || largs->stattype == STAT_STABILIZE ||
                                          largs->stattype == STAT_LOOKUP)
      update_k_bucket(ci->ki.id, ci->ki.ip, now() - ci->before);

    // j = 0;
    // NODES_ITER(&lresult->results) {
    //   KDEBUG(0) << "before merge lresults[" << j++ << "] = " << printID(i->id) << endl;
    // }
    // j = 0;
    // NODES_ITER(&successors) {
    //   KDEBUG(0) << "before merge successors[" << j++ << "] = " << printID(i->id) << endl;
    // }

    // put in successors list if:
    //   - it's better than the worst-of-best we have so far AND
    //   - it's not in our results set already
    closer::n = largs->key;
    for(unsigned i=0; i<ci->fr->results.size(); i++) {
      k_nodeinfo ki = ci->fr->results[i];
      if( Kademlia::death_notification ) {
	if( (*who_told_me)[ki.id] == NULL ) {
	  who_told_me->insert( ki.id, New vector<IPAddress> );
	}
	((*who_told_me)[ki.id])->push_back( ci->ki.ip );
      }
      if(distance(ki.id, largs->key) >= distance(worst, largs->key) ||
         lresult->results.find(ki) != lresult->results.end())
        continue;
      successors.insert(ki);
      lresult->results.insert(ki);
    }
    delete ci;

    // j = 0;
    // NODES_ITER(&lresult->results) {
    //   KDEBUG(0) << "after merge lresults[" << j++ << "] = " << printID(i->id) << endl;
    // }
    // j = 0;
    // NODES_ITER(&successors) {
    //   KDEBUG(0) << "after merge successors[" << j++ << "] = " << printID(i->id) << endl;
    // }

    // we're done if we've had a reply from everyone in the results set.
    if(!successors.size()) {
      bool we_are_done = true;
      NODES_ITER(&lresult->results) {
        if(!replied[i->id]) {
          we_are_done = false;
          break;
        }
      }
      if(we_are_done) {
        // KDEBUG(0) << "returning size = " << lresult->results.size() << endl;
        while(lresult->results.size() > Kademlia::k)
          lresult->results.erase(*lresult->results.rbegin());
        CREATE_REAPER(largs->stattype); // returns
      }
    }

    // last alpha RPCs did not change our view of the world.  send parallel RPCs
    // to all remaining ones.
    if(successors.size() <= (Kademlia::k - Kademlia::alpha)) {
      // KDEBUG(0) << "nothing changed. flushing" << endl;
      NODES_ITER(&successors) {
        k_nodeinfo ki = *i;
        SEND_RPC(ki, largs, lresult, 0, 0);
        successors.erase(ki);
      }
      continue;
    }

    // resize back to Kademlia::k
    while(successors.size() > Kademlia::k)
      successors.erase(*successors.rbegin());
    while(lresult->results.size() > Kademlia::k)
      lresult->results.erase(*lresult->results.rbegin());


    // j = 0;
    // NODES_ITER(&successors) {
    //   KDEBUG(0) << "after truncate successors[" << j++ << "] = " << printID(i->id) << endl;
    // }
    // j = 0;
    // NODES_ITER(&lresult->results) {
    //   KDEBUG(0) << "after truncate lresults[" << j++ << "] = " << printID(i->id) << endl;
    // }

    // need to update our worst-of-best?
    if(successors.size() &&
      (Kademlia::distance(lresult->results.rbegin()->id, largs->key) <
       Kademlia::distance(worst, largs->key)))
    {
      worst = lresult->results.rbegin()->id;
    }

next_candidate:
    k_nodeinfo front = *successors.begin();
    SEND_RPC(front, largs, lresult, 0, 0);
    successors.erase(front);
    // j = 0;
    // NODES_ITER(&successors) {
    //   KDEBUG(0) << "after next_candidate successors[" << j++ << "] = " << printID(i->id) << endl;
    // }
  }
}
// }}}
// {{{ Kademlia::find_node
// Kademlia's FIND_NODE.  Returns the best k from its own k-buckets
void
Kademlia::find_node(find_node_args *largs, find_node_result *lresult)
{
  // assert(alive());
  if(!Kademlia::learn_stabilize_only || largs->stattype == STAT_STABILIZE ||
                                        largs->stattype == STAT_LOOKUP)
    update_k_bucket(largs->id, largs->ip);

  lresult->rid = _id;

  // deal with the empty case
  if(!flyweight.size()) {
    // assert(p);
    KDEBUG(2) << "find_node: key " << printID(largs->key) << " tree is empty. returning myself" << endl;
    lresult->results.push_back(_me);
    return;
  }

  vector<k_nodeinfo*> tmpset;
  _root->find_node(largs->key, &tmpset);
  //KDEBUG(2) << "find_node: key "<< printID(largs->key) << " from ip " << largs->ip << " returning: ip " <<tmpset[0]->ip << " id " << printID(tmpset[0]->id) << endl;
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

  if(stabilize_timer)
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
Kademlia::insert(NodeID id, IPAddress ip, Time RTT, char timeouts, bool init_state)
{
  // KDEBUG(1) << "Kademlia::insert " << Kademlia::printID(id) << ", ip = " << ip << endl;
  static unsigned counter = 0;

  // assert(alive());
  // assert(id && ip);
  // assert(!flyweight.find(id, 0));

  k_nodeinfo *ni = Kademlia::pool->pop(id, ip, RTT, timeouts);
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
  // KDEBUG(2) << "Kademlia::erase deleting id = " << printID(id) << ", ip = " << flyweight[id]->ip << endl;
  k_nodeinfo *ki = flyweight[id];

  // 5, taken from Kademlia paper
  if(++ki->timeouts >= Kademlia::erase_count) {
    _root->erase(id);
    flyweight.remove(id);
    Kademlia::pool->push(ki);
    return;
  }
}
// }}}
// {{{ Kademlia::erase
void
Kademlia::do_erase(erase_args *a, erase_result *r)
{
  for( unsigned i = 0; i < a->ids->size(); i++ ) {
    NodeID n = (*(a->ids))[i];
    if( flyweight.find(n, 0) ) {
      erase(n);
    }
  }
}
// }}}
// {{{ Kademlia::update_k_bucket
inline void
Kademlia::update_k_bucket(NodeID id, IPAddress ip, Time RTT)
{
  // KDEBUG(1) << "Kademlia::update_k_bucket" << endl;

  // update k-bucket
  if(id == _id)
    return;

  if(!flyweight[id]) {
    // KDEBUG(2) << "Kademlia::update_k_bucket says " << printID(id) << " doesn't exist yet" << endl;
    insert(id, ip, RTT);
  } else if(RTT) {
    flyweight[id]->RTT = RTT;
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
  record_bw_stat(type, num_ids, num_else);
  if(collect_stat())
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
      _ok_by_reaper++;
      if(ri->k->alive() && (!Kademlia::learn_stabilize_only || ci->fa->stattype == STAT_STABILIZE ||
                                                               ci->fa->stattype == STAT_LOOKUP))
        ri->k->update_k_bucket(ci->ki.id, ci->ki.ip);
      ri->k->record_stat(ri->stat, ci->fr->results.size(), 0);

    } else if(ri->k->flyweight[ci->ki.id]) {
      _timeouts_by_reaper++;
      if(ri->k->alive() && (!Kademlia::learn_stabilize_only || ci->fa->stattype == STAT_STABILIZE ||
                                                               ci->fa->stattype == STAT_LOOKUP))
        ri->k->erase(ci->ki.id);
    }

    // Although it slightly affects success rate, it's only fair
    // to only count nodes as dead if you're alive when you receive
    // their response.
    if( !ok && Kademlia::death_notification && ri->k->alive() ) {
      ri->is_dead->insert( ci->ki.id, true );
    }

    ri->outstanding_rpcs->remove(donerpc);
    delete ci;
  }

  // make vectors of dead-nodes sorted by the neighbor that told you.
  if( Kademlia::death_notification ) {

    HashMap<IPAddress, vector<NodeID> *> bad_info;
    HashMap<unsigned, erase_args*> deathmap;
    RPCSet deathrpcset;

    for( HashMap<NodeID, bool>::iterator i=ri->is_dead->begin(); 
	 i != ri->is_dead->end(); ++i ) {
      NodeID dead_node = i.key();
      vector<IPAddress> *v = (*(ri->who_told_me))[dead_node];
      if( v != NULL ) {
	for( unsigned j = 0; j < v->size(); j++ ) {
	  if( bad_info[(*v)[j]] == NULL ) {
	    bad_info.insert( (*v)[j], New vector<NodeID> );
	  }
	  (bad_info[(*v)[j]])->push_back( dead_node );	
	}
      }
    }
    
    // cleanup who_told_me state
    for( HashMap<NodeID, vector<IPAddress> *>::iterator i=
	   ri->who_told_me->begin(); 
	 i != ri->who_told_me->end(); ++i ) {
      
      vector<IPAddress> *v = i.value();
      if( v != NULL ) delete v;
      
    }
    
    // notify neighbors about all the deaths
    for( HashMap<IPAddress,vector<NodeID> *>::iterator i=bad_info.begin(); 
	 i != bad_info.end(); ++i ) {
      
      IPAddress informant = i.key();
      vector<NodeID> *v = i.value();

      if( informant == ri->k->ip() ) {
	delete v;
	continue;
      }
      
      erase_args *ea = New erase_args(v);
      ri->k->record_stat(STAT_ERASE, v->size(), 0);
      
      unsigned rpc = ri->k->asyncRPC(informant, &Kademlia::do_erase, ea, 
				     (erase_result *) NULL, 
				     ri->k->timeout(informant));
      deathrpcset.insert(rpc);
      deathmap.insert(rpc, ea);

    }

    // clean up any death notification state as well
    while(deathmap.size()) {
      bool ok;
      unsigned donerpc = ri->k->rcvRPC(&deathrpcset, ok);
      erase_args *ea = deathmap.find(donerpc);
      // NOTE: there's a conscious decision here to not record_stat 
      // the response.  The reason being that I don't care what the 
      // response is, and in real life I wouldn't wait for it.  I only
      // have to do so here to free the memory.
      delete ea->ids;
      delete ea;
      deathmap.remove(donerpc);
    }
    
  }

  // ri->k->_riset.remove(ri);
  delete ri;

  // cout << "reaper done" << endl;
  taskexit(0);
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
    ninfo->timeouts = 0;
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
    delete _nodes;
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
    if(i_am_replacement_cache || Kademlia::use_replacement_cache == Kademlia::DISABLED) {
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
k_nodeinfo::k_nodeinfo(NodeID id, IPAddress ip, Time RTT) : id(id), ip(ip), RTT(RTT)
{
  lastts = 0;
  checkrep();
}
// }}}
// {{{ k_nodeinfo::k_nodeinfo
k_nodeinfo::k_nodeinfo(k_nodeinfo *k) :
  id(k->id), ip(k->ip), lastts(k->lastts), timeouts(timeouts), RTT(RTT)
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

  if(Kademlia::use_replacement_cache != Kademlia::FULL) {
    checkrep();
    return;
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

  if(Kademlia::use_replacement_cache == Kademlia::DISABLED) {
    checkrep();
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
  if(Kademlia::use_replacement_cache == Kademlia::DISABLED || !replacement_cache->size()) {
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
  if(!Kademlia::force_stabilization) {
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
  }

  // Oh.  stuff in this k-bucket is old, or we don't know anything. Lookup a
  // random key in this range.
  Kademlia::NodeID mask = 0L;
  for(unsigned i=0; i<Kademlia::idsize; i++) {
    unsigned bit = 1;
    if(i > depth)
      bit = (unsigned) random() & 0x1;
    mask |= (((Kademlia::NodeID) bit) << (Kademlia::idsize-i));
  }

  Kademlia::NodeID random_key = _id & mask;
  // KDEBUG(2) << "k_stabilizer: prefix = " << prefix << ", mask = " << Kademlia::printbits(mask) << ", random_key = " << Kademlia::printbits(random_key) << endl;

  // lookup the random key and update this k-bucket with what we learn
  Kademlia::lookup_args la(mykademlia->id(), mykademlia->ip(), random_key);
  la.stattype = Kademlia::STAT_STABILIZE;
  Kademlia::lookup_result lr;

  mykademlia->do_lookup(&la, &lr);
  if(!mykademlia->alive())
    return;

  // update our k-buckets
  NODES_ITER(&lr.results) {
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
