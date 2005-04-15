/*
 * Copyright (c) 2003-2005 Robert Morris
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

#include "kelips.h"
#include "p2psim/network.h"
#include <stdio.h>
#include <math.h>
#include <iostream>
#include <time.h>
#include <map>

using namespace std;

// Record stabilization times.
int sta[1000];
int nsta;

double Kelips::_rpc_bytes = 0;
double Kelips::_good_latency = 0;
double Kelips::_good_hops = 0;
int Kelips::_good_lookups = 0;
int Kelips::_ok_failures = 0;  // # legitimate lookup failures
int Kelips::_bad_failures = 0; // lookup failed, but node was live

Kelips::Kelips(IPAddress i, Args a) : P2Protocol(i)
{
  _started = false;
  _live = false;

  // settable parameters.
  _k = a.nget<unsigned>("k", 20, 10);
  _round_interval = a.nget<int>("round_interval", 2000, 10);
  _group_targets = a.nget<u_int>("group_targets", 3, 10);
  _contact_targets = a.nget<u_int>("contact_targets", 3, 10);
  _group_ration = a.nget<u_int>("group_ration", 4, 10);
  _contact_ration = a.nget<u_int>("contact_ration", 2, 10);
  _n_contacts = a.nget<u_int>("n_contacts", 2, 10);
  _item_rounds = a.nget<u_int>("item_rounds", 1, 10);
  _timeout = a.nget<u_int>("timeout", 25000, 10);
  _max_lookup_time = a.nget<uint>("maxlookuptime",4000,10);
  _purge_time = a.nget<uint>("purgetime",1000,10);
  _track_conncomp_timer = a.nget<uint>("track_conncomp_timer",0,10);
  _to_multiplier = a.nget<uint>("timeout_multiplier",3,10);
  _to_cheat = a.nget<uint>("timeout_cheat",0,10);
  printf("node %u first_ip %u\n",i,_first_ip);
}

void
Kelips::add_edge(int *matrix, int sz)
{
  vector<IPAddress> l;
  l.clear();
  for(map<IPAddress, Info *>::const_iterator ii = _info.begin();
      ii != _info.end();
      ++ii){
    if (ii->first!=ip() &&Network::Instance()->alive(ii->first))
      matrix[(first_ip()-1)*sz + getpeer(ii->first)->first_ip()-1] = 1;
  }
}

string
i2s(unsigned long long x)
{
  char buf[64];
  sprintf(buf, "%llu", x);
  return string(buf);
}

Kelips::~Kelips()
{
  if(ip() == 1){
    printf("rpc_bytes %.0f\n", _rpc_bytes);
    printf("%d good, %d ok failures, %d bad failures\n",
           _good_lookups, _ok_failures, _bad_failures);

    if(_good_lookups > 0)
      printf("avglat %.1f avghops %.2f\n",
             _good_latency / _good_lookups,
             _good_hops / _good_lookups); 
    print_stats();
  }
  if(ip() == 1 && nsta > 0){
    float sum = 0;
    for(int i = 0; i < nsta; i++)
      sum += sta[i];
    printf("avg stabilization rounds %.1f %d\n",
           sum / nsta, sta[nsta / 2]);
  }
}

// assign a score to a contact, to help decide which to keep.
// lower is better. pretty ad-hoc.
inline int
Kelips::contact_score(const Info &i)
{
  int rtt = i._rtt;
  if(rtt < 1)
    rtt = 200; // make a guess on the high side.
  int score = rtt + (i.age() / 128);
  return score;
}

// Find the worst contact in the given group.
// Actually oldest heartbeat has some freshness advantages.
// Returns 0 if there are less than _n_contacts.
//
// w/o rtt: p100/t100/e100 217.4 218.7 220.2
// w/ evict oldest rtt: 216.8 209.2 212.2
//    hmm: 237.2 231.5 232
//    zzz: 223 223 223
// w/ lookup via contact w/ best score: 225 229 216 222 204
// fix initial _rtt bug:  219 219
// record rtt after every doRPC: 206 203 207
// penalize nodes after failed RPC: 189 192 192 189
// lookup via closest contact, not best score: 191 197 191
// direct if we know the IP address: 178 168 172
IPAddress
Kelips::victim(int g)
{
  Info *worst = 0;
  int n_in_group = 0;

  for(map<IPAddress, Info *>::const_iterator ii = _info.begin();
      ii != _info.end();
      ++ii){
    Info *in = ii->second;
    if(ip2group(in->_ip) == g){
      n_in_group++;
      if(worst == 0 || contact_score(*in) > contact_score(*worst))
        worst = in;
    }
  }
  if(n_in_group >= _n_contacts && worst){
    return worst->_ip;
  } else {
    return 0;
  }
}

// Return contact w/ lowest rtt,
// or contact w/ newest hearbeat,
// or zero.
IPAddress
Kelips::closest_contact(int g)
{
  Info *best = 0;

  for(map<IPAddress, Info *>::const_iterator ii = _info.begin();
      ii != _info.end();
      ++ii){
    Info *in = ii->second;
    if(ip2group(in->_ip) == g){
      if(best == 0 ||
         (in->_rtt != -1 && best->_rtt != -1 && in->_rtt < best->_rtt) ||
         (in->_rtt != 9999 && in->_heartbeat > best->_heartbeat)){
        best = in;
      }
    }
  }
  if(best)
    return best->_ip;
  return 0;
}

void
Kelips::join(Args *a)
{
  notifyObservers(); // kick KelipsObserver so it calls all the init_state()s

  assert(_live == false);
  _live = true;

  IPAddress wkn = a->nget<IPAddress>("wellknown");
  if (0)
    printf("%qd %d join known=%d\n", now(), ip(), _info.size());
  assert(wkn != 0);

  if(wkn != ip()){
    // Remember well known node.
    gotinfo(Info(wkn, now()), -1);

    // Tell wkn about us, and ask it for a few random nodes.
    IPAddress myip = ip();
    vector<Info> ret;
    xRPC(wkn, 1 + 20 * 2 * 2, &Kelips::handle_join, &myip, &ret); //originally, robert sets this to 6, but i think it's actually big
    for(u_int i = 0; i < ret.size(); i++)
      gotinfo(ret[i], -1);
  }

  if(_started == false){
    _started = true;
    delaycb(1000, &Kelips::gossip, (void *) 0);
    delaycb(1000, &Kelips::purge, (void *) 0);
  }
}

void
Kelips::leave(Args *a)
{
  crash(a);
}

//
void
Kelips::crash(Args *a)
{
  if (0)
    printf("%qd %d crash\n", now(), ip());

  //cout << now()<<" crash on ip = " << ip() << ", first_ip = " << first_ip() << endl;

  assert(_live == true);
  _live = false;

  // XXX: Thomer says: not necessary
  // node()->crash();
  _info.clear();
  assert(_info.size() == 0);
}

// Return whether the node corresponding to a given key is alive.
// This is cheating, just for diagnostics.
bool
Kelips::node_key_alive(ID key)
{
  if(ip2id((IPAddress) key) == key){
    return Network::Instance()->alive((IPAddress) key);
  } 
  assert(0);

  // shut up compiler.
  return 0;
}

void
Kelips::lookup(Args *args)
{
  ID k = args->nget<ID>("key");
  assert(k);
  if (!node_key_alive(k))
    return;

  lookup_args *a = New lookup_args;
  a->key = args->nget<ID>("key");
  a->start = now();
  a->retrytimes = 0;
  a->history.clear();
  a->total_to = 0;
  a->num_to = 0;
  lookup_internal(a);
}

// In real Kelips, the file with key K is stored in group
// (K mod k), on a randomly chosen member of the group.
// All nodes in the group learn that K is on that node via
// gossiping filetuples. The Kelips paper doesn't talk about
// replicating files. Kelips has no direction notion
// of lookup(key).
//
// This implementation of lookup just looks for the host
// with a given key. I believe this is indistinguishable from
// looking for a file with a given key. It actually contacts
// the target host.
//
// Assuming iterative lookup, though not specified in the paper.
// XXX should send lookup to contact with lowest RTT.
// XXX should retry in various clever ways.
void
Kelips::lookup_internal(lookup_args *a)
{
  ID key = a->key;

  Time t1 = now();
  bool ok = lookup_loop(a);
  Time t2 = now();
  a->retrytimes++;

  bool oops = false;
  if(ok == false)
    oops = node_key_alive(key);

  IPAddress lasthop;
  if( a->history.size() > 0 ) {
    lasthop = a->history[a->history.size()-1];
  } else {
    lasthop = ip();
  }

  if (t2 - a->start >= _max_lookup_time) {
    if (Node::collect_stat()) 
      _bad_failures += 1;
    record_lookup_stat(ip(), lasthop, t2-a->start, false, false, a->history.size(), a->num_to, a->total_to);
  }else if (ok) {
    assert( lasthop == key );
    if (Node::collect_stat()) {
      _good_lookups += 1;
      _good_latency += t2 - t1;
      _good_hops += a->history.size();
    }
    record_lookup_stat(ip(), lasthop, t2-a->start, true, true, a->history.size(), a->num_to, a->total_to);
  }else if (oops) {
    if (Node::collect_stat()) 
      _bad_failures += 1;
    delaycb(100, &Kelips::lookup_internal, a);
    return;
  }else {
    //the destination node is dead
    if (a->retrytimes >= 2) 
      record_lookup_stat(ip(),lasthop,t2-a->start,false,false,a->history.size(),a->num_to,a->total_to);
  }
  delete a;
  if(0){
    printf("%qd %d lat=%d lookup(%qd) ", now(), ip(), (int)(t2 - t1),
           (unsigned long long) key);
    for(u_int i = 0; i < a->history.size(); i++)
      printf("%d ", a->history[i]);
    printf("%s%s   \n", ok ? "OK" : "FAIL", (!ok && oops) ? " OOPS" : "");
  }
}

// Keep trying to lookup.
bool
Kelips::lookup_loop(lookup_args *a) 
{
  // Are we looking for ourselves?
  if(a->key == id())
    return true;

  // Try an ordinary lookup via the closest contact.
  if(lookup1(a))
    return true;
  else if (now()-a->start>=_max_lookup_time)
    return false;

  // Try via each known contact.
  if(_k > 1){
    vector<IPAddress> cl = grouplist(id2group(a->key));
    for(u_int i = 0; i < cl.size(); i++)
      if(lookupvia(a,cl[i]))
        return true;
      else if (now()-a->start>=_max_lookup_time)
	return false;
  }

  // Try via random nodes a few times.
  for(int iter = 0; iter < 12; iter++){
    if(lookup2(a)) 
      return true;
    else if (now()-a->start>=_max_lookup_time)
      return false;
  }

  return false;
}

// Look up a key via closest contact.
// The contact should return the IP address of
// the lookup target, which we then try to talk to.
// This is only suitable for the fast/ordinary
// path, it doesn't try any alternate paths.
bool
Kelips::lookup1(lookup_args *a)
{
  IPAddress ip1 = 0;

  if(id2group(a->key) == group()){
    ip1 = find_by_id(a->key);
    if(ip1 == 0)
      return false;
  } else if((ip1 = find_by_id(a->key)) != 0){
    // go direct to a different group!
    // not mentioned in Kelips paper, of course, but seems
    // reasonable by analogy to Chord forwarding lookup to
    // known node with closest ID.
  } else {
    IPAddress ip = closest_contact(id2group(a->key));
    if(ip == 0)
      return false;

    assert(id2group(a->key) == id2group(ip));
    //bool ok = xRPC(ip, 3, &Kelips::handle_lookup1, &(a->key), &ip1, STAT_LOOKUP, &(a->total_to), &(a->num_to));
    lookup1_args aaa;
    aaa.key = a->key;
    aaa.dst_ip = ip;
    bool ok = xRPC(ip, 3, &Kelips::handle_lookup1, &aaa, &ip1, STAT_LOOKUP, &(a->total_to), &(a->num_to));
    a->history.push_back(ip);
    if(!ok || ip1 == 0 || (now()-a->start>=_max_lookup_time))
      return false;
    assert(ip1 != ip);
  }

  bool done = false;
  bool ok = xRPC(ip1, 2, &Kelips::handle_lookup_final, &(a->key), &done, STAT_LOOKUP, &(a->total_to), &(a->num_to));
  a->history.push_back(ip1);
  return(ok && done && (now()-a->start < _max_lookup_time));
}

// Look up a key via a given contact.
bool
Kelips::lookupvia(lookup_args *a, IPAddress via)
{
  IPAddress ip1 = 0;
  assert(id2group(a->key) == id2group(via));
  lookup1_args aaa;
  aaa.key = a->key;
  aaa.dst_ip = via;
  //bool ok = xRPC(via, 3, &Kelips::handle_lookup1, &(a->key), &ip1, STAT_LOOKUP,&(a->total_to), &(a->num_to));
  bool ok = xRPC(via, 3, &Kelips::handle_lookup1, &aaa, &ip1, STAT_LOOKUP,&(a->total_to), &(a->num_to));
  a->history.push_back(via);

  if(ok == false || ip1 == 0 || (now()-a->start>=_max_lookup_time))
    return false;

  bool done = false;
  ok = xRPC(ip1, 2, &Kelips::handle_lookup_final, &(a->key), &done, STAT_LOOKUP, &(a->total_to), &(a->num_to));
  a->history.push_back(ip1);

  return(ok && done && (now()-a->start<_max_lookup_time));
}

// ip: someone random we know about.
// ip1: random contact of ip in the target group.
// ip2: who ip1 thinks has the key.
bool
Kelips::lookup2(lookup_args *a) 
{
  vector<IPAddress> l = all();
  if(l.size() < 1)
    return false;
  IPAddress ip = l[random() % l.size()];

  IPAddress ip1 = 0;
  bool ok = xRPC(ip, 2, &Kelips::handle_lookup2, &(a->key), &ip1, STAT_LOOKUP, &(a->total_to),&(a->num_to));
  a->history.push_back(ip);

  if(!ok || ip1 == 0 || (now()-a->start >= _max_lookup_time))
    return false;

  IPAddress ip2 = 0;
  assert(id2group(a->key) == id2group(ip1));
  //ok = xRPC(ip1, 2, &Kelips::handle_lookup1, &(a->key), &ip2, STAT_LOOKUP, &(a->total_to),&(a->num_to));
  lookup1_args aaa;
  aaa.key = a->key;
  aaa.dst_ip = ip1;
  ok = xRPC(ip1, 2, &Kelips::handle_lookup1, &aaa, &ip2, STAT_LOOKUP, &(a->total_to),&(a->num_to));
  a->history.push_back(ip1);

  if(!ok || ip2 == 0 || (now()-a->start>=_max_lookup_time))
    return false;

  bool done = false;
  ok = xRPC(ip2, 2, &Kelips::handle_lookup_final, &(a->key), &done, STAT_LOOKUP, &(a->total_to),&(a->num_to));
  a->history.push_back(ip2);

  return(ok && done &&(now()-a->start<_max_lookup_time));
}

// Someone in our group wants us to return them a
// random contact in the key's group.
void
Kelips::handle_lookup2(ID *kp, IPAddress *res)
{
  if((*res = find_by_id(*kp)) != 0)
    return;

  vector<IPAddress> cl = grouplist(id2group(*kp));
  if(cl.size() > 0)
    *res = cl[random() % cl.size()];
  else
    *res = 0;
#if 0
  printf("%qd %d handle_lookup2(%qd) %d\n",
         now(), ip(), *kp, *res);
#endif
}

// Do we have the given node/key ID in our local state?
IPAddress
Kelips::find_by_id(ID key)
{
  vector<IPAddress> l = all();
  for(u_int i = 0; i < l.size(); i++)
    if(ip2id(l[i]) == key)
      return l[i];
  return 0;
}

// Someone outside the group is asking us which node is
// responsible for the given key.
void
//Kelips::handle_lookup1(ID *kp, IPAddress *res)
Kelips::handle_lookup1(lookup1_args *kp, IPAddress *res)
{
  //ID key = *kp;
  ID key = kp->key;
  assert(Network::Instance()->alive(kp->dst_ip));
  assert(ip() == kp->dst_ip);

  assert(id2group(key) == group());

  if(id() == key){
    *res = ip();
    return;
  }

  *res = find_by_id(key);

#if 0
  if(*res == 0)
    printf("%qd %d handle_lookup1(%qd) failed\n", now(), ip(), key);
#endif
}

void
Kelips::handle_lookup_final(ID *kp, bool *done)
{
  if(*kp == id()){
    *done = true;
  } else {
    *done = false;
  }
#if 0
  printf("%qd %d handle_lookup_final(%qd) %s\n",
         now(), ip(), *kp, *done == true ? "ok" : "OOPS");
#endif
}

void
Kelips::insert(Args *a)
{
}

// A new node is asking us to tell it about some random
// existing nodes. This is basically "pull" gossip.
// Only the well-known-node should receive this RPC.
// It remembers the caller to help seed its _info.
void
Kelips::handle_join(IPAddress *caller, vector<Info> *ret)
{
  gotinfo(Info(*caller, now()), -1); // XXX caller should supply an Info

  // send a super-big ration on join, per Indranil's e-mail.
  *ret = gossip_msg(ip2group(*caller), 20, 20);
}

// This node has just learned about another node.
// Remember the information, prepare to gossip it.
// Enforce the invariant that we have at most 2 contacts
// for each foreign group.
// If rtt != -1, it's newly measured.
void
Kelips::gotinfo(Info i, int rtt)
{
  if(i._ip == ip())
    return;

  assert(i._ip);

  if(_info.find(i._ip) == _info.end()){
    int g = ip2group(i._ip);
    bool add = false;
    if(g == group()){
      add = true;
    } else {
      IPAddress x = victim(g); // pick the lamest contact to replace.
      assert(x == 0 || ip2group(x) == g);
      if(x == 0){
        add = true;
      } else if(i.age() < 4 * _info[x]->age()){
        Info *in = _info[x];
        assert(in);
        _info.erase(x);
        delete in;
        add = true;
      }
    }
    if(add){
      _info[i._ip] = New Info(i);
      _info[i._ip]->_rounds = _item_rounds;
      _info[i._ip]->_rtt = -1;
    }
  } else if (i._heartbeat > _info[i._ip]->_heartbeat){
    _info[i._ip]->_heartbeat = i._heartbeat;
    if(_info[i._ip]->_rtt == 9999){
      // we once got an RPC timeout, but node seems to have restarted.
      _info[i._ip]->_rtt = -1;
    }
  }

  if(rtt != -1 && _info.find(i._ip) != _info.end())
    _info[i._ip]->_rtt = rtt;
}

// Return a list of all the IP addresses in _info.
vector<IPAddress>
Kelips::all()
{
  vector<IPAddress> l;
  for(map<IPAddress, Info *>::const_iterator ii = _info.begin();
      ii != _info.end();
      ++ii){
    l.push_back(ii->first);
  }
  return l;
}

// Return the list of the IP addresses in _info in our group.
vector<IPAddress>
Kelips::grouplist(int g)
{
  vector<IPAddress> l;
  for(map<IPAddress, Info *>::const_iterator ii = _info.begin();
      ii != _info.end();
      ++ii){
    if(ip2group(ii->second->_ip) == g)
      l.push_back(ii->first);
  }
  return l;
}

// Return the list of the IP addresses *not* in our group.
vector<IPAddress>
Kelips::notgrouplist(int g)
{
  vector<IPAddress> l;
  for(map<IPAddress, Info *>::const_iterator ii = _info.begin();
      ii != _info.end();
      ++ii){
    if(ip2group(ii->second->_ip) != g)
      l.push_back(ii->first);
  }
  return l;
}

// Given a list of nodes, limit it to ones that are new (or old).
// "new" means _rounds > 0.
vector<IPAddress>
Kelips::newold(vector<IPAddress> a, bool xnew)
{
  vector<IPAddress> b;
  for(u_int i = 0; i < a.size(); i++)
    if((_info[a[i]]->_rounds > 0) == xnew)
      b.push_back(a[i]);
  return b;
}

// Randomize the order of a list of nodes.
vector<IPAddress>
Kelips::randomize(vector<IPAddress> a)
{
  if(a.size() < 1)
    return a;
  for(u_int i = 0; i < a.size(); i++){
    int j = random() % a.size();
    IPAddress tmp = a[i];
    a[i] = a[j];
    a[j] = tmp;
  }
  return a;
}

// Given a list of nodes in l, add a ration of them to msg,
// half new and the rest old.
void
Kelips::newold_msg(vector<Info> &msg, vector<IPAddress> l, u_int ration)
{
  u_int n = 0;
  {
    // Half the ration for newly learned nodes.
    vector<IPAddress> nl = randomize(newold(l, true));
    for(u_int i = 0; n <= ration / 2 && i < nl.size(); i++, n++){
      Info *ip = _info[nl[i]];
      assert(ip->_rounds > 0);
      ip->_rounds -= 1;
      msg.push_back(*ip);
    }
  }
  {
    // The remainder of the ration for existing nodes.
    vector<IPAddress> ol = randomize(newold(l, false));
    for(u_int i = 0; n < ration && i < ol.size(); i++, n++){
      Info *ip = _info[ol[i]];
      assert(ip->_rounds == 0);
      msg.push_back(*ip);
    }
  }
}

// Create a gossip message.
// Always include an entry for ourselves.
// g allows targeted "pull" gossip during join, critical
// to avoid disconnection!
// XXX ought to send newer heartbeats preferentially????
vector<Kelips::Info>
Kelips::gossip_msg(int g, u_int gr, u_int cr)
{
  vector<Info> msg;

  // Include this node w/ new heartbeat in every gossip.
  msg.push_back(Info(ip(), now()));

  // Add some nodes from our group.
  newold_msg(msg, grouplist(g), gr);

  // Add some contact nodes.
  newold_msg(msg, notgrouplist(g), cr);

  assert(msg.size() <= 1 + gr + cr);

  return msg;
}

void
Kelips::handle_ping(void *xx, void *yy)
{
}

// One round of gossiping.
// Pick a few items to send, and send them to a few other nodes.
// XXX ought to send to nearby nodes preferentially (Section 2.1).
void
Kelips::gossip(void *junk)
{
  if(_live){
    vector<Info> msg = gossip_msg(group(), _group_ration, _contact_ration);

    {
      vector<IPAddress> gl = randomize(grouplist(group()));
      for(u_int i = 0; i < _group_targets && i < gl.size(); i++){
	//since each gossip contains the heartbeat timer for a node, add extra 4 byte
        xRPC(gl[i], 2 * msg.size(), &Kelips::handle_gossip, &msg, (void *) 0);
      }
    }

    {
      vector<IPAddress> cl = randomize(notgrouplist(group()));
      for(u_int i = 0; i < _contact_targets && i < cl.size(); i++){
        xRPC(cl[i], 2 * msg.size(), &Kelips::handle_gossip, &msg, (void *) 0);
      }
    }

    // ping one random node to find its RTT.
    {
      vector<IPAddress> l = all();
      for(int iters = 0; l.size() > 0 && iters < 10; iters++){
        IPAddress xip = l[random() % l.size()];
        if(_info[xip]->_rtt == -1){
          xRPC(xip, 2, &Kelips::handle_ping, (void*)0, (void*)0);
          break;
        }
      }
    }
  }

  delaycb(random() % (2 *_round_interval), &Kelips::gossip, (void *) 0);
}

void
Kelips::handle_gossip(vector<Info> *msg, void *ret)
{
  u_int i;

  for(i = 0; i < msg->size(); i++){
    gotinfo((*msg)[i], -1);
  }
}

// Periodically get rid of _info entries that have
// expired heartbeats.
void
Kelips::purge(void *junk)
{
  vector<IPAddress> l = all();
  for(u_int i = 0; i < l.size(); i++){
    Info *in = _info[l[i]];
    int to =
      (ip2group(in->_ip) == group() ? _timeout : 2*_timeout);
    if(in->_heartbeat + to < now()){
#if 0
      printf("%qd %d timed out %d %s\n",
             now(), ip(), in->_ip,
             node_key_alive(ip2id(in->_ip)) ? "oops" : "ok");
#endif
      _info.erase(l[i]);
      delete in;
    }
  }

  delaycb(_purge_time, &Kelips::purge, (void *) 0);
}

// Called by KelipsObserver::init_state() with the complete list
// of nodes to help us initialize our routing tables faster (i.e. cheat).
// So in most nodes it's called before join().
void
Kelips::init_state(const set<Node*> *lid)
{
  for(set<Node*>::const_iterator i = lid->begin(); i != lid->end(); ++i) {
    Kelips *k = dynamic_cast<Kelips*>(*i);
    assert(k);
    if(k->ip() == ip())
      continue;
    Time rtt = 2 * Network::Instance()->gettopology()->latency(ip(), k->ip());
    gotinfo(Info(k->ip(), now()), rtt);
  }
}

// KelipsObserver wants to know if we've stabilized.
// lid is a list of all live nodes.
bool
Kelips::stabilized(vector<ID> lid)
{
  // Do we know about all nodes in our own group?
  for(u_int i = 0; i < lid.size(); i++)
    if(id2group(lid[i]) == group() && find_by_id(lid[i]))
      return false;

  // Do we know of two contacts from each other group?
  int *cc = (int *) malloc(_k * sizeof(int));
  memset(cc, '\0', _k * sizeof(int));
  for(u_int i = 0; i < lid.size(); i++)
    if(find_by_id(lid[i]))
      cc[id2group(lid[i])] += 1;
  for(int i = 0; i < _k; i++){
    if(cc[i] < _n_contacts){
      delete cc;
      return false;
    }
  }

  delete cc;
  return true;
}

// Kelips just did an RPC. The request contained nsent IDs,
// the reply contained nrecv IDs. Update the RPC statistics.
// Jinyang/Jeremy convention:
//   20 bytes header, 4 bytes/ID, 1 byte/other
// (Kelips paper says 40 bytes per gossip entry...)
void
Kelips::rpcstat(bool ok, IPAddress dst, int latency, int nitems, uint type)
{

  if (Node::collect_stat()) {
    _rpc_bytes += 20 + nitems * 4; // paper says 40 bytes per node entry
    record_bw_stat( type, nitems, 0 );
    if(ok) {
      _rpc_bytes += 20;
      record_bw_stat( type, 0, 0 );
    }
  }

  if(ok)
    gotinfo(Info(dst, now()), latency);

  if(ok == false && _info.find(dst) != _info.end()){
    _info[dst]->_rtt = 9999;
  }
}
