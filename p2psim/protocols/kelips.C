/*
 * Copyright (c) 2003 Robert Morris
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
#include <map>
using namespace std;

// Record stabilization times.
int sta[1000];
int nsta;

Kelips::Kelips(Node *n, Args a)
  : P2Protocol(n)
{
  _k = a.nget<unsigned>("k", 20, 10);
  assert(_k > 0);
  _rounds = 0;
  _stable = false;
  _started = false;
  _live = false;
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
  check(true);
  if(ip() == 1 && nsta > 0){
    float sum = 0;
    for(int i = 0; i < nsta; i++)
      sum += sta[i];
    printf("avg stabilization rounds %.1f %d\n",
           sum / nsta, sta[nsta / 2]);
  }
}

// Do we know the nodes we are supposed to know?
void
Kelips::check(bool doprint)
{
  return;

  short gc[100]; // how many do we know of from each group?
  assert(_k < 100);
  for(int i = 0; i < _k; i++)
    gc[i] = 0;

  vector<IPAddress> l = all();
  for(u_int i = 0; i < l.size(); i++){
    Info *in = _info[l[i]];
    int g = ip2group(in->_ip);
    gc[g] += 1;
  }

  bool stable = true;
  if(gc[group()] != _k - 1)
    stable = false;
  for(int i = 0; i < _k; i++)
    if(i != group() && gc[i] != _n_contacts)
      stable = false;

  if(_stable == false && stable){
    sta[nsta++] = _rounds;
    _stable = true;
    cout << now() << " " << ip() << " stable after " << _rounds << " rounds\n";
  }

  if(doprint && stable == false){
    string s;
    s = i2s(ip());
    s += (_stable ? " s" : " u");
    s += ": ";
    for(int i = 0; i < _k; i++){
      s += i2s(gc[i]);
      s += " ";
    }
    s += "\n";
    cout << s;
  }
}

void
Kelips::join(Args *a)
{
  assert(_live == false);
  _live = true;

  IPAddress wkn = a->nget<IPAddress>("wellknown");
  cout << "Kelips::join ip=" << ip() << " wkn=" << wkn << "\n";
  assert(wkn != 0);

  if(wkn != ip()){
    // Remember well known node.
    gotinfo(Info(wkn, now()));

    // Tell wkn about us, and ask it for a few random nodes.
    IPAddress myip = ip();
    vector<Info> ret;
    doRPC(wkn, &Kelips::handle_join, &myip, &ret);
    for(u_int i = 0; i < ret.size(); i++)
      gotinfo(ret[i]);
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
  assert(_live == true);
  _live = false;

  node()->crash();
  _info.clear();
  assert(_info.size() == 0);
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
Kelips::lookup(Args *args)
{
  ID key = args->nget<ID>("key");

  vector<IPAddress> history;

  bool ok = lookup_loop(key, history);

  printf("%qd %d lookup(%qd) ", now(), ip(), key);
  for(u_int i = 0; i < history.size(); i++)
    printf("%d ", history[i]);
  printf("%s   ", ok ? "OK" : "FAIL");

  vector<IPAddress> l = all();
  for(u_int i = 0; i < l.size(); i++)
    printf("%d ", l[i]);
  printf("\n");
}

// Keep trying to lookup.
bool
Kelips::lookup_loop(ID key, vector<IPAddress> &history)
{
  // Are we looking for ourselves?
  if(key == id())
    return true;

  // Ordinary lookup via a contact.
  if(lookup1(key, history))
    return true;

  // Try various random things a bunch of times.
  for(int iter = 0; iter < 6; iter++){
    if((random() % 2) == 0){
      if(lookup1(key, history))
        return true;
    } else {
      if(lookup2(key, history))
        return true;
    }
  }

  return false;
}

// Look up a key via a randomly chosen contact.
// The contact should return the IP address of
// the lookup target, which we then try to talk to.
// XXX Try closer contact first.
bool
Kelips::lookup1(ID key, vector<IPAddress> &history)
{
  IPAddress ip1 = 0;

  if(id2group(key) == group()){
    ip1 = find_by_id(key);
    if(ip1 == 0)
      return false;
  } else {
    vector<IPAddress> cl = grouplist(id2group(key));
    if(cl.size() < 1)
      return false;
    IPAddress ip = cl[random() % cl.size()];

    history.push_back(ip);
    bool ok = doRPC(ip, &Kelips::handle_lookup1, &key, &ip1);
    if(!ok || ip1 == 0)
      return false;
  }

  bool done = false;
  history.push_back(ip1);
  bool ok = doRPC(ip1, &Kelips::handle_lookup_final, &key, &done);

  return(ok && done);
}

// Look up a key via a randomly member of our own group,
// hoping that they will have better contact info.
bool
Kelips::lookup2(ID key, vector<IPAddress> &history)
{
  vector<IPAddress> gl = grouplist(group());
  if(gl.size() < 1)
    return false;
  IPAddress ip = gl[random() % gl.size()];

  IPAddress ip1 = 0;
  history.push_back(ip);
  bool ok = doRPC(ip, &Kelips::handle_lookup2, &key, &ip1);
  if(!ok || ip1 == 0)
    return false;

  IPAddress ip2 = 0;
  history.push_back(ip1);
  ok = doRPC(ip1, &Kelips::handle_lookup1, &key, &ip2);
  if(!ok || ip2 == 0)
    return false;

  bool done = false;
  history.push_back(ip2);
  ok = doRPC(ip2, &Kelips::handle_lookup_final, &key, &done);

  return(ok && done);
}

// Someone in our group wants us to return them a
// random contact that might be useful in finding
// key *kp.
void
Kelips::handle_lookup2(ID *kp, IPAddress *res)
{
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
Kelips::handle_lookup1(ID *kp, IPAddress *res)
{
  ID key = *kp;

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
  gotinfo(Info(*caller, now())); // XXX caller should supply an Info
  *ret = gossip_msg(ip2group(*caller));
}

// This node has just learned about another node.
// Remember the information, prepare to gossip it.
// Enforce the invariant that we have at most 2 contacts
// for each foreign group.
// XXX should choose closest contacts.
// XXX actually favors contacts with newer heartbeats. not in paper...
void
Kelips::gotinfo(Info i)
{
  if(i._ip == ip())
    return;

  assert(i._ip);

  if(_info.find(i._ip) == _info.end()){
    int g = ip2group(i._ip);
    vector<IPAddress> gl = randomize(grouplist(g));
    bool add = false;
    if(g == group() || gl.size() < 2){
      add = true;
    } else if(i._heartbeat > _info[gl[0]]->_heartbeat){
      Info *in = _info[gl[0]];
      _info.erase(gl[0]);
      delete in;
      add = true;
    }
    if(add){
      _info[i._ip] = new Info(i);
      _info[i._ip]->_rounds = _item_rounds;
    }
  } else if (i._heartbeat > _info[i._ip]->_heartbeat){
    _info[i._ip]->_heartbeat = i._heartbeat;
  }

  check(false);
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
    for(u_int i = 0; n < ration / 2 && i < nl.size(); i++, n++){
      Info *ip = _info[nl[i]];
      ip->_rounds -= 1;
      msg.push_back(*ip);
    }
  }
  {
    // The remainder of the ration for existing nodes.
    vector<IPAddress> ol = randomize(newold(l, false));
    for(u_int i = 0; n < ration && i < ol.size(); i++, n++){
      Info *ip = _info[ol[i]];
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
Kelips::gossip_msg(int g)
{
  vector<Info> msg;

  // Include this node w/ new heartbeat in every gossip.
  msg.push_back(Info(ip(), now()));

  // Add some nodes from our group.
  newold_msg(msg, grouplist(g), _group_ration);

  // Add some contact nodes.
  newold_msg(msg, notgrouplist(g), _contact_ration);

  assert(msg.size() <= 1 + _group_ration + _contact_ration);

  return msg;
}

// One round of gossiping.
// Pick a few items to send, and send them to a few other nodes.
// XXX ought to send to nearby nodes preferentially (Section 2.1).
void
Kelips::gossip(void *junk)
{
  if(_live){
    vector<Info> msg = gossip_msg(group());

    {
      vector<IPAddress> gl = randomize(grouplist(group()));
      for(u_int i = 0; i < _group_targets && i < gl.size(); i++){
        doRPC(gl[i], &Kelips::handle_gossip, &msg, (void *) 0);
      }
    }

    {
      vector<IPAddress> cl = randomize(notgrouplist(group()));
      for(u_int i = 0; i < _contact_targets && i < cl.size(); i++){
        doRPC(cl[i], &Kelips::handle_gossip, &msg, (void *) 0);
      }
    }

    _rounds++;
  }

  delaycb(_round_interval, &Kelips::gossip, (void *) 0);
}

void
Kelips::handle_gossip(vector<Info> *msg, void *ret)
{
  u_int i;

  for(i = 0; i < msg->size(); i++){
    gotinfo((*msg)[i]);
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
      (ip2group(in->_ip) == group() ? _group_timeout : _contact_timeout);
    if(in->_heartbeat + to < now()){
      cout << now() << " " << ip() << " timed out " << in->_ip << "\n";
      _info.erase(l[i]);
      delete in;
    }
  }

  delaycb(1000, &Kelips::purge, (void *) 0);
}

// Called by KelipsObserver::init_state() with the complete list
// of nodes to help us initialize our routing tables faster.
// This is really cheating.
void
Kelips::init_state(list<Protocol*> lid)
{
  printf("%qd %d init_state _live=%d\n",
         now(), ip(), _live);
  for(list<Protocol*>::const_iterator i = lid.begin(); i != lid.end(); ++i) {
    Kelips *k = (Kelips *) *i;
    if(k->ip() == ip())
      continue;
    gotinfo(Info(k->ip(), now()));
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
