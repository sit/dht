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
    if(i != group() && gc[i] != 2)
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

  delaycb(1000, &Kelips::gossip, (void *) 0);
  delaycb(1000, &Kelips::purge, (void *) 0);
}

void
Kelips::leave(Args *a)
{
}

void
Kelips::crash(Args *a)
{
}

void
Kelips::lookup(Args *a)
{
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
void
Kelips::gotinfo(Info i)
{
  if(i._ip == ip())
    return;

  if(_info.find(i._ip) == _info.end()){
    int g = ip2group(i._ip);
    if(g == group() || grouplist(g).size() < 2){
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
Kelips::contactlist(int g)
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
  newold_msg(msg, contactlist(g), _contact_ration);

  assert(msg.size() <= 1 + _group_ration + _contact_ration);

  return msg;
}

// One round of gossiping.
// Pick a few items to send, and send them to a few other nodes.
// XXX ought to send to nearby nodes preferentially (Section 2.1).
void
Kelips::gossip(void *junk)
{
  vector<Info> msg = gossip_msg(group());

  {
    vector<IPAddress> gl = randomize(grouplist(group()));
    for(u_int i = 0; i < _group_targets && i < gl.size(); i++){
      doRPC(gl[i], &Kelips::handle_gossip, &msg, (void *) 0);
    }
  }

  {
    vector<IPAddress> cl = randomize(contactlist(group()));
    for(u_int i = 0; i < _contact_targets && i < cl.size(); i++){
      doRPC(cl[i], &Kelips::handle_gossip, &msg, (void *) 0);
    }
  }

  _rounds++;

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

// Periodically get rid of irrelevant _info entries.
// Retains only fresh-looking entries from the same
// group, plus two contacts in each other group.
// XXX
void
Kelips::purge(void *junk)
{
  delaycb(_purge_interval, &Kelips::purge, (void *) 0);
}
