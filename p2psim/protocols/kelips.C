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

Kelips::Kelips(Node *n, Args a)
  : P2Protocol(n)
{
  _k = a.nget<unsigned>("k", 20, 10);
  assert(_k > 0);
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
  check();
#if 0
  string s;
  s = i2s(ip());
  s += "/";
  s += i2s(group());
  s += ": ";
  vector<IPAddress> v = all();
  for(unsigned int i = 0; i < v.size(); i++){
    s += i2s(v[i]);
    s += "/";
    s += i2s(ip2group(v[i]));
    s += " ";
  }
  s += "\n";
  cout << s;
#endif
}

// Make sure we know about exactly the nodes we should know.
void
Kelips::check()
{
  int gc[_k]; // how many do we know of from each group?
  for(int i = 0; i < _k; i++)
    gc[i] = 0;

  vector<IPAddress> l = all();
  for(unsigned int i = 0; i < l.size(); i++){
    Info *in = _info[l[i]];
    int g = ip2group(in->_ip);
    gc[g] += 1;
  }

  string s;
  s = i2s(ip());
  s += ": ";
  for(int i = 0; i < _k; i++){
    s += i2s(gc[i]);
    s += " ";
  }
  s += "\n";
  cout << s;
}

void
Kelips::join(Args *a)
{
  IPAddress wkn = a->nget<IPAddress>("wellknown");
  cout << "Kelips::join ip=" << ip() << " wkn=" << wkn << "\n";
  assert(wkn != 0);

  if(wkn != ip()){
    // Remember well known node.
    gotinfo(Info(wkn));

    // Tell wkn about us, and ask it for a few random nodes.
    IPAddress myip = ip();
    vector<Info> ret;
    doRPC(wkn, &Kelips::handle_join, &myip, &ret);
    for(unsigned int i = 0; i < ret.size(); i++)
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
  gotinfo(Info(*caller));
  if(_info.size() > 0){
    for(int i = 0; i < _gossip_items; i++){
      IPAddress x = random_peer();
      (*ret).push_back(*(_info[x]));
    }
  }
}

// This node has just learned about another node.
// Remember the information, prepare to gossip it.
void
Kelips::gotinfo(Info i)
{
  if(i._ip != ip() && _info.find(i._ip) == _info.end()){
    cout << ip() << " got " << i._ip << "\n";
    _info[i._ip] = new Info(i);
  }
}

// Return the IP address of a random peer chosen
// from our group members or contacts.
IPAddress
Kelips::random_peer()
{
  int n = random() % _info.size();
  map<IPAddress, Info *>::const_iterator i;
  for(i = _info.begin(); n-- > 0; ++i)
    ;
  return i->first;
}

// Send one gossip message to one other node.
// Chooses what to say and who to say it to.
// Always include an entry for ourselves.
// XXX ought to send the most recently learned items.
// XXX ought to send to nearby nodes preferentially.
void
Kelips::gossip(void *junk)
{
  if(_info.size() > 0){
    IPAddress target = random_peer();
    vector<Info> msg;
    msg.push_back(Info(ip()));
    for(int i = 0; i < _gossip_items; i++){
      IPAddress x = random_peer();
      msg.push_back(*(_info[x]));
    }
    doRPC(target, &Kelips::handle_gossip, &msg, (void *) 0);
  }

  delaycb(_gossip_interval, &Kelips::gossip, (void *) 0);
}

void
Kelips::handle_gossip(vector<Info> *msg, void *ret)
{
  unsigned int i;

  for(i = 0; i < msg->size(); i++){
    gotinfo((*msg)[i]);
  }
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

// Periodically get rid of irrelevant _info entries.
// Retains only fresh-looking entries from the same
// group, plus two contacts in each other group.
// XXX should keep the closest contacts, not random ones.
// XXX why do this in the background? why not just decide
//   which contacts to accept in gotinfo()?
void
Kelips::purge(void *junk)
{
  int gc[_k]; // how many do we know of from each group?
  for(int i = 0; i < _k; i++)
    gc[i] = 0;

  vector<IPAddress> l = all();
  for(unsigned int i = 0; i < l.size(); i++){
    Info *in = _info[l[i]];
    int g = ip2group(in->_ip);
    if(g != group() && gc[g] >= _n_contacts){
      _info.erase(in->_ip);
      delete in;
    } else {
      gc[g] += 1;
    }
  }

  delaycb(_purge_interval, &Kelips::purge, (void *) 0);
}
