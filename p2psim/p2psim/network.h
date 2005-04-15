/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu),
 *                    Robert Morris (rtm@csail.mit.edu).
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

#ifndef __NETWORK_H
#define __NETWORK_H

#include "topology.h"
#include "node.h"
#include "failuremodel.h"
#include "bighashmap.hh"
#include <list>
using namespace std;

class Network : public Threaded {
public:
  static Network* Instance() { return Instance(0, 0); }
  static Network* Instance(Topology*, FailureModel*);
  Channel* nodechan() { return _nodechan; }
  void send(Packet *);

  // observers
  Node* getnode(IPAddress id) { return _nodes[first_ip(id)];}

  Node* getnodefromfirstip(IPAddress f) {
    return _nodes[f];
  }
  IPAddress first2currip (IPAddress first_ip) { return _nodes[first_ip]->ip();}
  Topology *gettopology() { return _top; }
  const set<Node*> *getallnodes();
  vector<IPAddress> *getallfirstips();
  unsigned size() { return _nodes.size(); }
  Time avglatency();
  bool alive(IPAddress ip) {
    Node *n = getnode(ip);
    return (n->ip()==ip && n->alive());
  }

  // 
  IPAddress unused_ip();
  void map_ip(IPAddress, IPAddress);
  IPAddress first_ip(IPAddress);
  bool changed() { return _changed; }

  ~Network();

private:
  Network(Topology*, FailureModel*);

  virtual void run();
  float gaussian(double var);

  static Network *_instance;

  HashMap<IPAddress, Node*> _nodes;
  Topology *_top;
  FailureModel *_failure_model;

  set<Node*> *_all_nodes;
  vector<IPAddress> *_all_ips;
  HashMap<IPAddress, bool> _corpses;

  HashMap<IPAddress, IPAddress> _new2old;
  IPAddress _highest_ip;
  bool _changed;
  Channel *_nodechan;
};

#endif // __NETWORK_H
