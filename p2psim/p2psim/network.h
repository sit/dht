/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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
 */

#ifndef __NETWORK_H
#define __NETWORK_H

#include "topology.h"
#include "protocol.h"
#include "failuremodel.h"
#include <list>
using namespace std;

class Network : public Threaded {
public:
  static Network* Instance() { return Instance(0, 0); }
  static Network* Instance(Topology*, FailureModel*);
  Channel* failedpktchan() { return _failedpktchan; }
  Channel* pktchan() { return _pktchan; }
  Channel* nodechan() { return _nodechan; }

  // information
  Node* getnode(IPAddress id) { return _nodes[id]; }
  Topology *gettopology() { return _top; }
  FailureModel *getfailuremodel() { return _failure_model; }
  list<Protocol*> getallprotocols(string);

  ~Network();

private:
  Network(Topology*, FailureModel*);

  virtual void run();

  static Network *_instance;
  typedef hash_map<IPAddress, Node*> NM;
  typedef NM::const_iterator NMCI;
  NM _nodes;
  Topology *_top;
  FailureModel *_failure_model;

  Channel *_pktchan;
  Channel *_failedpktchan;
  Channel *_nodechan;
};

#endif // __NETWORK_H
