#include "p2pevent.h"
#include "node.h"
#include "args.h"
#include "protocol.h"
#include "network.h"
#include "parse.h"
#include <lib9.h>
#include <thread.h>
#include <iostream>
#include <vector>
#include <string>
#include <stdio.h>
using namespace std;

P2PEvent::P2PEvent()
{
}

// expects: timestamp node-id protocol:operation-id [arguments]
//
// see Protocol::dispatch() for the mapping from operation-id to operation
//
P2PEvent::P2PEvent(vector<string> *v) : Event(v)
{
  // node-id
  IPAddress ip = (IPAddress) atoi((*v)[0].c_str());
  this->node = ip2node(ip);
  if(!this->node) {
    cerr << "can't execute event on non-exiting node with id " << ip << endl;
    return;
  }

  // protocol
  vector<string> proto_action = split((*v)[1], ":");
  this->protocol = proto_action[0];

  // operation-id
  this->event = (Protocol::EventID) atoi(proto_action[1].c_str());

  // create a map for the arguments
  this->args = new Args;
  assert(this->args);
  for(unsigned int i=2; i<v->size(); i++) {
    vector<string> arg = split((*v)[i], "=");
    this->args->insert(make_pair(arg[0], arg[1]));
  }
}


P2PEvent::~P2PEvent()
{
}

void
P2PEvent::execute()
{
  // get node, protocol on that node, application interface for that protocol
  // and invoke the event
  Protocol *proto = node->getproto(protocol);
  if(!proto) {
    cerr << "ERROR: protocol " << protocol << " not running on node " << node->ip() << endl;
    threadexitsall(0);
  }
  Channel *c = proto->appchan();
  assert(c);

  // XXX: this seems to be non-blocking.  is that correct?
  P2PEvent *me = this;
  send(c, &me);
}
