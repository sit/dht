#include "p2pevent.h"
#include "node.h"
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

Protocol::event_f
P2PEvent::name2fn(string name)
{
  if(name == "join" || name == "0")
    return &Protocol::join;
  if(name == "leave" || name == "1")
    return &Protocol::leave;
  if(name == "crash" || name == "2")
    return &Protocol::crash;
  if(name == "insert" || name == "3")
    return &Protocol::insert;
  if(name == "lookup" || name == "4")
    return &Protocol::lookup;
  assert(0);
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
  this->fn = name2fn(proto_action[1]);

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
  if (proto->node()->alive())
    (proto->*fn)(args);
}
