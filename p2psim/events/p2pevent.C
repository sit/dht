/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
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

#include "p2pevent.h"
#include "p2psim/network.h"
#include "p2psim/parse.h"
#include <iostream>
using namespace std;

P2PEvent::P2PEvent() : Event("P2PEvent")
{
}

P2Protocol::event_f
P2PEvent::name2fn(string name)
{
  // XXX: no longer supported
  if(name == "insert" || name == "3" || name == "leave" || name == "1")
    assert(false);

  if(name == "join" || name == "0") {
    type = "join";
    return &P2Protocol::join;
  }
  if(name == "crash" || name == "2") {
    type = "crash";
    return &P2Protocol::crash;
  }
  if(name == "lookup" || name == "4") {
    type = "lookup";
    return &P2Protocol::lookup;
  }
  if(name == "nodeevent" || name == "5") {
    type = "nodeevent";
    return &P2Protocol::nodeevent;
  }
  assert(0);
}

// expects: timestamp node-id node:operation-id [arguments]
//
// see Node::dispatch() for the mapping from operation-id to operation
//
P2PEvent::P2PEvent(vector<string> *v) : Event("P2PEvent", v)
{
  // node-id
  IPAddress first_ip = (IPAddress) strtoull((*v)[0].c_str(), NULL, 10);
  this->node = Network::Instance()->getnodefromfirstip(first_ip);
  if(!this->node) {
    cerr << "can't execute event on non-existing node with id " << first_ip << endl;
    return;
  }

  // operation-id
  //this->fn = name2fn(proto_action[1]);
  this->fn = name2fn((*v)[1]);

  // create a map for the arguments
  this->args = New Args(v, 2);
  assert(this->args);
}

P2PEvent::P2PEvent(Time ts, IPAddress first_ip, string operation, Args *a) :
    Event("P2PEvent", ts, true)
{
  this->node = Network::Instance()->getnodefromfirstip(first_ip);
  this->fn = name2fn(operation);
  if(!(this->args = a)) {
    this->args = New Args();
    assert(this->args);
  }
}


P2PEvent::~P2PEvent()
{
  delete args;
}

void
P2PEvent::execute()
{
  // get node, node on that node, application interface for that node
  // and invoke the event
  P2Protocol *proto = dynamic_cast<P2Protocol*>(node);
  assert(proto);

  // if join: set proto-alive flag
  if (fn == &P2Protocol::join) 
    proto->set_alive(true);

  if (proto->alive())
    (proto->*fn)(args);

  // if this was a crash: set proto-dead flag
  if (fn == &P2Protocol::crash) 
    proto->set_alive(false);
}
