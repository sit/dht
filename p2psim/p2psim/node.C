/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu),
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

#include "parse.h"
#include "network.h"
#include "args.h"
#include "protocols/protocolfactory.h"
#include "p2psim/threadmanager.h"
#include <iostream>
using namespace std;

string Node::_protocol = ""; 
Args Node::_args;
Time Node::_collect_stat_time = 0;
bool Node::_collect_stat = false;

Node::Node(IPAddress i) : _ip(i), _alive(true), _token(1) 
{
}

Node::~Node()
{
}

Node *
Node::getpeer(IPAddress a)
{
  return Network::Instance()->getnode(a);
}

unsigned
Node::rcvRPC(RPCSet *hset, bool &ok)
{
  int na = hset->size() + 1;
  Alt *a = (Alt *) malloc(sizeof(Alt) * na); // might be big, take off stack!
  Packet *p;
  unsigned *index2token = (unsigned*) malloc(sizeof(unsigned) * hset->size());

  int i = 0;
  for(RPCSet::const_iterator j = hset->begin(); j != hset->end(); j++) {
    assert(_rpcmap[*j]);
    a[i].c = _rpcmap[*j]->channel();
    a[i].v = &p;
    a[i].op = CHANRCV;
    index2token[i] = *j;
    i++;
  }
  a[i].op = CHANEND;

  if((i = alt(a)) < 0) {
    cerr << "interrupted" << endl;
    assert(false);
  }
  assert(i < (int) hset->size());

  unsigned token = index2token[i];
  assert(token);
  hset->erase(token);
  _deleteRPC(token);
  ok = p->ok();
  delete p;
  free(a);
  free(index2token);
  return token;
}


void
Node::_deleteRPC(unsigned token)
{
  assert(_rpcmap[token]);
  delete _rpcmap[token];
  _rpcmap.remove(token);
}


void
Node::parse(char *filename)
{
  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    threadexitsall(0);
  }

  string line;

  hash_map<string, Args> xmap;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // read protocol string
    _protocol = words[0];
    words.erase(words.begin());

    // if it has no arguments, you still need to register the prototype
    // if(!words.size())
      // xmap[protocol];

    // this is a variable assignment
    while(words.size()) {
      vector<string> xargs = split(words[0], "=");
      words.erase(words.begin());
      if (_args.find (xargs[0]) == _args.end())
	_args.insert(make_pair(xargs[0], xargs[1]));
    }

    break;
  }

  in.close();
}

bool
Node::collect_stat()
{
  if (_collect_stat) 
    return true;
  if (now() >= _collect_stat_time) {
    _collect_stat = true;
    return true;
  }else
    return false;
}


// Called by NetEvent::execute() to deliver a packet to a Node,
// after Network processing (i.e. delays and failures).
void
Node::packet_handler(Packet *p)
{
  if(p->reply()){
    // RPC reply, give to waiting thread.
    send(p->channel(), &p);
  } else {
    // RPC request, start a handler thread.
    ThreadManager::Instance()->create(Node::Receive, p);
  }
}

//
// Send off a request packet asking Node::Receive to
// call fn(args), wait for reply.
// Return value indicates whether we received a reply,
// i.e. absence of time-out.
//
bool
Node::_doRPC(IPAddress dst, void (*fn)(void *), void *args)
{
  return _doRPC_receive(_doRPC_send(dst, fn, 0, args));
}


RPCHandle*
Node::_doRPC_send(IPAddress dst, void (*fn)(void *), void (*killme)(void *), void *args)
{
  Packet *p = New Packet;
  p->_fn = fn;
  p->_killme = killme;
  p->_args = args;
  p->_src = ip();
  p->_dst = dst;

  // where to send the reply, buffered for single reply
  Channel *c = p->_c = chancreate(sizeof(Packet*), 1);

  Network::Instance()->send(p);

  return New RPCHandle(c, p);
}


bool
Node::_doRPC_receive(RPCHandle *rpch)
{
  Packet *reply = (Packet *) recvp(rpch->channel());
  bool ok = reply->_ok;
  delete reply;
  delete rpch;
  return ok;
}

//
// Node::got_packet() invokes Receive() when an RPC request arrives.
// The reply goes back directly to the appropriate channel.
//
void
Node::Receive(void *px)
{
  Packet *p = (Packet *) px;
  Node *proto = Network::Instance()->getnode(p->dst());
  assert(proto);

  // make reply
  Packet *reply = New Packet;
  reply->_c = p->_c;
  reply->_src = p->_dst;
  reply->_dst = p->_src;

  if (proto->alive ()) {
    (p->_fn)(p->_args);
    reply->_ok = true;
  } else {
    reply->_ok = false;  // XXX delete reply for timeout?
  }

  // send it back, potentially with a latency punishment for when this node was
  // dead.
  Network::Instance()->send(reply);

  // ...and we're done
  threadexits(0);
}

#include "bighashmap.cc"
