/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu)
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

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "eventqueue.h"
#include "observed.h"
#include <stdio.h>
class Node;

// A Protocol is just a named object attached to a Node.
// The point is, for example, to help the Chord object on
// one node find the Chord on another node by calling
// getpeer(IPAddress). P2Protocol has the DHT-specific
// abstract methods.
class Protocol : public Observed {
public:
  Protocol(Node*);
  virtual ~Protocol();
  Node *node() { return _node; }
  virtual string proto_name() = 0;
  static void Protocol::parse(char*);

protected:
  typedef set<unsigned> RPCSet;

  IPAddress ip() { return _node->ip(); }

  // find peer protocol of my sub-type on a distant node.
  Protocol *getpeer(IPAddress);

  // Why are we forbidding non-Protocols from using delaycb()?
  // Use of a template allows us to type-check the argument
  // to fn(), and to check fn() is a member
  // of the same sub-class of Protocol as the caller.
  template<class BT, class AT>
    void delaycb(int d, void (BT::*fn)(AT), AT args) {
    // Compile-time check: does BT inherit from Protocol?
    Protocol *dummy = (BT *) 0; dummy = dummy;

    class XEvent : public Event {
    public:
      XEvent() : Event( "XEvent" ) {};
      BT *_target;
      void (BT::*_fn)(AT);
      AT _args;
    private:
      void execute() {
        (_target->*_fn)(_args);
      };
    };

    XEvent *e = New XEvent;
    e->ts = now() + d;
    e->_target = dynamic_cast<BT*>(this);
    assert(e->_target);
    e->_fn = fn;
    e->_args = args;

    EventQueue::Instance()->add_event(e);
  }

  // Send an RPC from a Protocol on one Node to a method
  // of the same Protocol sub-class on a different Node.
  template<class BT, class AT, class RT>
    bool doRPC(IPAddress dst, void (BT::* fn)(AT *, RT *),
               AT *args, RT *ret) {
    assert(dst > 0);
    return _node->doRPC(dst,
                        dynamic_cast<BT*>(getpeer(dst)),
                        fn, args, ret);
  }

  // Send an RPC from a Protocol on one Node to a method
  // of the same Protocol sub-class on a different Node.
  // This one is asynchronous
  template<class BT, class AT, class RT>
  unsigned asyncRPC(IPAddress dst,
      void (BT::* fn)(AT *, RT *), AT *args, RT *ret, unsigned token = 0)
  {
    assert(dst);
    if(token)
      assert(_rpcmap.find(token) == _rpcmap.end());
    else
      while(!token || _rpcmap.find(token) != _rpcmap.end())
        token = _token++;

    RPCHandle *rpch = _node->asyncRPC(dst, dynamic_cast<BT*>(getpeer(dst)),
        fn, args, ret);
    if(!rpch)
      return 0;

    _rpcmap[token] = rpch;
    return token;
  }

  // returns one of the RPCHandle's for which a reply has arrived. BLOCKING.
  unsigned rcvRPC(RPCSet*, bool&);

  // returns whether rcvRPC can be called without blocking.
  bool select(RPCSet*);

  void cancelRPC(unsigned);


private:
  Node *_node;
  hash_map<unsigned, RPCHandle*> _rpcmap;
  unsigned _token;
};

#endif // __PROTOCOL_H
