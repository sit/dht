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
#include "rpchandle.h"
#include "observed.h"
#include "p2psim/args.h"
#include <assert.h>
#include <stdio.h>

// A Node is the superclass of
// The point is, for example, to help the Chord object on
// one node find the Chord on another node by calling
// getpeer(IPAddress). P2Node has the DHT-specific
// abstract methods.
class Node : public Observed {
public:
  Node(IPAddress);
  virtual ~Node();
  static void Node::parse(char*);
  virtual void initstate() {};

  IPAddress ip() { return _ip; }
  void set_alive(bool a) { _alive = a;}
  bool alive () { return _alive; }
  void packet_handler(Packet *);
  static void Receive(void*);

  // the One Node that we're running and its arguments
  static string protocol() { return _protocol; }
  static Args args() { return _args; }

protected:
  typedef set<unsigned> RPCSet;

  // find peer protocol of my sub-type on a distant node.
  Node *getpeer(IPAddress);

  // Why are we forbidding non-Nodes from using delaycb()?
  // Use of a template allows us to type-check the argument
  // to fn(), and to check fn() is a member
  // of the same sub-class of Node as the caller.
  template<class BT, class AT>
    void delaycb(int d, void (BT::*fn)(AT), AT args) {
    // Compile-time check: does BT inherit from Node?
    Node *dummy = (BT *) 0; dummy = dummy;

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


  // Send an RPC from a Node on one Node to a method
  // of the same Node sub-class with a different ip.
  template<class BT, class AT, class RT>
  bool doRPC(IPAddress dst, void (BT::* fn)(AT *, RT *), AT *args, RT *ret)
  {
    assert(dst > 0);
    Thunk<BT, AT, RT> *t = _makeThunk(dst, dynamic_cast<BT*>(getpeer(dst)), fn, args, ret);
    bool ok = _doRPC(dst, Thunk<BT, AT, RT>::thunk, (void *) t);
    delete t;
    return ok;
  }

  // Same as doRPC, but this one is asynchronous
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

    Thunk<BT, AT, RT> *t = _makeThunk(dst, dynamic_cast<BT*>(getpeer(dst)), fn, args, ret);
    RPCHandle *rpch = _doRPC_send(dst, Thunk<BT, AT, RT>::thunk, Thunk<BT, AT, RT>::killme, (void *) t);

    if(!rpch)
      return 0;

    _rpcmap[token] = rpch;
    return token;
  }

  // returns one of the RPCHandle's for which a reply has arrived. BLOCKING.
  unsigned rcvRPC(RPCSet*, bool&);

private:
  IPAddress _ip;
  bool _alive;

  hash_map<unsigned, RPCHandle*> _rpcmap;
  unsigned _token;
  void _deleteRPC(unsigned);


  //
  // RPC machinery
  //
  template<class BT, class AT, class RT>
  class Thunk {
  public:
    BT *_target;
    void (BT::*_fn)(AT *, RT *);
    AT *_args;
    RT *_ret;
    static void thunk(void *xa) {
      Thunk *t = (Thunk *) xa;
      (t->_target->*(t->_fn))(t->_args, t->_ret);
      t->_target->notifyObservers();
    }

    static void killme(void *xa) {
      delete (Thunk*) xa;
    }
  };

  // implements _doRPC
  friend class Vivaldi;
  bool _doRPC(IPAddress, void (*fn)(void *), void *args);
  RPCHandle* _doRPC_send(IPAddress, void (*)(void *), void (*)(void*), void *);
  bool _doRPC_receive(RPCHandle*);

  // creates a Thunk object with the necessary croft for an RPC
  template<class BT, class AT, class RT>
  Node::Thunk<BT, AT, RT> *
  Node::_makeThunk(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
      AT *args, RT *ret)
  {
    // target is probably the result of a dynamic_cast<BT*>...
    assert(target);

    Thunk<BT, AT, RT>  *t = New Thunk<BT, AT, RT>;
    t->_target = target;
    t->_fn = fn;
    t->_args = args;
    t->_ret = ret;

    return t;
  }

  // The One Protocol and Its Arguments
  static string _protocol;
  static Args _args;
};

#endif // __PROTOCOL_H
