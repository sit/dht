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
 */

#ifndef __NODE_H
#define __NODE_H

#include "threaded.h"
#include "rpchandle.h"
#include "observed.h"
#include "p2psim_hashmap.h"

class Protocol;

class Node : public Threaded {
public:
  Node(IPAddress);
  virtual ~Node();

  IPAddress ip() { return _ip; }
  void register_proto(Protocol *);
  Protocol *getproto(string p) { return _protmap[p]; }
  bool _doRPC(IPAddress, void (*fn)(void *), void *args);


  // This doRPC is not tied to Protocols, but it does require you
  // to specify the target object on the remote node.
  template<class BT, class AT, class RT>
  bool Node::doRPC(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
      AT *args, RT *ret)
  {
    Thunk<BT, AT, RT> *t = _makeThunk(dst, target, fn, args, ret);
    bool ok = _doRPC(dst, Thunk<BT, AT, RT>::thunk, (void *) t);
    delete t;
    return ok;
  }
  
  // This doRPC is not tied to Protocols, but it does require you
  // to specify the target object on the remote node.
  // It's asynchronous
  template<class BT, class AT, class RT>
  RPCHandle* asyncRPC(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
      AT *args, RT *ret)
  {
    Thunk<BT, AT, RT> *t = _makeThunk(dst, target, fn, args, ret);
    RPCHandle *rpch = _doRPC_send(dst, Thunk<BT, AT, RT>::thunk, Thunk<BT, AT, RT>::killme, (void *) t);
    return rpch;
  }


  Channel *pktchan() { return _pktchan; }
  void crash () { _alive = false; }
  bool alive () { return _alive; }
  void set_alive() { _alive = true;}
  void got_packet(Packet *);
  static void Receive(void*);

private:

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



  virtual void run();

  IPAddress _ip;        // my ip address
  bool _alive;
  Channel *_pktchan;    // for packets

  typedef hash_map<string, Protocol*> PM;
  typedef PM::const_iterator PMCI;
  PM _protmap;
};

#endif // __NODE_H
