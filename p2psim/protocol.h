#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "protocolfactory.h"
#include "args.h"
#include "network.h"
#include "eventqueue.h"
#include <string>
#include <map>
#include <typeinfo>
#include "p2psim.h"
#include "event.h"
using namespace std;

class Node;
class P2PEvent;
class Protocol;

class Protocol {
public:
  Protocol(Node*);
  virtual ~Protocol();
  Node *node() { return _node; }
  string proto_name() {
    return ProtocolFactory::Instance()->name(this);
  }

  typedef void (Protocol::*event_f)(Args*);
  virtual void join(Args*) = 0;
  virtual void leave(Args*) = 0;
  virtual void crash(Args*) = 0;
  virtual void insert(Args*) = 0;
  virtual void lookup(Args*) = 0;

protected:

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
      BT *_target;
      void (BT::*_fn)(AT);
      AT _args;
    private:
      void execute() {
        (_target->*_fn)(_args);
      };
    };

    XEvent *e = new XEvent;
    e->ts = now() + d;
    e->_target = dynamic_cast<BT*>(this);
    assert(e->_target);
    e->_fn = fn;
    e->_args = args;

    send(EventQueue::Instance()->eventchan(), &e);
  }

  IPAddress ip() { return _node->ip(); }

  // Send an RPC from a Protocol on one Node to a method
  // of the same Protocol class on a different Node.
  template<class BT, class AT, class RT>
    bool doRPC(IPAddress dst, void (BT::* fn)(AT *, RT *),
               AT *args, RT *ret) {
    // Compile-time check: does BT inherit from Protocol?
    Protocol *dummy1 = (BT *) 0; dummy1 = dummy1;

    class Thunk {
    public:
      BT *_target;
      void (BT::*_fn)(AT *, RT *);
      AT *_args;
      RT *_ret;
      static void thunk(void *xa) {
        Thunk *t = (Thunk *) xa;
        (t->_target->*(t->_fn))(t->_args, t->_ret);
      }
    };

    Thunk *t = new Thunk;
    t->_target = dynamic_cast<BT*>
      (Network::Instance()->getnode(dst)->getproto(proto_name()));
    t->_fn = fn;
    t->_args = args;
    t->_ret = ret;

    // Will fail if it's not legal to call BT::fn
    // in the target Protocol sub-class. I.e. if BT is not
    // the same as the calling Protocol sub-class, or
    // a super-class. We want to let Koorde call Chord methods,
    // but we want to prevent Koorde from calling a Kademlia
    // method on a Koorde Protocol.
    assert(t->_target);

    bool ok = _node->_doRPC(dst, Thunk::thunk, (void *) t);

    delete t;
    return ok;
  }

private:
  Node *_node;
};

#endif // __PROTOCOL_H
