#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "protocolfactory.h"
#include <string>
#include "event.h"
#include "eventqueue.h"
#include "p2psim.h"
using namespace std;

class Node;

// A Protocol is just a named object attached to a Node.
// The point is, for example, to help the Chord object on
// one node find the Chord on another node by calling
// getpeer(IPAddress). DHTProtocol has the DHT-specific
// abstract methods.
class Protocol {
public:
  Protocol(Node*);
  virtual ~Protocol();
  Node *node() { return _node; }
  virtual string proto_name() = 0;

protected:

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

  // Send an RPC from a Protocol on one Node to a method
  // of the same Protocol sub-class on a different Node.
  template<class BT, class AT, class RT>
    bool doRPC(IPAddress dst, void (BT::* fn)(AT *, RT *),
               AT *args, RT *ret) {
    return _node->doRPC(dst,
                        dynamic_cast<BT*>(getpeer(dst)),
                        fn, args, ret);
  }

private:
  Node *_node;
};

#endif // __PROTOCOL_H
