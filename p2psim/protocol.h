#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "threaded.h"
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

class Protocol : public Threaded {
public:
  typedef void (Protocol::*member_f)(void*, void *);

  Protocol(Node*);
  virtual ~Protocol();
  Node *node() { return _node; }
  Channel *appchan() { return _appchan; }
  Channel *netchan() { return _netchan; }

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
    // Is BT the same as the calling class?
    assert(typeid(BT) == typeid(*this));

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
    e->_fn = fn;
    e->_args = args;

    send(EventQueue::Instance()->eventchan(), &e);
  }

  IPAddress ip();

#define doRPC(DST, FN, ARGS, RET) this->_doRPC(DST, ((member_f)(FN)), \
      ((void*) (ARGS)), ((void*) (RET)))
  bool _doRPC(IPAddress, member_f, void*, void*);

private:
  Node *_node;
  Channel *_appchan; // to receive calls from applications
  Channel *_netchan; // to receive packets from network

  void run();
};

#endif // __PROTOCOL_H
