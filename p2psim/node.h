#ifndef __NODE_H
#define __NODE_H

#include "threaded.h"
#include <lib9.h>
#include <thread.h>
#include <map>
#include <string>
#include "p2psim.h"
#include "packet.h"
using namespace std;

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
    bool doRPC(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
               AT *args, RT *ret) {
    // target is probably the result of a dynamic_cast<BT*>...
    assert(target);

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
    t->_target = target;
    t->_fn = fn;
    t->_args = args;
    t->_ret = ret;

    bool ok = _doRPC(dst, Thunk::thunk, (void *) t);

    delete t;
    return ok;
  }

  Channel *pktchan() { return _pktchan; }
  Channel *protchan() { return _protchan; }
  void crash () { _alive = false; }
  bool alive () { return _alive; }

private:
  virtual void run();
  static void Receive(void*);

  IPAddress _ip;        // my ip address
  bool _alive;
  Channel *_pktchan;    // for packets
  Channel *_protchan;   // to register protocols

  typedef map<string,Protocol*> PM;
  typedef PM::const_iterator PMCI;
  PM _protmap;
};

#endif // __NODE_H
