#ifndef __VIVALDI_H
#define __VIVALDI_H

#include <map>
#include "p2psim.h"
#include "protocol.h"
#include "network.h"
#include "protocolfactory.h"
using namespace std;

// Compute Vivaldi synthetic coordinates.
// Protocol-independent: doesn't care where the measurements
// come from.
// Indexed by IPAddress.
// Anyone can create a Vivaldi, call sample() after each
// RPC, and then call my_location().
// Or you can use Vivaldi::RPC.

class Vivaldi {
 public:
  Vivaldi(Node *n);
  virtual ~Vivaldi();

  struct Coord {
    double _x;
    double _y;
  };

  void sample(IPAddress who, Coord c, unsigned latency);
  Coord my_location() { return _c; }

  // Anyone can use this to make an RPC and have Vivaldi time it.
  template<class BT, class AT, class RT>
    bool RPC(IPAddress a, void (BT::* fn)(AT *, RT *),
             AT *args, RT *ret);

 private:
  Node *_n; // this node
  Coord _c;
  int _nsamples;
};

#if 0
// Make an RPC call, but time it and tell Vivaldi.
// Basically wraps the RPC in an RPC to rpc_handler.
// Use this only for simple RPCs: don't use it for
// recursive RPCs.
template<class BT, class AT, class RT>
bool Vivaldi::RPC(IPAddress dsta,
                  void (BT::* fn)(AT *, RT *),
                  AT *args,
                  RT *ret)
{
  Vivaldi *vtarget = find(dsta);
  assert(vtarget);

  // find target node from IP address.
  Node *dstnode = Network::Instance()->getnode(dsta);
  assert(dstnode && dstnode->ip() == dsta);

  // find target protocol from class name.
  Protocol *dstproto = dstnode->getproto(typeid(BT));
  BT *target = dynamic_cast<BT*>(dstproto);
  assert(target);

  struct rpc_glop {
    BT *_target;
    Vivaldi *_vtarget;
    void (BT::* _fn)(AT *, RT *);
    AT *_args;
    RT *_ret;
    Coord _c;
    static void thunk(void *xg){
      rpc_glop *g = (rpc_glop*) xg;
      (g->_target->*(g->_fn))(g->_args, g->_ret);
      g->_c = g->_vtarget->my_location();
    }
  };

  rpc_glop *gp = new rpc_glop;
  gp->_target = target;
  gp->_vtarget = vtarget;
  gp->_fn = fn;
  gp->_args = args;
  gp->_ret = ret;

  Time before = now();

  bool ok = Node::_doRPC(dsta, rpc_glop::thunk, (void*) gp);

  if(ok){
    Time after = now();
    sample(dsta, gp->_c, after - before);
  }

  delete gp;

  return ok;
}
#endif

#endif
