#ifndef __RPC_H
#define __RPC_H

#include "threaded.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "network.h"
#include "threadmanager.h"
#include <typeinfo>
#include <string>
#include <map>
#include "p2psim.h"
using namespace std;

// This version of RPC is not tied to Protocols.
// You pass it the object pointer and method you
// want to call, and the node to which you want the network
// to simulate latency.
template<class BT, class AT, class RT>
bool doRPC(IPAddress dsta,
           BT *target,
           void (BT::* fn)(AT *, RT *),
           AT *args,
           RT *ret)
{
  // find target node from IP address.
  Node *dstnode = Network::Instance()->getnode(dsta);
  assert(dstnode && dstnode->ip() == dsta);

  struct rpc_glop {
    BT *_target;
    void (BT::*_fn)(AT *, RT*);
    AT *_args;
    RT *_ret;
    static void thunk(void *xg) {
      rpc_glop *g = (rpc_glop*) xg;
      (g->_target->*(g->_fn))(g->_args, g->_ret);
      delete g;
    }
  };

  rpc_glop *gp = new rpc_glop;
  gp->_target = target;
  gp->_fn = fn;
  gp->_args = args;
  gp->_ret = ret;

  bool ok = Node::_doRPC(dsta, rpc_glop::thunk, (void*) gp);

  return ok;
}

#endif // __RPC_H
