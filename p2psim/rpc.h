#ifndef _RPC_H
#define _RPC_H

#include <typeinfo>
#include "threaded.h"
#include "p2psim.h"
#include <string>
#include <map>
using namespace std;
#include "protocol.h"
#include "protocolfactory.h"
#include "network.h"

// Anyone can call this doRPC, not just a Protocol.
// But the called function must inherit from Protocol.
// And this implementation correctly delivers to the
// Protocol of the callee rather than the caller.
template<class BT,
  class AT,
  class RT> bool doRPC(IPAddress dsta,
                       void (BT::* fn)(AT *, RT *),
                       AT *args,
                       RT *ret) {

  // find target node from IP address.
  Node *dstnode = Network::Instance()->getnode(dsta);
  assert(dstnode && dstnode->ip() == dsta);

  // find target protocol from class name.
  string dstprotname = ProtocolFactory::Instance()->name(typeid(BT));
  Protocol *dstproto = dstnode->getproto(dstprotname);
  assert(dstproto);

  // find source IP address.
  extern int rtm_thread_id();
  int tid = rtm_thread_id();
  assert(tid != -1);
  Node *srcnode = Node::thread2node(tid);
  assert(srcnode);
  IPAddress srca = srcnode->ip();

  struct rpc_glop {
    BT *_proto;
    void (BT::*_fn)(AT *, RT*);
    AT *_args;
    RT *_ret;
    static void thunk(void *xg) {
      rpc_glop *g = (rpc_glop*) xg;
      (g->_proto->*(g->_fn))(g->_args, g->_ret);
      delete g;
    }
  };

  rpc_glop *gp = new rpc_glop;
  gp->_proto = dynamic_cast<BT*>(dstproto);
  gp->_fn = fn;
  gp->_args = args;
  gp->_ret = ret;

  bool ok = Node::_doRPC(srca, dsta, rpc_glop::thunk, (void*) gp);

  return ok;
}

#endif
