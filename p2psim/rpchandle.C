#include <lib9.h>
#include <thread.h>
#include "rpchandle.h"
#include "packet.h"
#include "thing.h"

RPCHandle::RPCHandle(Channel *c, Packet *p, Thing *t)
  : _c(c), _p(p), _t(t)
{
}

RPCHandle::~RPCHandle()
{
  chanfree(_c);
  delete _p;
  if(_t)
    delete _t;
}
