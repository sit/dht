#include <lib9.h>
#include "rpchandle.h"

RPCHandle::RPCHandle(Channel *c, Packet *p)
  : _c(c), _p(p)
{
}

RPCHandle::~RPCHandle()
{
  chanfree(_c);
  delete _p;
}
