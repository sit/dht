#ifndef __RPCHANDLE_H
#define __RPCHANDLE_H

#include "packet.h"
#include "thing.h"

class RPCHandle { public:
  RPCHandle(Channel*, Packet*, Thing* = 0);
  ~RPCHandle();

  Channel *channel() { return _c; }
  Packet *packet() { return _p; }

private:
  Channel* _c;
  Packet* _p;
  Thing *_t; // this is actually a Thunk

};

#endif // __RPCHANDLE_H
