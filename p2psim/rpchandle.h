#ifndef __RPCHANDLE_H
#define __RPCHANDLE_H

#include "packet.h"

class RPCHandle { public:
  RPCHandle(Channel*, Packet*);
  ~RPCHandle();

  Channel *channel() { return _c; }
  Packet *packet() { return _p; }

private:
  Channel* _c;
  Packet* _p;

};

#endif // __RPCHANDLE_H
