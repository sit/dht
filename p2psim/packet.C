#include "packet.h"
#include <lib9.h>
#include <thread.h>

#include "p2psim.h"


Packet::Packet() : size(0), _c(0), _protocol(""), _fn(0), _args(0), 
                  _ret(0), _src(0), _dst(0)
{
  _c = chancreate(sizeof(Packet*), 0);
  assert(_c);
}

Packet::~Packet()
{
  chanfree(_c);
}
