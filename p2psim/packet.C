#include "packet.h"
#include <lib9.h>
#include <thread.h>


Packet::Packet() : size(0), type(0), data(0), _protocol(""),
  _src(0), _dst(0), _reply(false)
{
}

Packet::~Packet()
{
  chanfree(_c);
}
