#include "packet.h"
#include <lib9.h>
#include <thread.h>


Packet::Packet() : size(0), _c(0), _protocol(""), _fn(0), _args(0), 
                  _src(0), _dst(0)
{
}

Packet::~Packet()
{
  chanfree(_c);
}
