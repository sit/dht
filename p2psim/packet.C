#include "packet.h"

unsigned Packet::_unique = 0;

Packet::Packet() : _fn(0), _killme(0), _args(0), _c(0), _src(0),
		   _dst(0), _ok (true)
{
  _id = _unique++;
}

Packet::~Packet()
{
  if(_killme)
    (_killme)(_args);
}
