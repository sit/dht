#include "packet.h"
#include <lib9.h>
#include <thread.h>

#include "p2psim.h"

Packet::Packet() : _proto(string("")), _fn(0), _args(0), _c(0), _src(0),
		   _dst(0), _ok (true)
{
}

Packet::~Packet()
{
}
