#include "packet.h"
#include <lib9.h>
#include <thread.h>

#include "p2psim.h"

Packet::Packet(Channel *c, void (*fn)(void*), void *args,
        IPAddress src, IPAddress dst) :
  _c(c), _fn(fn), _args(args), _src(src), _dst(dst)
{
}

Packet::~Packet()
{
}
