#ifndef __PACKET_H
#define __PACKET_H

#include <lib9.h>
#include <thread.h>
#include "p2psim.h"

class Packet {
public:
  Packet(Channel*, void (*)(void*), void *, IPAddress, IPAddress);
  ~Packet();

  //  unsigned size;
  void (*(fn()))(void*) { return _fn; }
  void *args() { return _args; }
  IPAddress src() { return _src; }
  IPAddress dst() { return _dst; }
  Channel *channel() { return _c; }
  bool reply() { return _fn == 0; }

private:
  Channel *_c;            // where to send the reply
  void (*_fn)(void*);     // function to invoke
  void *_args;            // caller-supplied argument

  IPAddress _src;
  IPAddress _dst;
};

#endif // __PACKET_H
