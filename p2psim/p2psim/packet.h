#ifndef __PACKET_H
#define __PACKET_H

#include <lib9.h>
#include <thread.h>
#include "p2psim.h"
using namespace std;

// The only thing Packet is useful for is to send RPCs,
// as managed by Node::_doRPC() and Node::Receive().
class Packet {
public:
  Packet();
  ~Packet();

  IPAddress src() { return _src; }
  IPAddress dst() { return _dst; }
  Channel *channel() { return _c; }
  bool reply() { return _fn == 0; }
  unsigned id() { return _id; }
  bool ok()     { return _ok; }

  void touch()    { _touchtime = now(); }
  Time touched()  { return _touchtime; }

  void punish()   { _punished = true; }
  bool punished() { return _punished; }

private:
  // RPC function and arguments.
  // No explicit reply; put it in the args.
  void (*_fn)(void *);
  void (*_killme)(void *);
  void *_args;

  Time _touchtime;        // last time packet was touch()-ed
  bool _punished;         // whether packet has been punished by
                          // failure model

  friend class Node;
  Channel *_c;            // where to send the reply
  IPAddress _src;
  IPAddress _dst;
  bool _ok;               // was the target node available?
  unsigned _id;
  static unsigned _unique;
};

#endif // __PACKET_H
