#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "threaded.h"
#include "protocolfactory.h"
#include "args.h"
#include "network.h"
#include <string>
#include <map>
#include <typeinfo>
#include "p2psim.h"
using namespace std;

class Node;
class P2PEvent;
class Protocol;

class Protocol : public Threaded {
public:
  typedef void (Protocol::*member_f)(void*, void *);
  typedef enum {
    JOIN = 0,
    LEAVE,
    CRASH,
    INSERT,
    LOOKUP,
  } EventID;

  Protocol(Node*);
  virtual ~Protocol();
  Node *node() { return _node; }
  Channel *appchan() { return _appchan; }
  Channel *netchan() { return _netchan; }

  virtual void join(Args*) = 0;
  virtual void leave(Args*) = 0;
  virtual void crash(Args*) = 0;
  virtual void insert(Args*) = 0;
  virtual void lookup(Args*) = 0;


protected:

#define delaycb(X, Y, Z) this->_delaycb(X, ((member_f)(&Y)), ((void*) (Z)))
  void _delaycb(Time, member_f, void*);
  IPAddress ip();

#define doRPC(DST, FN, ARGS, RET) this->_doRPC(DST, ((member_f)(FN)), \
      ((void*) (ARGS)), ((void*) (RET)))
  bool _doRPC(IPAddress, member_f, void*, void*);

private:
  Node *_node;
  Channel *_appchan; // to receive calls from applications
  Channel *_netchan; // to receive packets from network

  static void Dispatch(void*);
  void dispatch(P2PEvent*);
  void run();
};

#endif // __PROTOCOL_H
