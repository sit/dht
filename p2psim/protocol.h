#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "threaded.h"
#include "p2psim.h"
#include <string>
using namespace std;

class Packet;
class P2PEvent;
class Node;


#define doRPC(X, Y, Z) _doRPC((X), ((member_f)(&Y)), ((void*) (Z)))
#define delaycb(X, Y, Z) _delaycb(X, ((member_f)(&Y)), ((void*) (Z)))

class Protocol;
class Protocol : public Threaded {
public:
  typedef void (Protocol::*member_f)(void*);
  typedef enum {
    JOIN = 0,
    LEAVE,
    CRASH,
    INSERT,
    LOOKUP,
  } EventID;

  Protocol(Node*);
  ~Protocol();
  Channel *appchan() { return _appchan; }
  Channel *netchan() { return _netchan; }

  virtual void join(void*) = 0;
  virtual void leave(void*) = 0;
  virtual void crash(void*) = 0;
  virtual void insert_doc(void*) = 0;
  virtual void lookup_doc(void*) = 0;

protected:
  void _doRPC(IPAddress, member_f, void*);
  void _delaycb(Time, member_f, void*);
  IPAddress id();

private:
  Channel *_appchan; // to receive calls from applications
  Channel *_netchan; // to receive messages from network

  // for application calls
  struct appdispatch_t {
    Protocol *p;
    P2PEvent *e;
  };
  static void Dispatch(void*);
  void dispatch(P2PEvent*);

  // for network packets
  struct netdispatch_t {
    Protocol *p;
    Packet *packet;
  };

  static void Receive(void*);



  void run();
  Node *_node;
};

#endif // __PROTOCOL_H
