#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "threaded.h"
#include "p2psim.h"
#include <string>
using namespace std;

class Packet;
class P2PEvent;
class Node;


#define MEMBER_FUNC(X) (void*(Protocol::*)(void*))(&X)

class Protocol : public Threaded {
public:
  typedef unsigned msg_t;
  typedef void*    data_t;
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

  virtual void* join(void*) = 0;
  virtual void* leave(void*) = 0;
  virtual void* crash(void*) = 0;
  virtual void* insert_doc(void*) = 0;
  virtual void* lookup_doc(void*) = 0;

protected:
  void* doRPC(IPAddress, void* (Protocol::*)(void*));

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
