#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "threaded.h"
#include "p2psim.h"
#include <string>
#include <map>
using namespace std;

class Node;
class P2PEvent;
class Protocol;

class Protocol : public Threaded {
public:
  typedef void (Protocol::*member_f)(void*, void *);
  typedef map<string,string> Args;
  typedef enum {
    JOIN = 0,
    LEAVE,
    CRASH,
    INSERT,
    LOOKUP,
  } EventID;

  Protocol(Node*);
  virtual ~Protocol();
  Channel *appchan() { return _appchan; }
  Channel *netchan() { return _netchan; }

  virtual void join(Args*) = 0;
  virtual void leave(Args*) = 0;
  virtual void crash(Args*) = 0;
  virtual void insert_doc(Args*) = 0;
  virtual void lookup_doc(Args*) = 0;

protected:
#define doRPC(X, Y, A, R) (this->_doRPC((X), ((member_f)(&Y)), \
                                        ((void*) (A)), ((void*) R)))
  bool _doRPC(IPAddress, member_f, void*, void*);

#define delaycb(X, Y, Z) this->_delaycb(X, ((member_f)(&Y)), ((void*) (Z)))
  void _delaycb(Time, member_f, void*);
  IPAddress ip();

private:
  Channel *_appchan; // to receive calls from applications
  static void Dispatch(void*);
  void dispatch(P2PEvent*);

  Channel *_netchan; // to receive packets from network
  static void Receive(void*);


  void run();
  Node *_node;
};

#endif // __PROTOCOL_H
