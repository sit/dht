#ifndef __NODE_H
#define __NODE_H

#include "threaded.h"
#include "protocol.h"
#include <lib9.h>
#include <thread.h>
#include <map>
#include <string>
#include "p2psim.h"
#include "packet.h"
using namespace std;

class Node : public Threaded {
public:
  Node(IPAddress);
  virtual ~Node();

  IPAddress ip() { return _ip; }
  Channel *pktchan() { return _pktchan; }
  Channel *protchan() { return _protchan; }
  Protocol *getproto(string p) { return _protmap[p]; }
  static Node *thread2node(int tid) {
    if(_threads.find(tid) != _threads.end())
      return _threads[tid];
    else
      return 0;
  }
  static void SetThread(int tid, Node *n) { _threads[tid] = n; }

  static bool _doRPC(IPAddress srca, IPAddress dsta,
                     void (*)(void*), void*);

private:
  virtual void run();
  static void Receive(Packet *);

  IPAddress _ip;
  Channel *_pktchan;    // for packets
  Channel *_protchan;   // to register protocols

  typedef map<string,Protocol*> PM;
  typedef PM::const_iterator PMCI;
  PM _protmap;
  static map<int,Node*> _threads; // map thread ID to Node
};

#endif // __NODE_H
