#include "kademlia.h"
#include "packet.h"
#include <iostream>
using namespace std;

Kademlia::Kademlia(Node *n) : Protocol(n)
{
}

Kademlia::~Kademlia()
{
}

void
Kademlia::join()
{
  // send my id to next node in ring
  Packet *p = new Packet();
  p->data = 0;
  p->type = 0;
  Packet *ret = doRPC((NodeID) ((id()+1) % 5), p);
  cout << id() << " got its reply from " << ret->src() << endl;
}

void
Kademlia::leave()
{
  cout << "Kademlia leave" << endl;
}

void
Kademlia::crash()
{
  cout << "Kademlia crash" << endl;
}

void
Kademlia::insert_doc()
{
  cout << "Kademlia insert_doc" << endl;
}

void
Kademlia::lookup_doc()
{
  cout << "Kademlia lookup_doc" << endl;
}

void
Kademlia::stabilize()
{
  cout << "Kademlia stabilize" << endl;
}

Packet *
Kademlia::receive(Packet *p)
{
  cout << id() << " got a join message from " << p->src() << endl;
  return p;
}
