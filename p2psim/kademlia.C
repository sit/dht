#include "p2psim.h"
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
Kademlia::delayedcb(void*)
{
  cout << "Invoked at: " << now() << endl;
}

void *
Kademlia::do_join(void*)
{
  cout << "do_join at " << now() << endl;
  delaycb(10, Kademlia::delayedcb, 0);
  return 0;
}

void
Kademlia::join(void*)
{
  // send my id to next node in ring
  doRPC((IPAddress) ((id()+1) % 5), Kademlia::do_join, 0);
}

void
Kademlia::leave(void*)
{
  cout << "Kademlia leave" << endl;
}

void
Kademlia::crash(void*)
{
  cout << "Kademlia crash" << endl;
}

void
Kademlia::insert_doc(void*)
{
  cout << "Kademlia insert_doc" << endl;
}

void
Kademlia::lookup_doc(void*)
{
  cout << "Kademlia lookup_doc" << endl;
}
