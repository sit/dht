#include "kademlia.h"
#include "packet.h"
#include <iostream>
#include "p2psim.h"
using namespace std;

Kademlia::Kademlia(Node *n) : Protocol(n)
{
}

Kademlia::~Kademlia()
{
}

void
Kademlia::join_kademlia(void *x)
{
  Args *a = (Args*) x;
  IPAddress wkn = a->nget<IPAddress>("wellknown");

  IPAddress myip = ip();

  int reply = 0;
  doRPC(wkn, &Kademlia::do_join, &myip, &reply);
  cout << "reply is now " << reply << endl;
}

void
Kademlia::do_join(IPAddress *ip, void *reply)
{
  cout << "Kademlia node with ip=" << *ip << " just registered." << endl;
  *((int *) reply) = 2206;
}

void
Kademlia::join(Args *a)
{
  cout << "Node " << ip() << " doing a 10ms wait" << endl;
  delaycb(10, Kademlia::join_kademlia, a);
}

void
Kademlia::leave(Args*)
{
  cout << "Kademlia leave" << endl;
}

void
Kademlia::crash(Args*)
{
  cout << "Kademlia crash" << endl;
}

void
Kademlia::insert(Args*)
{
  cout << "Kademlia insert" << endl;
}

void
Kademlia::lookup(Args *args)
{
  cout << "Kademlia lookup" << endl;
}
