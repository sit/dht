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
Kademlia::join_kademlia(void *x)
{
  Args *a = (Args*) x;
  IPAddress wkn = (IPAddress) a->uget("wellknown");

  IPAddress myip = ip();
  doRPC(wkn, Kademlia::do_join, &myip, (void*) 0);
}

void
Kademlia::do_join(void *ip, void *ret)
{
  cout << "Kademlia node with ip=" << *((IPAddress*)ip) << " just registered." << endl;
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
Kademlia::lookup(Args*)
{
  cout << "Kademlia lookup" << endl;
}
