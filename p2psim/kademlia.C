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
  IPAddress wkn = (IPAddress) atoi(((*a)["wellknown"]).c_str());

  IPAddress myip = ip();
  doRPC(wkn, Kademlia::do_join, &myip);
}

void
Kademlia::do_join(void *ip)
{
  cout << "Node with ip=" << *((IPAddress*)ip) << " just registed." << endl;
}

void
Kademlia::join(Args *a)
{
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
Kademlia::insert_doc(Args*)
{
  cout << "Kademlia insert_doc" << endl;
}

void
Kademlia::lookup_doc(Args*)
{
  cout << "Kademlia lookup_doc" << endl;
}
