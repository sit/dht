#include "kademlia.h"
#include "packet.h"
#include "nodefactory.h"
#include <iostream>
#include "p2psim.h"
using namespace std;


#define DEBUG(x) if(verbose >= (x)) cout


Kademlia::Kademlia(Node *n) : Protocol(n)
{
  _id = (((NodeID) random()) << 32) | random();
}

Kademlia::~Kademlia()
{
}


void
Kademlia::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
  if(wkn == ip())
    return;
}


void
Kademlia::handle_join(void *args, void *result)
{
  // join_args *jargs = (join_args*) args;
  join_result *jresult = (join_result*) result;

  jresult->ok = 1;
}


void
Kademlia::handle_join(NodeID id, IPAddress ip)
{
  for(unsigned i=0; i<idsize; i++) {
    NodeID newdist = distance(id, _id ^ (1<<i));
    NodeID curdist = distance(_fingers.get_id(i), _id ^ (1<<i));
    if(newdist < curdist) {
      cout << "handle_join " << printbits(id) << " is better than old " << printbits(_fingers.get_id(i)) << " for entry " << i << "\n";
      _fingers.set(i, id, ip);
    }
  }
}

string
Kademlia::printbits(NodeID id)
{
  string s;
  for(int i=idsize-1; i>=0; i--)
    s += ((id >> i) & 0x1);
  s += " (";
  s += id;
  s += ")";
  return s;
}



Kademlia::NodeID
Kademlia::distance(Kademlia::NodeID from, Kademlia::NodeID to)
{
  DEBUG(5) << "distance between " << printbits(from) << " and " << printbits(to) << " = ";
  NodeID ret;

  ret = from ^ to;

  DEBUG(5) << ret << "\n";
  return ret;
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
