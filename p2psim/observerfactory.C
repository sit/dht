#include "observerfactory.h"
#include "chordobserver.h"
#include "kademliaobserver.h"
#include "args.h"
#include "p2psim.h"
using namespace std;

Observer *
ObserverFactory::create(string s, Args *a)
{
  Observer *t = 0;

  if ((s == "Chord") || (s == "ChordFinger") 
      || (s == "ChordFingerPNS") || (s == "ChordToe") 
      || (s == "Koorde")) {
    t = ChordObserver::Instance(a);
  }
  if(s == "Kademlia") {
    t = KademliaObserver::Instance(a);
  }

  return t;
}
