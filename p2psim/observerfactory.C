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

  if(s == "ChordObserver") {
    t = ChordObserver::Instance(a);
  }
  if(s == "KademliaObserver") {
    t = KademliaObserver::Instance(a);
  }

  return t;
}
