#ifndef __OBSERVED_H
#define __OBSERVED_H

#include "observer.h"
#include "observerinfo.h"
#include <set>
using namespace std;

class Observed {
public:
  void registerObserver(Observer *);
  void unregisterObserver(Observer *);
  void notifyObservers(ObserverInfo* = 0);

protected:
  Observed();

private:
  set<Observer*> _observers;
};


#endif // __OBSERVED_H
