#ifndef __OBSERVED_H
#define __OBSERVED_H

#include "observer.h"
#include <set>
using namespace std;

class Observed { public:
  void registerObserver(Observer *);
  void unregisterObserver(Observer *);
  void notifyObservers();

protected:
  Observed();

private:
  set<Observer*> _observers;
};


#endif // __OBSERVED_H
