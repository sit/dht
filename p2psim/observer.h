#ifndef __OBSERVER_H
#define __OBSERVER_H

class Observed;
class ObserverInfo;

class Observer {
public:
  Observer() {}
  virtual ~Observer() {}
  virtual void kick(Observed *, ObserverInfo* = 0) = 0;
};


#endif // __OBSERVER_H
