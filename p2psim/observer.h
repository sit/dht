#ifndef __OBSERVER_H
#define __OBSERVER_H

class Observed;

class Observer {
public:
  Observer() {}
  virtual ~Observer() {}
  virtual void kick(Observed *) = 0;
};


#endif // __OBSERVER_H
