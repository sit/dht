#ifndef __OBSERVER_H
#define __OBSERVER_H

class Observer {
public:
  Observer();
  ~Observer();

  virtual void execute() = 0;
};

#endif // __OBSERVER_H
