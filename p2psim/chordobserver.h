#ifndef __CHORD_OBSERVER_H
#define __CHORD_OBSERVER_H

#include "observer.h"
#include "args.h"

class ChordObserver : public Observer {
public:
  static ChordObserver* Instance(Args*);
  virtual void execute();

private:
  static ChordObserver *_instance;
  ChordObserver(Args*);
  ~ChordObserver();
};

#endif // __CHORD_OBSERVER_H
