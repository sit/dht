#ifndef __CHORD_OBSERVER_H
#define __CHORD_OBSERVER_H

#include "observer.h"
#include "chord.h"

class ChordObserver : public Observer {
public:
  static ChordObserver* Instance(Args*);
  virtual void execute();
  vector<Chord::IDMap> get_sorted_nodes(unsigned int = 0);

private:
  void init_nodes(unsigned int num);
  void vivaldi_error();
  static ChordObserver *_instance;
  ChordObserver(Args*);
  ~ChordObserver();
  unsigned int _reschedule;
  string _type;
  unsigned int _num_nodes;
  unsigned int _init_num;

  vector<ConsistentHash::CHID> lid;
  vector<Chord::IDMap> _allsorted;
};

#endif // __CHORD_OBSERVER_H
