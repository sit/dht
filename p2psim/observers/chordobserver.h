#ifndef __CHORD_OBSERVER_H
#define __CHORD_OBSERVER_H

#include "protocols/chord.h"
#include "p2psim/oldobserver.h"

class ChordObserver : public Oldobserver {
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
  unsigned int _num_nodes;
  unsigned int _init_num;

  vector<ConsistentHash::CHID> lid;
  vector<Chord::IDMap> _allsorted;
};

#endif // __CHORD_OBSERVER_H
