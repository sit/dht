#ifndef __CHORDTOE_H
#define __CHORDTOE_H

#include "chord.h"
#include "chordfinger.h"

/* ChordFinger implements finger table in addition to Chord with succ list*/

class LocTableToe : public LocTable {
public:
  LocTableToe () : LocTable () {};
  virtual ~LocTableToe () {};

  Chord::IDMap next_hop(Chord::CHID key, Chord::IDMap me); 
};

class ChordToe : public ChordFinger {
public:
  ChordToe(Node *n, uint base, uint successors, uint maxf);
  ChordToe ();
  ~ChordToe() {};
  string proto_name() { return "ChordToe"; }
  
  void dump() {};
  bool stabilized (vector<CHID> lid) { return true;}
  void init_state(vector<IDMap> ids);
  
protected:
  
  int add_toe (IDMap i);

  vector<IDMap> _toes;
  uint _numtoes;
  
};

#endif
