#ifndef __CHORDTOE_H
#define __CHORDTOE_H

#include "chordfinger.h"
#include "chordfingerpns.h"

/* ChordFinger implements finger table in addition to Chord with succ list*/

class LocTableToe : public LocTable {
public:
  int _style;
  vector<Chord::IDMap> _toes;

  LocTableToe () : LocTable () {};
  virtual ~LocTableToe () {};

  Chord::IDMap next_hop(Chord::CHID key, bool *done);

  void set_style (int i) {_style = i;};
  void set_toes (vector<Chord::IDMap> t) { _toes = t; };
};

class ChordToe : public ChordFingerPNS {
public:
  ChordToe(Node *n, Args& a);
  ChordToe ();
  ~ChordToe() {};
  string proto_name() { return "ChordToe"; }
  
  void dump() {};
  bool stabilized (vector<CHID> lid) { return true;}
  void init_state(vector<IDMap> ids);
  
protected:

  int _lookup_style;
  
  int add_toe (IDMap i);

  vector<IDMap> _toes;
  uint _numtoes;
  
};

#endif
