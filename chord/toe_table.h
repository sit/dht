#ifndef _TOE_TABLE_H_
#define _TOE_TABLE_H_

#define MAX_LEVELS 5

class toe_table : public stabilizable {
  static const int max_delay = 800; // ms

  vec<chordID> toes;
  ptr<locationtable> locations;
  chordID myID;
  
  short target_size[MAX_LEVELS];
  int in_progress;
  
  short last_level;

  void add_toe_ping_cb (chordID id, int level, chordstat err);
  void get_toes_rmt_cb (chord_nodelistextres *res, int level, clnt_stat err);

 public:
  toe_table (ptr<locationtable> locs, chordID id);

  vec<chordID> get_toes (int level);
  void add_toe (chordID id, net_address r, int level);
  int filled_level ();
  int level_to_delay ();
  void get_toes_rmt (int level);
  void stabilize_toes ();
  int level_to_delay (int level);
  void dump ();
  short get_last_level () { return last_level; };
  void set_last_level (int l) { last_level = l; };
  void bump_target (int l) { target_size[l] *= 2; };

  // Stabilizable methods
  bool backoff_stabilizing () { return in_progress > 0; }
  void do_backoff () { stabilize_toes (); }
  bool isstable () { return true; } // XXX
};

#endif /* _TOE_TABLE_H_ */
