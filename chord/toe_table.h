#ifndef _TOE_TABLE_H_
#define _TOE_TABLE_H_

#define MAX_LEVELS 10

#include "stabilize.h"

class toe_table : public stabilizable {
  static const int max_delay = 800; // ms

  vec<vec<ptr<location> >*, MAX_LEVELS> toes;
  ptr<locationtable> locations;
  chordID myID;
  ptr<vnode> myvnode;

  short target_size[MAX_LEVELS];
  int in_progress;
  
  short last_level;

  void get_toes_rmt_cb (chord_nodelistres *res, int level, clnt_stat err);

  bool stable_toes;

 public:
  toe_table (ptr<vnode> v, ptr<locationtable> l);
  
  bool present (chordID id);
  bool present (chordID id, int level);
  vec<ptr<location> > get_toes (int level);
  void add_toe (const chord_node &n, int level);
  int filled_level ();
  void get_toes_rmt (int level);
  void stabilize_toes ();
  int level_to_delay (int level);
  short get_last_level () { return last_level; };
  void set_last_level (int l) { last_level = l; };
  void bump_target (int l) { target_size[l] *= 2; };
  void prune_toes (int level);
  short get_target_size (int level) { return target_size[level]; }
  int count_unique ();

  // Stabilizable methods
  bool backoff_stabilizing () { return in_progress > 0; }
  void do_backoff () { stabilize_toes (); }
  bool isstable () { return stable_toes; }

  void fill_nodelistresext (chord_nodelistextres *res);
  void fill_nodelistres (chord_nodelistres *res);

  //  ptr<location> closestpred (const chordID &x, vec<chordID> fail);

  void print (strbuf &outbuf);
};

#endif /* _TOE_TABLE_H_ */
