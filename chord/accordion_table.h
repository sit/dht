#ifndef _ACCORDION_TABLE_H_
#define _ACCORDION_TABLE_H_

#include "stabilize.h"
#include "accordion.h"

class accordion_table : public stabilizable {

protected:
  ptr<accordion> myvnode;
  ptr<locationtable> locations;

  chordID myID;

  accordion_table(ptr<accordion> v, ptr<locationtable> l);

public:
  virtual bool backoff_stabilizing () { return false; }
  virtual void do_backoff () { explore_table(); }
  virtual bool isstable () { return false; }

  void explore_table();
  void del_node(const chordID x);
  vec<ptr<location> > get_fingers (ptr<location> src, chordID end);
  vec<ptr<location> > nexthops (const chordID &x, unsigned p);

  void fill_gap_nodelistres (chord_nodelistres *res, ptr<location> src, chordID end);

  void fill_gap_cb(ptr<location> l, vec<chord_node> nlist, chordstat err);

  //XXX i do not really use these funnctions
  void fill_nodelistres (chord_nodelistres *res) { assert(0);}
  void fill_nodelistresext (chord_nodelistextres *res) {assert(0);}

private:
  chordID mingap;
  
  ptr<location> biggest_gap(chordID &end);
  ptr<location> random_nbr(chordID &end);

};
#endif

