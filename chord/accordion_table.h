#ifndef _ACCORDION_TABLE_H_
#define _ACCORDION_TABLE_H_

#include "stabilize.h"
#include "accordion.h"

class accordion_table {

protected:
  ptr<accordion> myvnode;
  ptr<locationtable> locations;
  chordID myID;
  accordion_table(ptr<accordion> v, ptr<locationtable> l);

public:
  void start ();

  void explore_table();
  void del_node(const chordID x);
  vec<ptr<location> > get_fingers (ptr<location> src, chordID end, unsigned p);
  vec<ptr<location> > nexthops (const chordID &x, unsigned p, vec<ptr<location> > tried);

  void fill_gap_cb(ptr<location> l, vec<chord_node> nlist, chordstat err);

  //XXX i do not really use these funnctions
  void fill_nodelistresext (chord_nodelistextres *res);

private:
  chordID mingap;
  int nout;
  
  vec<ptr<location> > biggest_gap(unsigned p);
  vec<ptr<location> > random_nbr(unsigned p);

  unsigned d_sec;
  unsigned d_msec;

};
#endif

