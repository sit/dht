#ifndef _FINGER_TABLE_PNS_H_
#define _FINGER_TABLE_PNS_H_

#include "stabilize.h"
#include "skiplist.h"
#include "finger_table.h"

class finger_table_pns : public finger_table {
  // these are the "fast" fingers corresponding to each slow finger
  ptr<location> pnsfingers[NBIT];

  int fp;

  void getsucclist_cb (int l, int r, vec<chord_node> succs, chordstat err);

protected:
  finger_table_pns (ptr<vnode> v, ptr<locationtable> l);

 public:
  static ptr<finger_table> produce_finger_table (ptr<vnode> v, ptr<locationtable> l);

  virtual ~finger_table_pns ();

  ptr<location> finger (int i);
  ptr<location> operator[] (int i);
  
  vec<ptr<location> > get_fingers ();
  
  void stabilize_finger ();

  void print (strbuf &outbuf);
  void stats ();
  
  // Stabilize methods
  void do_backoff () { stabilize_finger (); }

};

#endif /* _FINGER_TABLE_PNS_H_ */
