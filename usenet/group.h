#ifndef GROUP_H
#define GROUP_H

#include <dbfe.h>
#include <chord.h>

struct grouplist {
  ptr<dbEnumeration> it;
  ptr<dbPair> d;

  grouplist ();
  void next (str *, unsigned long *);
  bool more (void) { return d; };
};

struct group {
  ptr<dbrec> rec;
  unsigned long cur_art, start, stop;
  char *c;
  int len;
  str group_name;

  group () : rec (0), cur_art (0) {};
  int open (str);
  int open (str, volatile unsigned long *, unsigned long *, unsigned long *);
  str name (void) { return group_name; };
  
  void xover (unsigned long, unsigned long);
  strbuf next (void);
  //  void next_cb (str, dhash_stat, ptr<dhash_block>, vec<chordID>);
  bool more (void) { return start <= stop; };
  bool loaded (void) { return rec; };
  chordID getid (unsigned long);
  chordID getid (void) { return getid (cur_art); };
  chordID getid (str);

  void addid (str, chordID);
};

#endif /* GROUP_H */
