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

struct newsgroup {
  ptr<dbrec> rec;
  unsigned long cur_art, start, stop;
  char *c;
  int len;
  str group_name;

  newsgroup ();
  int open (str);
  int open (str, unsigned long *, unsigned long *, unsigned long *);
  str name (void) { return group_name; };
  
  void xover (unsigned long, unsigned long);
  strbuf next (void);
  bool more (void) { return start < stop && len > 0; };
  bool loaded (void) { return rec; };
  chordID getid (unsigned long);
  chordID getid (void) { return getid (cur_art); };
  chordID getid (str);

  void addid (str, chordID);
};

#endif /* GROUP_H */
