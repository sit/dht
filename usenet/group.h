#ifndef GROUP_H
#define GROUP_H

#include <dbfe.h>

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
  unsigned long open (str);
  unsigned long open (str, unsigned long *, unsigned long *);
  str name (void) { return group_name; };
  
  void xover (unsigned long, unsigned long);
  strbuf next (void);
  bool more (void) { return start <= stop; };
  bool loaded (void) { return rec; };
  str getid (unsigned long);
  str getid (void) { return getid (cur_art); };

  void addid (str);
};

#endif /* GROUP_H */
