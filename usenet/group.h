#ifndef GROUP_H
#define GROUP_H

#include <dbfe.h>

struct grouplist {
  ptr<dbEnumeration> it;
  ptr<dbPair> d;

  grouplist ();
  void next (str *, int *);
  bool more (void) { return d; };
};

struct group {
  ptr<dbrec> rec;
  int cur_art;
  int start, stop;
  char *c;
  int len;
  str group_name;

  group () : rec (0), cur_art (0) {};
  int open (str);
  int open (str, int *, int *);
  str name (void) { return group_name; };
  
  void xover (int, int);
  strbuf next (void);
  bool more (void) { return start <= stop; };
  bool loaded (void) { return rec; };
  str getid (int);
  str getid (void) { return getid (cur_art); };

  void addid (str);
};

#endif /* GROUP_H */
