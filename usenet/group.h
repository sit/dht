#ifndef GROUP_H
#define GROUP_H

struct dbrec;
struct dbEnumeration;
#include <chord_types.h>
#include "usenetdht_storage.h"

bool valid_group_name (str g);
bool create_group (const char *group);

struct grouplist {
  ptr<dbEnumeration> it;
  ptr<dbPair> d;

  grouplist ();
  void next (str *, unsigned long *);
  bool more (void) { return d; };
};

/*
 * A view of a newsgroup for _one_ connection.
 */ 
class newsgroup {
  // Last fetched information about the group
  group_entry *group;

  // Private state for xover
  unsigned long start, stop;
  unsigned long next_idx;

  str group_name;
  ptr<dbrec> group_name_rec;

  static group_entry *load (ptr<dbrec> g);

public: 
  unsigned long cur_art;

  newsgroup ();
  ~newsgroup ();
  int open (str);
  int open (str, unsigned long *, unsigned long *, unsigned long *);
  str name (void) { return group_name; };
  
  void xover (unsigned long, unsigned long);
  strbuf next (void);
  bool more (void);
  bool loaded (void) { return group != NULL; };
  chordID getid (unsigned long);
  chordID getid (void) { return getid (cur_art); };
  static chordID getid (str msgid);

  void addid (str, chordID);
};

#endif /* GROUP_H */
