%#include <chord_types.h>

struct article_mapping {
  u_int32_t	artno;
  string	msgid<>;
  chordID	blkid;
};

struct group_entry {
  /* Invariant: articles are sorted in increasing order of artno */
  article_mapping articles<>;
};

struct article_overview {
  str subject;
  str from;
  str date;
  str msgid;
  str references;
  u_int32_t lines;
};
