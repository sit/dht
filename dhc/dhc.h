#ifndef _DHC_H_
#define _DHC_H_

#include "dhash_impl.h"
#include <dbfe.h>

// PK blocks data structure for maintaining consistency.

ptr<dbfe> keyhash_rep_db;

struct kh_replica {
  long seqnum;
  vec<chordID> nodes;
};

#endif /*_DHC_H_*/
