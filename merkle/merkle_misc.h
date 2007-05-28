#ifndef _MERKLE_MISC_H_
#define _MERKLE_MISC_H_

#include "merkle_hash.h"
#include "merkle_sync_prot.h"

#if 0
static inline ref<dbrec>
todbrec (const merkle_hash &h)
{
  ref<dbrec> ret = New refcounted<dbrec> (h.bytes, h.size);
  reverse ((u_char *)ret->value, ret->len);
  return ret;
}
#endif

#if 0
static inline ref<dbrec>
id2dbrec(chordID id)
{
  char buf[sha1::hashsize];
  bzero (buf, sha1::hashsize);
  mpz_get_rawmag_be (buf, sha1::hashsize, &id);
  return New refcounted<dbrec> (buf, sha1::hashsize);
}


static inline chordID
dbrec2id (ptr<dbrec> r)
{
  return tobigint (to_merkle_hash (r));
}
#endif

#endif /* _MERKLE_MISC_H_ */
