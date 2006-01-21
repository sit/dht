#ifndef _MERKLE_MISC_H_
#define _MERKLE_MISC_H_

#include "merkle_hash.h"
#include "merkle_sync_prot.h"

static inline str err2str (merkle_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}

template <class T1, class T2>
struct pair {
  T1 first;
  T2 second;
  pair (T1 f, T2 s) : first (f), second (s) {}
};


static inline void
reverse (u_char *buf, u_int size)
{
  assert (size == sha1::hashsize);

  for (u_int i = 0; i < (size / 2); i++) {
    char tmp = buf[i];
    buf[i] = buf[size - 1 - i];
    buf[size - 1 - i] = tmp;
  }
}

merkle_hash to_merkle_hash (str a);
merkle_hash to_merkle_hash (bigint id);


#if 0
static inline ref<dbrec>
todbrec (const merkle_hash &h)
{
  ref<dbrec> ret = New refcounted<dbrec> (h.bytes, h.size);
  reverse ((u_char *)ret->value, ret->len);
  return ret;
}
#endif

static inline bigint
tobigint (const merkle_hash &h)
{
#if 0
  str raw = str ((char *)h.bytes, h.size);
  bigint ret;
  ret.setraw (raw);
  return ret;
#else
  bigint ret = 0;
  for (int i = h.size - 1; i >= 0; i--) {
    ret <<= 8;
    ret += h.bytes[i];
  }
  return ret;
#endif
}

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
