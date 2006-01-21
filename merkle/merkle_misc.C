#include "merkle_misc.h"
#include "id_utils.h"
#include "bigint.h"

merkle_hash
to_merkle_hash (str a)
{
  merkle_hash h;
  assert (a.len () == h.size);
  bcopy (a.cstr (), h.bytes, h.size);
  reverse (h.bytes, h.size);
  return h;
}

merkle_hash
to_merkle_hash (bigint id)
{
  char buf[sha1::hashsize];
  bzero (buf, sha1::hashsize);
  mpz_get_rawmag_be (buf, sha1::hashsize, &id);
  merkle_hash h;
  bcopy (buf, h.bytes, h.size);
  reverse (h.bytes, h.size);
  return h;
}
