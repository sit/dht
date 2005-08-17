#include "merkle_misc.h"
#include "id_utils.h"
#include "dhblock_noauth_srv.h"
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



#if 0
ptr<dbrec>
to_merkle_key (ptr<dbfe> db, ptr<dbrec> key, dhash_ctype c) 
{
  ptr<dbrec> ret;
  switch (c) {
  case DHASH_CONTENTHASH:
  case DHASH_KEYHASH:
    {
      ret = key;
    }
    break;
  case DHASH_NOAUTH:
    {
      //ptr<dbrec>dbkey = dhblock_noauth_srv::get_database_key (dbrec2id(key));
      chordID dbkey = dbrec_to_id(key) >> 32;
      dbkey = dbkey << 32;

      ptr<dbrec> data = db->lookup (id2dbrec(dbkey));
      //      ret = dhblock_noauth_srv::get_merkle_key (dbrec2id(key), data);
      assert (data);
      
      //get the low bits: xor of the marshalled words
      long *d = (long *)data->value;
      long hash = 0;
      for (int i = 0; i < data->len/4; i++) 
	hash ^= d[i];
      

      chordID mkey = dbkey | bigint(hash);
      
      return id2dbrec(mkey);
    }
    break;
  default:
    warn << "unsupported block type " << c << "\n";
  }
  return ret;
}

#endif
