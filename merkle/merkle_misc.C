#include "merkle_misc.h"
#include "id_utils.h"
#include "dhblock_noauth_srv.h"

vec<merkle_hash>
database_get_keys (dbfe *db, u_int depth, const merkle_hash &prefix)
{
  vec<merkle_hash> ret;
  ptr<dbEnumeration> iter = db->enumerate ();
  ptr<dbPair> entry = iter->nextElement (todbrec(prefix));

  while (entry) {
    merkle_hash key = to_merkle_hash (entry->key);
    if (!prefix_match (depth, key, prefix))
      break;
    ret.push_back (key);
    entry = iter->nextElement ();
  }
  iter = NULL;
  return ret;
}


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
      ptr<dbrec> dbkey = dhblock_noauth_srv::get_database_key (dbrec2id(key));
      ptr<dbrec> data = db->lookup (dbkey);
      ret = dhblock_noauth_srv::get_merkle_key (dbrec2id(key), data);
    }
    break;
  default:
    warn << "unsupported block type " << c << "\n";
  }
  return ret;
}

