#include "merkle_misc.h"
#include "id_utils.h"

//XXX probably could rewrite this in terms of below function
vec<bigint>
database_get_keyrange (dbfe *db, const bigint &min, 
		       const bigint &max)
{
  vec<bigint> ret;
  ptr<dbEnumeration> iter = db->enumerate ();
  ptr<dbPair> entry = iter->firstElement ();

  while (entry) {
    merkle_hash key = to_merkle_hash (entry->key);
    if (betweenbothincl (min, max, tobigint (key))) 
      ret.push_back (tobigint(key));
    entry = iter->nextElement ();
  }
  iter = NULL;
  return ret;
}

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


