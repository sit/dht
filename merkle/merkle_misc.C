#include "merkle_misc.h"

// ---------------------------------------------------------------------------
// db_range_iterator

bool
db_range_iterator::match ()
{ 
  if (!entry)
    return false;
  merkle_hash key = to_merkle_hash (entry->key);
  return prefix_match (depth, key, prefix);
}

bool
db_range_iterator::more ()
{
  return !!entry;
}

merkle_hash
db_range_iterator::peek ()
{
  assert (entry);
  merkle_hash key = to_merkle_hash (entry->key);
  return key;
}

merkle_hash
db_range_iterator::next ()
{
  assert (entry);
  merkle_hash key = to_merkle_hash (entry->key);
  entry = iter->nextElement ();
  if (!match ())
    entry = NULL;
  return key;
}

db_range_iterator::db_range_iterator (dbfe *db, u_int depth, merkle_hash prefix) 
  :  db (db), iter (NULL), entry (NULL), depth (depth), prefix (prefix)
{
  iter = db->enumerate ();
  entry = iter->nextElement (todbrec(prefix));
  if (!match ())
    entry = NULL;
}

db_range_iterator::~db_range_iterator () 
{ 
}


// ---------------------------------------------------------------------------
// db_range_xiterator

void
db_range_xiterator::advance ()
{
  while (db_range_iterator::more ()) {
    merkle_hash h = db_range_iterator::peek ();
    bigint hh = tobigint (h);
    if ((!(*xset)[h]) && hh >= rngmin && hh <= rngmax)
      break;
    db_range_iterator::next ();
  }
}

merkle_hash
db_range_xiterator::next ()
{
  merkle_hash ret = db_range_iterator::next ();
  advance ();
  return ret;
}

db_range_xiterator::db_range_xiterator (dbfe *db, u_int depth, 
					merkle_hash prefix, 
					qhash<merkle_hash, bool> *xset, 
					bigint rngmin, bigint rngmax)
  : db_range_iterator (db, depth, prefix), xset (xset), rngmin (rngmin), rngmax (rngmax)
{ 
  advance (); 
}


db_range_xiterator::~db_range_xiterator () 
{ 
  delete xset; 
  xset = NULL; 
}

