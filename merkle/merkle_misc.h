#ifndef _MERKLE_MISC_H_
#define _MERKLE_MISC_H_

#include "qhash.h"
#include "merkle_hash.h"
#include "merkle_sync_prot.h"
#include "dhash_prot.h"
#include "dhash_common.h"


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


struct block {
  merkle_hash key;
  ptr<dbrec> data;
  block (merkle_hash key, ptr<dbrec> data) : key (key), data (data) {}
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


static inline merkle_hash
to_merkle_hash (ptr<dbrec> a)
{
  merkle_hash h;
  assert (a->len == h.size);
  bcopy (a->value, h.bytes, h.size);
  reverse (h.bytes, h.size);
  return h;
}

static inline ref<dbrec>
todbrec (const merkle_hash &h)
{
  ref<dbrec> ret = New refcounted<dbrec> (h.bytes, h.size);
  reverse ((u_char *)ret->value, ret->len);
  return ret;
}

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


static inline vec<merkle_hash>
database_get_keys (dbfe *db, u_int depth, const merkle_hash &prefix)
{
#if 0
  warn << " >>database_get_keys " << depth << "/" << prefix << "\n";
#endif

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

#if 0
  warn << " <<database_get_keys, returning " << ret.size () << "\n";
#endif
  return ret;
}


static inline int
database_remove (dbfe *db, block *b)
{
  return db->del (todbrec(b->key));
}


static inline int
database_insert (dbfe *db, block *b)
{
#if 0
  warn << " >>database_insert " << b->key << "\n";
#endif
  int ret = db->insert (todbrec (b->key), b->data);
#if 0
  warn << " <<database_insert\n";
#endif
  return ret;
}


static inline ptr<dbrec>
database_lookup (dbfe *db, const merkle_hash &key)
{
#if 0
  warn << " >>database_lookup " << key << "\n";
#endif
  ptr<dbrec> ret = db->lookup (todbrec (key));
#if 0
  warn << " <<database_lookup\n";
#endif
  return ret;
}


static inline ptr<dbEnumeration>
database_enumerate (dbfe *db)
{
  // XXX: DEAD CODE?

  assert (0); // WHY??
  return db->enumerate();
}

static inline ptr<dbPair>
nextElement (ptr<dbEnumeration> iterator)
{
  // XXX: DEAD CODE?

  assert (0); // want to return pair<merkle_hash, ptr<dbrec>> ?
  return iterator->nextElement ();
}

static inline ptr<dbPair>
nextElement (ptr<dbEnumeration> iterator, const merkle_hash &startkey)
{
  // XXX: DEAD CODE?


  assert (0); // want to return pair<merkle_hash, ptr<dbrec>> ?
  return iterator->nextElement (todbrec (startkey));
}



// ------------------------------------------------------------------------------
// database iterators

class db_iterator {
public:
  virtual bool more () = 0;
  virtual merkle_hash next () = 0;
  virtual merkle_hash peek () = 0;
};



class db_range_iterator : public db_iterator {
  // iterates over a range of database keys   
private:
  dbfe *db;
  ptr<dbEnumeration> iter;
  ptr<dbPair> entry;
  u_int depth;
  merkle_hash prefix;

protected:
  bool match ();

public:
  virtual bool more ();
  virtual merkle_hash peek ();
  virtual merkle_hash next ();
  db_range_iterator (dbfe *db, u_int depth, merkle_hash prefix);
  virtual ~db_range_iterator ();
};


class db_range_xiterator : public db_range_iterator {
  // iterates over a range of database keys, keys in the exclusion set
  // (i.e., 'xset') are skipped

private:
  qhash<merkle_hash, bool> *xset;
  bigint rngmin;
  bigint rngmax;

  void advance ();

public:
  virtual merkle_hash next ();
  db_range_xiterator (dbfe *db, u_int depth, merkle_hash prefix,
		      qhash<merkle_hash, bool> *xset, bigint rngmin, bigint rngmax);
  virtual ~db_range_xiterator ();
};



#endif /* _MERKLE_MISC_H_ */
