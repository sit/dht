#ifndef _MERKLE_MISC_H_
#define _MERKLE_MISC_H_

#include "merkle_hash.h"

template <class T1, class T2>
struct pair {
  T1 first;
  T2 second;
  pair (T1 f, T2 s) : first (f), second (s) {}
};


struct block {
  merkle_hash key;
  itree_entry<block> link;
  block () { key.randomize (); }
  block (u_int i) : key (i) {}
  block (merkle_hash key) : key (key) {}
};




// Supposed to roughly correspond to the on-disk DB.
// It supports insert(), lookup(), and blocks are in sorted order.
struct database : public itree<merkle_hash, block, &block::key, &block::link> {
  typedef itree<merkle_hash, block, &block::key, &block::link> super_t;

  struct database_stats {
    uint64 num_lookup;
    uint64 num_insert;
    uint64 num_remove;
    uint64 num_cursor;
    uint64 num_get_keys;
    uint64 num_next;
  } stats;

  database ()
  {
    bzero (this, sizeof (this));
  }


  void
  clear ()
  {
    while (block *b = first ()) {
      remove (b);
      delete b;
    }
  }

  ~database ()
  {
    clear ();
  }

  block *
  next (block *b)
  {
    stats.num_next++;
    return super_t::next (b);
  }

  // find first block with key >= pos.
  block *
  cursor (merkle_hash pos)
  {
    stats.num_cursor++;

#if 1
    block *ret = lookup (pos);
    if (ret)
      return ret;
    else {
      block b(pos);
      insert (&b);
      block *ret = lookup (pos);
      assert (ret == &b);
      ret = super_t::next (ret);
      remove (&b);
      return ret;
    }
#else
    for (block *b = first (); b; b = super_t::next (b))
      if (b->key >= pos)
	return b;
    return NULL; 
#endif
  }

  block *
  lookup (merkle_hash key)
  {
    stats.num_lookup++;
    return this->operator [] (key);
  }

  vec<merkle_hash>
  get_keys (u_int depth, merkle_hash prefix)
  {
    stats.num_get_keys++;
    vec<merkle_hash> ret;
    block *b = cursor (prefix);
    for ( ; b && prefix_match (depth, b->key, prefix); b = next (b))
      ret.push_back (b->key);
    return ret;
  }

  void
  dump ()
  {
    warnx << "DATABASE\n";
    for (block *b = first (); b; b = next (b))
      warnx << b->key << "\n";
  }



  void
  dump_stats ()
  {
    warn << "Database stats......\n";
    warn << "  size " << stats.num_insert - stats.num_remove << "\n";
    warn << "  num_insert " << stats.num_insert << "\n";
    warn << "  num_remove " << stats.num_remove << "\n";
    warn << "  num_cursor " << stats.num_cursor << "\n";
    warn <<"   num_get_keys " << stats.num_get_keys << "\n";
    warn << "  num_next " << stats.num_next << "\n";
  }
};


static inline vec<merkle_hash>
database_get_keys (database *db, u_int depth, merkle_hash prefix)
{
  vec<merkle_hash> ret;
  block *b = db->cursor (prefix);
  for ( ; b && prefix_match (depth, b->key, prefix); b = db->next (b))
    ret.push_back (b->key);
  return ret;
}


#endif /* _MERKLE_MISC_H_ */
