#include "merkle_node.h"

static void
indent (u_int depth)
{
  while (depth-- > 0)
    warnx << " ";
}

const merkle_node *
merkle_node::child (u_int i) const
{
  return &(*entry)[i];
}

merkle_node *
merkle_node::child (u_int i)
{
  assert (!isleaf ());
  assert (entry);
  assert (i >= 0 && i < 64);
  return &(*entry)[i];
}

bool
merkle_node::isleaf () const
{
  return (entry == NULL);
}

bool
merkle_node::leaf_is_full () const
{
  // XXX what about at the bottom level (count == 16)!!!!
  assert (isleaf ());
  return (count == 64);
}

void
merkle_node::internal2leaf ()
{
  // This recursively deletes down to the leaves
  // since entry is an array<...>
  delete entry;
  entry = NULL;
}

void
merkle_node::leaf2internal ()
{
  // XXX only 16 branches on the lowest level ???
  assert (entry == NULL);
  entry = New array<merkle_node, 64> ();
}


void
merkle_node::dump (u_int depth) const
{
#if 1
  warnx << "[NODE " 
	<< strbuf ("0x%x", (u_int)this)
	<< ", entry " << strbuf ("0x%x", (u_int)entry)
	<< " cnt:" << count
	<< " hash:" << hash
	<< ">\n";
  err_flush ();
#endif
  
  const merkle_node *n = this;
  if (!n->isleaf ()) {
    for (int i = 0; i < 64; i++) {
      const merkle_node *child = n->child (i);
      if (child->count) {
	indent (depth + 1);
	warnx << "[" << i << "]: ";
	child->dump (depth + 1);
      }
    }
  }
}


merkle_node::merkle_node ()
{
  bzero (this, sizeof (*this));
}

void
merkle_node::initialize (u_int64_t _count)
{
  bzero (this, sizeof (*this));
  this->count = _count; 
}  


merkle_node::~merkle_node ()
{
  // recursively deletes down the tree
  delete entry;
  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
}

void
merkle_node::check_invariants (u_int depth, merkle_hash prefix, dbfe *db)
{
#if 0
  warn << "CHECKING: " << strbuf ("0x%x", (u_int)this) << " depth " << depth << " pfx: " << prefix << "\n";


  bool ack = (((u_int)this) == 0x8104000);
  // XXXX
#endif
  bool ack = false;

  sha1ctx sc;
  merkle_hash mhash = 0;
  u_int64_t _count = 0;
  if (isleaf ()) {
    assert (count <= 64);

#ifdef NEWDB
    vec<merkle_hash> keys = database_get_keys (db, depth, prefix);
    if (ack) {
      warn << "depth " << depth << "\n";
      warn << "prefix " << prefix << "\n";
      warn << "keys.size () " << keys.size() << "\n";
    }

    for (u_int i = 0; i < keys.size (); i++, _count++)
      sc.update (keys[i].bytes, keys[i].size);
#else
    for (block *cur = db->cursor (prefix) ; cur; cur = db->next (cur)) {
      if (!prefix_match (depth, cur->key, prefix))
	break;
      _count += 1;
      sc.update (cur->key.bytes, cur->key.size);
    }
#endif
  } else {
    assert (count > 64);
    for (int i = 0; i < 64; i++) {
      merkle_node *n = child (i);
      _count += n->count;
      sc.update (n->hash.bytes, n->hash.size);
      prefix.write_slot (depth, i);
      n->check_invariants (depth+1, prefix, db);
    }
  }
  
  //warn << "[" << strbuf ("0x%x", (u_int)this) << "] cnt " << count << " _cnt " << _count << "\n";

  assert (count == _count);
  if (count == 0)
    assert (hash == 0);
  else {
    sc.final (mhash.bytes);
    assert (hash == mhash);
    assert (hash != 0);
  }
}
