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
#if 0
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
      indent (depth + 1);
      warnx << "[" << i << "]: ";
      child->dump (depth + 1);
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
#if 0
  warnx << "~MN:" 
	<< strbuf ("0x%x", (u_int)this)
	<< ", entry " << strbuf ("0x%x", (u_int)entry)
	<< ", trash " << strbuf ("0x%x", (u_int)(((merkle_node *)0x8172028)->entry))
	<< ">> \n";
  err_flush ();
#endif

  assert ((u_int)entry != 0xc5c5c5c5);

#if 0
  if (!isleaf ())
    for (int i = 0; i < 64; i++) {
      warnx << strbuf ("~MN: 0x%x child[%d] -- before (%x)\n", 
		       (u_int)this, i, (u_int)(((merkle_node *)0x8172028)->entry));
      delete child (i);
      warnx << strbuf ("~MN: 0x%x child[%d] -- after (%x)\n", (u_int)this, i, 
		       (u_int)(((merkle_node *)0x8172028)->entry));
    }
#else
  // recursively deletes down the tree
  delete entry;
#endif

  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
#if 0
  warnx << strbuf ("~MN: 0x%x (%x)\n", (u_int)this, (u_int)(((merkle_node *)0x8172028)->entry));
#endif
#if 0
  if ((u_int)this == 0x8172008) {
    asm ("int $3");
  }
#endif
}

void
merkle_node::check_invariants (u_int depth, merkle_hash prefix, database *db)
{
  sha1ctx sc;
  merkle_hash mhash = 0;
  u_int64_t _count = 0;
  if (isleaf ()) {
    assert (count <= 64);
    for (block *cur = db->cursor (prefix) ; cur; cur = db->next (cur)) {
      if (!prefix_match (depth, cur->key, prefix))
	break;
      _count += 1;
      sc.update (cur->key.bytes, cur->key.size);
    }
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
  
  assert (count == _count);
  if (count == 0)
    assert (hash == 0);
  else {
    sc.final (mhash.bytes);
    assert (hash == mhash);
    assert (hash != 0);
  }
}
