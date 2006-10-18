#include "async.h"
#include "sha1.h"
#include "merkle_node.h"

static void
indent (u_int depth)
{
  while (depth-- > 0)
    warnx << " ";
}

merkle_node *
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
merkle_node::dump (u_int depth)
{
  warnx << "[NODE " 
	<< strbuf ("0x%x", (u_int)this)
	<< ", entry " << strbuf ("0x%x", (u_int)entry)
	<< " cnt:" << count
	<< " hash:" << hash
	<< ">\n";
  err_flush ();
  
  merkle_node *n = this;
  if (!n->isleaf ()) {
    for (int i = 0; i < 64; i++) {
      merkle_node *child = n->child (i);
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
#if 0
  warnx << "[init NODE " 
	<< strbuf ("0x%x", (u_int)this) 
	<< count << "\n";
#endif
}


merkle_node::~merkle_node ()
{
  // recursively deletes down the tree
  delete entry;
  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
}
