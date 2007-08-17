#include <async.h>

#include <chord_types.h>
#include <id_utils.h>

#include "merkle_hash.h"
#include "merkle_tree.h"

// {{{ Utility Functions
static void
indent (u_int depth)
{
  while (depth-- > 0)
    warnx << " ";
}

static merkle_key *
closestsucc (itree<chordID, merkle_key, &merkle_key::id, &merkle_key::ik> &keylist, chordID k)
{
  merkle_key *p = NULL;
  merkle_key *n = keylist.root ();
  // warnx << "closestsucc k = " << k << "\n";
  if (!n)
    return NULL;
  while (n) {
    p = n;
    // warnx << "closestsucc n->id = " << n->id << "\n";
    if (k < n->id)
      n = keylist.left (n);
    else if (k > n->id)
      n = keylist.right (n);
    else
      return n;
  }
  if (k < p->id)
    return p;
  else {
    // Wrap around at the end.
    n = keylist.next (p);
    return n ? n : keylist.first ();
  }
}
// }}}
// {{{ merkle_node_mem
class merkle_node_mem : public merkle_node {
  friend class merkle_tree_mem;
private:
  array<merkle_node_mem, 64> *entry;
  void initialize (u_int64_t _count);

public:
  virtual merkle_hash child_hash (u_int i);
  virtual merkle_node *child (u_int i);
  virtual bool isleaf () const;
  virtual void internal2leaf ();
  virtual void leaf2internal ();
  void dump (u_int depth);

  merkle_node_mem ();
  ~merkle_node_mem ();
};

merkle_node_mem::merkle_node_mem () :
  entry (NULL)
{
}

void
merkle_node_mem::initialize (u_int64_t _count)
{
  //bzero (this, sizeof (*this));
  entry = NULL;
  hash = 0;
  this->count = _count;
#if 0
  warnx << "[init NODE "
	<< strbuf ("0x%x", (u_int)this)
	<< count << "\n";
#endif
}

merkle_node_mem::~merkle_node_mem ()
{
  // recursively deletes down the tree
  delete entry;
  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this));
}

merkle_node *
merkle_node_mem::child (u_int i)
{
  assert (!isleaf ());
  assert (entry);
  assert (i >= 0 && i < 64);
  return &(*entry)[i];
}

merkle_hash
merkle_node_mem::child_hash (u_int i)
{
  assert (!isleaf ());
  assert (entry);
  assert (i >= 0 && i < 64);
  return (*entry)[i].hash;
}

bool
merkle_node_mem::isleaf () const
{
  return (entry == NULL);
}

void
merkle_node_mem::internal2leaf ()
{
  // This recursively deletes down to the leaves
  // since entry is an array<...>
  delete entry;
  entry = NULL;
}

void
merkle_node_mem::leaf2internal ()
{
  // XXX only 16 branches on the lowest level ???
  assert (entry == NULL);
  entry = New array<merkle_node_mem, 64> ();
}

void
merkle_node_mem::dump (u_int depth)
{
  warnx << "[NODE "
	<< strbuf ("0x%x", (u_int)this)
	<< ", entry " << strbuf ("0x%x", (u_int)entry)
	<< " cnt:" << count
	<< " hash:" << hash
	<< ">\n";
  err_flush ();

  merkle_node *n = dynamic_cast<merkle_node *> (this);
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
// }}}
// {{{ merkle_tree_mem
merkle_tree_mem::merkle_tree_mem () :
  root (New merkle_node_mem ())
{
  // warn << "root: " << root->isleaf() << "\n";
}

merkle_tree_mem::~merkle_tree_mem ()
{
  keylist.deleteall_correct ();
  delete root;
  root = NULL;
}

merkle_node *
merkle_tree_mem::get_root ()
{
  return dynamic_cast<merkle_node *> (root);
}

// return the node a given depth matching key
// returns NULL if no such node exists
merkle_node *
merkle_tree_mem::lookup_exact (u_int depth, const merkle_hash &key)
{
  u_int realdepth = 0;
  merkle_node *n = merkle_tree::lookup (&realdepth, depth, key, get_root());
  assert (realdepth <= depth);
  return  (realdepth != depth) ? NULL : n;
}

// Might return parent.
// Never returns NULL.
// Never returns a node deeper than depth.
merkle_node *
merkle_tree_mem::lookup (u_int depth, const merkle_hash &key)
{
  u_int depth_ignore = 0;
  return merkle_tree::lookup (&depth_ignore, depth, key, get_root());
}

merkle_node *
merkle_tree_mem::lookup (u_int *depth, u_int max_depth, const merkle_hash &key)
{
  *depth = 0;
  return merkle_tree::lookup (depth, max_depth, key, get_root());
}

void
merkle_tree_mem::count_blocks (u_int depth, const merkle_hash &key,
			       array<u_int64_t, 64> &nblocks)
{
  for (int i = 0; i < 64; i++)
    nblocks[i] = 0;

  // XXX duplicates code with rehash ()
  merkle_hash prefix = key;
  prefix.clear_suffix (depth);

  vec<merkle_hash> keys = database_get_keys (depth, prefix);
  for (u_int i = 0; i < keys.size (); i++) {
    u_int32_t branch = keys[i].read_slot (depth);
    nblocks[branch] += 1;
  }
}

void
merkle_tree_mem::leaf2internal (u_int depth, const merkle_hash &key,
			        merkle_node *n)
{
  assert (n->isleaf ());
  array<u_int64_t, 64> nblocks;
  count_blocks (depth, key, nblocks);
  n->leaf2internal ();

  merkle_hash prefix = key;
  prefix.clear_suffix (depth);

  u_int xmax = (depth == merkle_hash::NUM_SLOTS) ? 16 : 64;
  for (u_int i = 0; i < xmax; i++) {
    // warn << "leaf2internal [" << i << "] = " << nblocks[i] << "\n";
    merkle_node_mem *child =
      dynamic_cast<merkle_node_mem *> (n->child (i));
    child->initialize (nblocks[i]);
    prefix.write_slot (depth, i);
    rehash (depth+1, prefix, child);
  }
}

int
merkle_tree_mem::remove (u_int depth, merkle_hash& key, merkle_node *n)
{
  if (n->isleaf ()) {
    chordID k = static_cast<bigint> (key);
    merkle_key *mkey = keylist[k];
    assert (mkey);
    keylist.remove (mkey);
    delete mkey;
  } else {
    u_int32_t branch = key.read_slot (depth);
    remove (depth+1, key, n->child (branch));
  }

  assert (n->count != 0);
  n->count -= 1;
  if (!n->isleaf () && n->count <= 64)
    n->internal2leaf ();
  rehash (depth, key, n);

  return 0;
}


int
merkle_tree_mem::insert (u_int depth, merkle_hash& key, merkle_node *n)
{
  int ret = 0;

  if (n->isleaf () && n->leaf_is_full ())
    leaf2internal (depth, key, n);

  if (n->isleaf ()) {
    merkle_key *k = New merkle_key (key);
    assert (!keylist[k->id]);
    keylist.insert (k);
  } else {
    u_int32_t branch = key.read_slot (depth);
    ret = insert (depth+1, key, n->child (branch));
  }

  n->count += 1;
  rehash (depth, key, n);
  return ret;
}

int
merkle_tree_mem::insert (merkle_hash &key)
{
  if (keylist[static_cast<bigint> (key)])
    fatal << "merkle_tree_mem::insert: key already exists " << key << "\n";

  return insert (0, key, get_root());
}

int
merkle_tree_mem::remove (merkle_hash &key)
{
  // assert block must exist..
  str foo;
  if (!keylist[static_cast<bigint> (key)]) {
    warn << (u_int) this << " merkle_tree_mem::remove: key does not exist " << key << "\n";
    return -1; // XXX Use ENOENT?  DB_NOTFOUND?
  }

  return remove (0, key, get_root());
}

vec<merkle_hash>
merkle_tree_mem::database_get_keys (u_int depth, const merkle_hash &prefix)
{
  vec<merkle_hash> ret;
  merkle_key *cur = closestsucc (keylist, static_cast<bigint> (prefix));

  while (cur) {
    merkle_hash key (cur->id);
    if (!prefix_match (depth, key, prefix))
      break;
    ret.push_back (key);
    cur = keylist.next (cur);
  }
  return ret;
}

void
merkle_tree_mem::get_keyrange_nowrap (const chordID &min,
    const chordID &max, u_int n, vec<chordID> &keys)
{
  merkle_key *k = closestsucc (keylist, min);
  for (u_int i = 0; k && keys.size () < n; i++) {
    if (!betweenbothincl (min, max, k->id))
      break;
    keys.push_back (k->id);
    k = keylist.next (k);
  }
}
// }}}

/* vim:set foldmethod=marker: */
