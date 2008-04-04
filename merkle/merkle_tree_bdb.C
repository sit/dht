#include <async.h>
#include <db.h>

#include <chord_types.h>
#include <id_utils.h>
#include <dbfe.h>

#include "merkle_tree_bdb.h"

// {{{ Marshalling functions
inline void
str_to_dbt (const str &s, DBT *d)
{
  bzero (d, sizeof (*d));
  d->size = s.len ();
  d->data = (void *) s.cstr ();
}

inline void
mhash_to_dbt (const merkle_hash &h, DBT *d)
{
  static char buf[sha1::hashsize]; // XXX bug waiting to happen
  // We want big-endian for BTree mapping efficiency
  bzero (d, sizeof (*d));
  bigint i = static_cast<bigint> (h);
  bzero (buf, sizeof (buf)); // XXX unnecessary; handled by rawmag.
  mpz_get_rawmag_be (buf, sizeof (buf), &i);
  d->size = sizeof (buf);
  d->data = (void *) buf;
}

inline void
prefix_to_dbt (u_int depth, const merkle_hash &h, DBT *d)
{
  static char buf[sha1::hashsize + 4]; // XXX bug waiting to happen
  // We want big-endian for BTree mapping efficiency
  bzero (d, sizeof (*d));
  bigint i = static_cast<bigint> (h);
  bzero (buf, sizeof (buf)); // XXX unnecessary; handled by rawmag.
  mpz_get_rawmag_be (buf, sizeof (buf), &i);
  assert (buf[0] == 0); buf[0] = (depth & 0xFF000000) >> 24;
  assert (buf[1] == 0); buf[1] = (depth & 0x00FF0000) >> 16;
  assert (buf[2] == 0); buf[2] = (depth & 0x0000FF00) >>  8;
  assert (buf[3] == 0); buf[3] = (depth & 0x000000FF) >>  0;
  d->size = sizeof (buf);
  d->data = (void *) buf;
}

inline merkle_hash
dbt_to_mhash (const DBT &d)
{
  // Like merkle_hash::merkle_hash (const bigint &id)
  merkle_hash h;
  assert (d.size == h.size);
  bcopy (d.data, h.bytes, d.size);
  for (u_int i = 0; i < (h.size / 2); i++) {
    char tmp = h.bytes[i];
    h.bytes[i] = h.bytes[h.size - 1 - i];
    h.bytes[h.size - 1 - i] = tmp;
  }
  return h;
}
// }}}
// {{{ merkle_node_bdb
// {{{ merkle_node_bdb::operator str () const
// The code in these functions assume that hash.size is constant and global.
merkle_node_bdb::operator str () const
{
  // Count, whether or not a leaf, node's hash, children hashes
  mstr mbuf (marshaled_size);
  size_t outp = 0;
  char *buf = mbuf.cstr ();
  bzero (buf, marshaled_size);

  bcopy (prefix.bytes, buf + outp, prefix.size); outp += prefix.size;

  buf[outp++] = (depth & 0xFF000000) >> 24;
  buf[outp++] = (depth & 0x00FF0000) >> 16;
  buf[outp++] = (depth & 0x0000FF00) >>  8;
  buf[outp++] = (depth & 0x000000FF) >>  0;

  bcopy (hash.bytes, buf + outp, hash.size); outp += hash.size;

  buf[outp++] = 0;
  buf[outp++] = 0;
  buf[outp++] = 0;
  buf[outp++] = (leaf ? 1 : 0);

  buf[outp++] = (count & 0xFF000000) >> 24;
  buf[outp++] = (count & 0x00FF0000) >> 16;
  buf[outp++] = (count & 0x0000FF00) >>  8;
  buf[outp++] = (count & 0x000000FF) >>  0;

  if (!leaf) {
    for (size_t i = 0; i < 64; i++) {
      bcopy (_child_hash[i].bytes, buf + outp, _child_hash[i].size);
      outp += _child_hash[i].size;
    }
    assert (outp == marshaled_size);
  } else {
    mbuf.setlen (outp);
  }

  return mbuf;
}
// }}}
// {{{ merkle_node_bdb::merkle_node_bdb (const char *buf, size_t sz)
merkle_node_bdb::merkle_node_bdb (const unsigned char *buf, size_t sz, merkle_tree_bdb *t) :
  merkle_node (),
  leaf (true),
  tree (t)
{
  if (sz < prefix.size + 12) {
    warn << "XXX Bad merkle_node_bdb (str): len " << sz << " too short.\n";
    return;
  }
  size_t inp = 0;

  bcopy (buf + inp, prefix.bytes, prefix.size); inp += prefix.size;

  depth = (buf[inp + 0] << 24) | (buf[inp + 1] << 16) |
          (buf[inp + 2] <<  8) | (buf[inp + 3]);
  inp += 4;

  bcopy (buf + inp, hash.bytes, hash.size); inp += hash.size;

  inp += 3; // skip zeros;
  leaf = (buf[inp++] > 0);

  count = (buf[inp + 0] << 24) | (buf[inp + 1] << 16) |
	  (buf[inp + 2] <<  8) | (buf[inp + 3]);
  inp += 4;

  if (!leaf) {
    if (sz < marshaled_size) {
      warn << "XXX Bad merkle_node_bdb (buf, sz): " << sz << " too short.\n";
      return;
    }
    for (size_t i = 0; i < 64; i++) {
      bcopy (buf + inp, _child_hash[i].bytes, _child_hash[i].size);
      inp += _child_hash[i].size;
    }
  }
}
// }}}
// {{{ merkle_node_bdb::child
merkle_node *
merkle_node_bdb::child (u_int i)
{
  assert (i < 64);
  assert (!isleaf ());
  merkle_hash cprefix (prefix);
  cprefix.write_slot (depth, i);
  merkle_node_bdb *c = tree->read_node (depth + 1, cprefix);
  // to_delete preserves merkle_tree_disk memory management semantics
  to_delete.push_back (c);
  return c;
}
// }}}
// {{{ merkle_node_bdb::~merkle_node_bdb
merkle_node_bdb::~merkle_node_bdb ()
{
  // also delete all children
  for (size_t i = 0; i < to_delete.size(); i++) {
    delete to_delete[i];
  }
}
// }}}
// {{{ merkle_node_bdb::internal2leaf
int
merkle_node_bdb::internal2leaf (DB_TXN *t)
{
  // warnx << "internal2leaf " << depth << " / " << prefix << "\n";
  sha1ctx sc;
  vec<merkle_hash> keys;
  tree->get_hash_list (keys, depth, prefix, t);

  assert (keys.size () <= 64);
  for (size_t i = 0; i < keys.size (); i++)
    sc.update (keys[i].bytes, keys[i].size);
  sc.final (hash.bytes);

  merkle_hash x (prefix);
  for (u_int32_t i = 0; i < 64; i++) {
    x.write_slot (depth, i);
    int r = tree->del_node (depth + 1, x, t);
    // Whole operation will abort if any error.
    if (r)
      return r;
  }

  leaf = true;
  return tree->write_node (this, t);
}

void
merkle_node_bdb::internal2leaf ()
{
  // Clearly, the merkle_node abstraction needs work.
  assert (0);
}
// }}}
// {{{ merkle_node_bdb::leaf2internal
int
merkle_node_bdb::leaf2internal (DB_TXN *t)
{
  // warnx << "leaf2internal " << depth << " / " << prefix << "\n";
  vec<merkle_hash> keys;
  tree->get_hash_list (keys, depth, prefix, t);

  assert (keys.size () == count);

  merkle_hash x (prefix);
  array<merkle_node_bdb *, 64> newnodes;
  array<sha1ctx, 64> hashes;
  u_int xmax = (depth == merkle_hash::NUM_SLOTS) ? 16 : 64;
  for (size_t i = 0; i < xmax; i++) {
    newnodes[i] = New merkle_node_bdb ();
    newnodes[i]->depth = depth + 1;
    x.write_slot (depth, i);
    newnodes[i]->prefix = x;
    newnodes[i]->tree = tree;
  }
  for (size_t i = 0; i < keys.size (); i++) {
    u_int32_t branch = keys[i].read_slot (depth);
    newnodes[branch]->count++;
    hashes[branch].update (keys[i].bytes, keys[i].size);
  }
  for (size_t i = 0; i < xmax; i++) {
    if (newnodes[i]->count)
      hashes[i].final (newnodes[i]->hash.bytes);
    _child_hash[i] = newnodes[i]->hash;
    int r = tree->write_node (newnodes[i], t);
    delete newnodes[i];
    newnodes[i] = NULL;
    if (r)
      return r;
  }
  leaf = false;
  return tree->write_node (this, t);
}

void
merkle_node_bdb::leaf2internal ()
{
  // Clearly, the merkle_node abstraction needs work.
  assert (0);
}
// }}}
// {{{ merkle_node_bdb::dump
void
merkle_node_bdb::dump (u_int d)
{
  assert (d == depth);
  // XXX
}
// }}}
// }}}
// {{{ merkle_tree_bdb
// {{{ merkle_tree_bdb::merkle_tree_bdb (const char *, bool, bool)
merkle_tree_bdb::merkle_tree_bdb (const char *path, bool join, bool ro) :
  dbe_closable (true),
  dbe (NULL),
  nodedb (NULL),
  keydb (NULL)
{
#define DB_ERRCHECK(desc) \
  if (r) {		  \
    fatal << desc << " returned " << r << ": " << db_strerror (r) << "\n"; \
    return;		  \
  }

  // Should be able to handle a million objects easily in a 5MB cache
  int r = dbfe_initialize_dbenv (&dbe, path, join, 5*1024);
  DB_ERRCHECK ("dbe->open");

  r = init_db (ro);
  DB_ERRCHECK ("init_db");
}
// }}}
// {{{ merkle_tree_bdb:;merkle_tree_bdb (DB_ENV *, bool)
merkle_tree_bdb::merkle_tree_bdb (DB_ENV *parentdbe, bool ro) :
  dbe_closable (false),
  dbe (parentdbe),
  nodedb (NULL),
  keydb (NULL)
{
  int r = init_db (ro);
  DB_ERRCHECK ("init_db");
}
// }}}
// {{{ merkle_tree_bdb::init_db
int
merkle_tree_bdb::init_db (bool ro)
{
  assert (dbe != NULL);
  int r = 0;

  int flags = DB_CREATE;
  if (ro)
    flags = DB_RDONLY;

  DB_TXN *t = NULL;
  r = dbfe_txn_begin (dbe, &t);

  // BTree makes the most sense for a tree structure.
  const char *err = "";
  do {
    err = "nodedb->create";
    r = db_create (&nodedb, dbe, 0);
    if (r) break;

    err = "nodedb->reverse split";
    r = nodedb->set_flags (nodedb, DB_REVSPLITOFF);
    if (r) break;

    err = "nodedb->open";
    r = nodedb->open (nodedb, t, "node.db", NULL, DB_BTREE, flags, 0);
    if (r) break;

    err = "keydb->create";
    r = db_create (&keydb, dbe, 0);
    if (r) break;

    err = "keydb->reverse split";
    r = keydb->set_flags (keydb, DB_REVSPLITOFF);
    if (r) break;

    err = "keydb->open";
    r = keydb->open (keydb, t, "key.db", NULL, DB_BTREE, flags, 0);
    if (r) break;
  } while (0);
  if (r) {
    warnx << "merkle_tree_bdb::init_db: " << err << ": "
          << db_strerror (r) << " (" << r << ")\n";
    dbfe_txn_abort (dbe, t);
    return r;
  } else {
    dbfe_txn_commit (dbe, t);
  }

  r = dbfe_txn_begin (dbe, &t);
  merkle_node_bdb *root = read_node (0, 0, t);
  if (!root) {
    // No old root, make up a new one.
    root = New merkle_node_bdb ();
    root->tree = this;
    err = "root write";
    r = write_node (root, t);
  }
  delete root;
  if (r)
    dbfe_txn_abort (dbe, t);
  else
    dbfe_txn_commit (dbe, t);

  return r;
}
// }}}
// {{{ merkle_tree_bdb::tree_exists
bool
merkle_tree_bdb::tree_exists (const char *path)
{
  struct stat sb;
  if (stat (path, &sb) < 0) {
    // warn << "tree_exists (" << path << "): " << strerror (errno) << "\n";
    return false;
  }
  if (!S_ISDIR (sb.st_mode)) {
    // warn << "tree_exists (" path << "): not a directory\n";
    return false;
  }
  str npath (strbuf ("%s/node.db", path));
  if (stat (npath.cstr (), &sb) < 0) {
    // warn << "tree_exists (" path << "): node.db: " << strerror (errno) << "\n";
    return false;
  }
  str kpath (strbuf ("%s/key.db", path));
  if (stat (kpath.cstr (), &sb) < 0) {
    // warn << "tree_exists (" path << "): key.db: " << strerror (errno) << "\n";
    return false;
  }
  return true;
}
// }}}
// {{{ merkle_tree_bdb::~merkle_tree_bdb
merkle_tree_bdb::~merkle_tree_bdb ()
{
  sync ();
  
#define DBCLOSE(x)			\
  if (x) {				\
    (void) x->close (x, 0); x = NULL;	\
  }
  DBCLOSE(nodedb);
  DBCLOSE(keydb);
  if (dbe_closable) {
    DBCLOSE(dbe);
  }
}
// }}}
// {{{ merkle_tree_bdb::sync
void
merkle_tree_bdb::sync (bool reopen)
{
  // reopen is ignored
#if (DB_VERSION_MAJOR < 4)
  txn_checkpoint (dbe, 30*1024, 10, 0);
#else
  dbe->txn_checkpoint (dbe, 30*1024, 10, 0);
#endif
}
// }}}
// {{{ merkle_tree_bdb::warner
void
merkle_tree_bdb::warner (const char *method, const char *desc, int r) const
{
  timespec ts;
  strbuf t;
  clock_gettime (CLOCK_REALTIME, &ts);
  t.fmt ("%d.%06d: ", int (ts.tv_sec), int (ts.tv_nsec/1000));
  warn << t << method << ": " << desc << ": " << db_strerror (r) << "\n";
}
// }}}
// {{{ merkle_tree_bdb::read_node
merkle_node_bdb *
merkle_tree_bdb::read_node (u_int depth, const merkle_hash &key,
    DB_TXN *t, int flags)
{
  merkle_hash prefix (key);
  prefix.clear_suffix (depth);
  DBT pfx; prefix_to_dbt (depth, prefix, &pfx);
  DBT data; bzero (&data, sizeof (data));
  // warnx << "read_node of " << hexdump (pfx.data, pfx.size) << "\n";

  int r = nodedb->get (nodedb, t, &pfx, &data, flags);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("merkle_tree_bdb::read_node", "nodedb->get", r);
    return NULL;
  }
  merkle_node_bdb *node =
    New merkle_node_bdb (static_cast<unsigned char *> (data.data), data.size, this);
  return node;
}
// }}}
// {{{ merkle_tree_bdb::write_node
int
merkle_tree_bdb::write_node (const merkle_node_bdb *node, DB_TXN *t)
{
  merkle_hash prefix = node->prefix;
  prefix.clear_suffix (node->depth);
  DBT pfx; prefix_to_dbt (node->depth, prefix, &pfx);
  str noderep = *node;
  DBT data; str_to_dbt (noderep, &data);

  // warnx << "write_node of " << hexdump (pfx.data, pfx.size) << "\n";

  int flags = 0;
  if (!t)
    flags = DB_AUTO_COMMIT;

  int r = nodedb->put (nodedb, t, &pfx, &data, flags);
  if (r)
    warner ("merkle_tree_bdb::write_node", "nodedb->put", r);

  return r;
}
// }}}
// {{{ merkle_tree_bdb::del_node
int
merkle_tree_bdb::del_node (u_int depth, const merkle_hash &key, DB_TXN *t)
{
  merkle_hash prefix (key);
  prefix.clear_suffix (depth);
  DBT pfx; prefix_to_dbt (depth, prefix, &pfx);

  // warnx << "del_node of " << hexdump (pfx.data, pfx.size) << "\n";

  int flags = 0;
  if (!t)
    flags = DB_AUTO_COMMIT;
  int r = nodedb->del (nodedb, t, &pfx, flags);
  if (r && r != DB_NOTFOUND)
    warner ("merkle_tree_bdb::del_node", "nodedb->del", r);

  return r;
}
// }}}
// {{{ merkle_tree_bdb::check_key
bool
merkle_tree_bdb::check_key (const merkle_hash &key, DB_TXN *t)
{
  DBT dkey; mhash_to_dbt (key, &dkey);
  DBT data; bzero (&data, sizeof (data)); data.flags = DB_DBT_PARTIAL;

  int r = keydb->get (keydb, t, &dkey, &data, 0);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("merkle_tree_bdb::check_key", "keydb->get", r);
    return false;
  }
  return true;
}
// }}}
// {{{ merkle_tree_bdb::insert_key
int
merkle_tree_bdb::insert_key (const merkle_hash &key, DB_TXN *t)
{
  DBT dkey; mhash_to_dbt (key, &dkey);
  DBT data; bzero (&data, sizeof (data));
  int flags = DB_NOOVERWRITE;
  if (!t)
    flags |= DB_AUTO_COMMIT;

  int r = keydb->put (keydb, t, &dkey, &data, flags);
  if (r) {
    if (r == DB_KEYEXIST)
      warner ("merkle_tree_bdb::insert_key", "keydb->put", r);
  }
  return r;
}
// }}}
// {{{ merkle_tree_bdb::remove_key
int
merkle_tree_bdb::remove_key (const merkle_hash &key, DB_TXN *t)
{
  DBT dkey; mhash_to_dbt (key, &dkey);
  int flags = 0;
  if (!t)
    flags = DB_AUTO_COMMIT;
  int r = keydb->del (keydb, t, &dkey, flags);
  if (r && r != DB_NOTFOUND)
    warner ("merkle_tree_bdb::remove_key", "keydb->del", r);
  return r;
}
// }}}
// {{{ merkle_tree_bdb::get_root
merkle_node *
merkle_tree_bdb::get_root ()
{
  static merkle_hash h (0);
  merkle_node_bdb *node = read_node (0, h);
  return node;
}
// }}}
// {{{ merkle_tree_bdb::insert
// Duplicates code more or less from merkle_tree.C
int
merkle_tree_bdb::insert (const chordID &id, DB_TXN *t)
{
  merkle_hash mkey (id);
  return insert (mkey, t);
}

int
merkle_tree_bdb::insert (const chordID &id, const u_int32_t aux, DB_TXN *t)
{
  // When there is auxiliary information, we use the low-order bytes
  // of the key to hold the information.  This is used mostly for mutable
  // data that needs to distinguish whether or not the remote node has
  // the same version as the local node.
  chordID key = id;
  key >>= 32;
  key <<= 32;
  assert (key > 0);
  key |= aux;
  merkle_hash mkey (key);
  return insert (mkey, t);
}

int
merkle_tree_bdb::insert (merkle_hash &key, DB_TXN *parent)
{
  // Run this (potentially) in a nested transaction
  DB_TXN *t = NULL;
  dbe->txn_begin (dbe, parent, &t, 0);

  merkle_hash last_h;
  // Find the nodes that will need to be rehashed.
  vec<merkle_node_bdb *> nodes;
  u_int depth = 0;
  merkle_node_bdb *n = read_node (depth, key, t, DB_RMW);
  while (n != NULL) {
    nodes.push_back (n);
    depth++;
    n = read_node (depth, key, t, DB_RMW);
  }
  n = nodes.back ();
  if (!n->isleaf ()) {
    dbfe_txn_abort (dbe, t);
    fatal << "merkle_tree_bdb::insert: bottom of tree is not a leaf?\n";
  }
  int r = 0;
  while (n->leaf_is_full ()) {
    r = n->leaf2internal (t); // Creates all the children
    if (r)
      goto insert_cleanup;
    n = read_node (n->depth + 1, key, t, DB_RMW);
    nodes.push_back (n);
    assert (n->isleaf ());
  }
  // n is a leaf with enough room for another key

  r = insert_key (key, t);
  if (r) {
    if (r != DB_KEYEXIST && r != ENOSPC)
      fatal << "merkle_tree_bdb::insert: unexpected error.\n";
    goto insert_cleanup;
  }

  // Rehash this path and return to disk.
  while (nodes.size ()) {
    n = nodes.pop_back ();
    assert (n->depth == nodes.size ());
    n->count += 1;
    sha1ctx sc;
    if (n->isleaf ()) {
      assert (n->count > 0 && n->count <= 64);
      vec<merkle_hash> keys;
      get_hash_list (keys, n->depth, n->prefix, t);
      for (u_int i = 0; i < keys.size (); i++)
	sc.update (keys[i].bytes, keys[i].size);
    } else {
      u_int32_t branch = key.read_slot (n->depth);
      n->_child_hash[branch] = last_h;
      for (u_int i = 0; i < 64; i++)
	sc.update (n->_child_hash[i].bytes, n->_child_hash[i].size);
    }
    sc.final (n->hash.bytes);
    last_h = n->hash;

    r = write_node (n, t);
    delete n;
    if (r)
      goto insert_cleanup;
  }
  assert (!r);
  dbfe_txn_commit (dbe, t);
  return r;

insert_cleanup:
  while (nodes.size ())
    delete nodes.pop_back ();
  dbfe_txn_abort (dbe, t);
  return r;
}
// }}}
// {{{ merkle_tree_bdb::remove
int
merkle_tree_bdb::remove (const chordID &id, DB_TXN *t)
{
  merkle_hash mkey (id);
  return remove (mkey, t);
}

int
merkle_tree_bdb::remove (const chordID &id, const u_int32_t aux, DB_TXN *t)
{
  chordID key = id;
  key >>= 32;
  key <<= 32;
  assert (key > 0);
  key |= aux;
  merkle_hash mkey (key);
  return remove (mkey, t);
}
int
merkle_tree_bdb::remove (merkle_hash &key, DB_TXN *parent)
{
  DB_TXN *t = NULL;
  dbe->txn_begin (dbe, parent, &t, 0);

  int r = remove_key (key, t);
  if (r) {
    dbfe_txn_abort (dbe, t);
    return r;
  }

  // Find the nodes that will need to be rehashed.
  vec<merkle_node_bdb *> nodes;
  u_int depth = 0;
  merkle_node_bdb *n = read_node (depth, key, t, DB_RMW);
  while (n != NULL) {
    nodes.push_back (n);
    depth++;
    n = read_node (depth, key, t, DB_RMW);
  }
  n = nodes.back ();
  if (!n->isleaf ()) {
    dbfe_txn_abort (dbe, t);
    fatal << "merkle_tree_bdb::remove: bottom of tree is not a leaf?\n";
  }

  // Rehash this path and return to disk.
  r = 0;
  merkle_hash last_h;
  while (nodes.size ()) {
    n = nodes.pop_back ();
    assert (n->depth == nodes.size ());
    assert (n->count != 0);
    n->count -= 1;
    if (!n->isleaf () && n->count <= 64) {
      r = n->internal2leaf (t);
    }
    if (r) {
      delete n;
      goto remove_cleanup;
    }

    sha1ctx sc;
    if (n->isleaf ()) {
      if (n->count == 0) {
	n->hash = merkle_hash (0);
      } else {
	assert (n->count > 0 && n->count <= 64);
	vec<merkle_hash> keys;
	get_hash_list (keys, n->depth, n->prefix, t);
	assert (keys.size () == n->count);
	for (u_int i = 0; i < keys.size (); i++)
	  sc.update (keys[i].bytes, keys[i].size);
	sc.final (n->hash.bytes);
      }
    } else {
      u_int32_t branch = key.read_slot (n->depth);
      n->_child_hash[branch] = last_h;
      for (u_int i = 0; i < 64; i++)
	sc.update (n->_child_hash[i].bytes, n->_child_hash[i].size);
      sc.final (n->hash.bytes);
    }
    last_h = n->hash;

    r = write_node (n, t);
    delete n;
    if (r)
      goto remove_cleanup;
  }
  assert (!r);
  dbfe_txn_commit (dbe, t);
  return r;
remove_cleanup:
  while (nodes.size ())
    delete nodes.pop_back ();
  dbfe_txn_abort (dbe, t);
  return r;
}
// }}}
// {{{ merkle_tree_bdb::key_exists
bool
merkle_tree_bdb::key_exists (chordID key)
{
  return check_key (key);
}
// }}}
// {{{ merkle_tree_bdb::get_hash_list
int
merkle_tree_bdb::get_hash_list (vec<merkle_hash> &keys,
      u_int depth, const merkle_hash &prefix, DB_TXN *t)
{
  merkle_hash h = prefix;
  h.clear_suffix (depth);
  DBT key; mhash_to_dbt (h, &key);
  DBT content; bzero (&content, sizeof (content)); // Irrelevant

  DBC *cursor;
  int r = keydb->cursor (keydb, t, &cursor, 0);
  if (r) {
    warner ("merkle_tree_bdb::get_hash_list", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }
  r = cursor->c_get (cursor, &key, &content, DB_SET_RANGE);
  while (!r) {
    merkle_hash h = dbt_to_mhash (key);
    if (!prefix_match (depth, prefix, h))
      break;
    keys.push_back (h);
    bzero (&key, sizeof (key));
    bzero (&content, sizeof (content));
    r = cursor->c_get (cursor, &key, &content, DB_NEXT);
  }
  if (r && r != DB_NOTFOUND)
    warner ("merkle_tree_bdb::get_hash_list", "cursor c_get", r);
  (void) cursor->c_close (cursor);
  return r;
}
// }}}
// {{{ merkle_tree_bdb::database_get_keys
vec<merkle_hash>
merkle_tree_bdb::database_get_keys (u_int depth, const merkle_hash &prefix)
{
  DB_TXN *t;
  dbfe_txn_begin (dbe, &t);
  vec<merkle_hash> keys;
  get_hash_list (keys, depth, prefix, t);
  dbfe_txn_commit (dbe, t);
  return keys;
}
// }}}
// {{{ merkle_tree_bdb::get_keyrange_nowrap
void
merkle_tree_bdb::get_keyrange_nowrap (const chordID &min,
    const chordID &max, u_int n, vec<chordID> &keys)
{
  merkle_hash h (min);
  DBT key; mhash_to_dbt (h, &key);
  DBT content; bzero (&content, sizeof (content)); // Irrelevant

  // This cursor is not transaction protected:
  // get_keyrange is typically used for key exchange
  // in merkle_server, or debugging in merkledump.
  DBC *cursor;
  int r = keydb->cursor (keydb, NULL, &cursor, 0);
  if (r) {
    warner ("merkle_tree_bdb::get_keyrange", "cursor open", r);
    (void) cursor->c_close (cursor);
    return;
  }
  r = cursor->c_get (cursor, &key, &content, DB_SET_RANGE);
  while (!r && keys.size () < n) {
    merkle_hash h = dbt_to_mhash (key);
    chordID c = static_cast<bigint> (h);
    if (!betweenbothincl (min, max, c))
      break;
    keys.push_back (c);
    bzero (&key, sizeof (key));
    bzero (&content, sizeof (content));
    r = cursor->c_get (cursor, &key, &content, DB_NEXT);
  }
  if (r && r != DB_NOTFOUND)
    warner ("merkle_tree_bdb::get_keyrange", "cursor c_get", r);
  (void) cursor->c_close (cursor);
}
// }}}
// {{{ merkle_tree_bdb::lookup_exact
merkle_node *
merkle_tree_bdb::lookup_exact (u_int depth, const merkle_hash &key)
{
  return read_node (depth, key);
}
// }}}
// {{{ merkle_tree_bdb::lookup (no max depth)
merkle_node *
merkle_tree_bdb::lookup (u_int depth, const merkle_hash &key)
{
  return lookup (&depth, MAX_DEPTH, key);
}
// }}}
// {{{ merkle_tree_bdb::lookup (max depth)
merkle_node *
merkle_tree_bdb::lookup (u_int *depth, u_int max_depth, const merkle_hash &key)
{
  DB_TXN *t = NULL;
  dbfe_txn_begin (dbe, &t);
  merkle_node_bdb *n = NULL;
  // Start at the deepest allowed position and search upwards.
  for (*depth = max_depth; *depth >= 0; (*depth)--) {
    n = read_node (*depth, key, t);
    if (n || !*depth)
      break;
  }
  dbfe_txn_commit (dbe, t);
  return n;
}
// }}}
// {{{ merkle_tree_bdb::lookup_release
void
merkle_tree_bdb::lookup_release (merkle_node *n)
{
  delete n;
}
// }}}
// {{{ merkle_tree_bdb::check_invariants
void
merkle_tree_bdb::check_invariants ()
{
  // Must override merkle_tree's implementation to place whole
  // check in a transaction and to ensure that extra invariants hold.
  DB_TXN *t = NULL;
  dbfe_txn_begin (dbe, &t);
  merkle_node_bdb *root = read_node (0, 0, t);
  verify_subtree (root, t);
  delete root;
  dbfe_txn_commit (dbe, t);
}

void
merkle_tree_bdb::verify_subtree (merkle_node_bdb *n, DB_TXN *t)
{
  u_int64_t ncount (0);
  sha1ctx sc;
  if (!n->isleaf ()) {
    merkle_hash nprefix (n->prefix);
    for (int i = 0; i < 64; i++) {
      nprefix.write_slot (n->depth, i);
      merkle_node_bdb *child = read_node (n->depth + 1, nprefix, t);
      ncount += child->count;
      verify_subtree (child, t);
      if (n->_child_hash[i] != child->hash) {
	fatal << "merkle_tree: child hash mismatch "
	      << n->_child_hash[i] << " vs " << child->hash
	      << " at "
	      << (n->isleaf () ? "leaf" : "non-leaf")
	      << " at depth " << n->depth << " and prefix "
	      << n->prefix << "\n";
      }
      sc.update (child->hash.bytes, child->hash.size);
      delete child;
    }
  } else {
    vec<merkle_hash> keys;
    get_hash_list (keys, n->depth, n->prefix, t);
    ncount = keys.size ();
    assert (n->count <= 64);
    for (u_int i = 0; i < keys.size (); i++) {
      sc.update (keys[i].bytes, keys[i].size);
    }
  }
  if (ncount != n->count) {
    fatal << "merkle_tree: counted " << ncount
          << " children but had recorded " << n->count << " at "
          << (n->isleaf () ? "leaf" : "non-leaf")
	  << " at depth " << n->depth << " and prefix "
          << n->prefix << "\n";
  }

  merkle_hash nhash;
  if (n->count)
    sc.final (nhash.bytes);
  if (nhash != n->hash) {
    warn << "nhash   = " << nhash << "\n";
    warn << "n->hash = " << n->hash << "\n";
    warn << "n->count= " << n->count << "\n";
    fatal << "nhash of "
	  << (n->isleaf () ? "leaf" : "non-leaf")
	  << " didn't match at depth " << n->depth << " and prefix "
	  << n->prefix << "\n";
  }
}
// }}}
// }}}

/* vim:set foldmethod=marker: */
