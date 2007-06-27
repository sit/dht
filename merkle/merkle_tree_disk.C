#include <chord_types.h>
#include <id_utils.h>
#include <rxx.h>
#include "merkle_tree_disk.h"
#include "dhash_common.h"

static FILE *
open_file (str name) {
  // make all the parent directories if applicable
  vec<str> dirs;
  static const rxx dirsplit("\\/");
  (void) split (&dirs, dirsplit, name);
  strbuf filestrbuf;
  for (uint i = 0; i < dirs.size()-1; i++) {
    filestrbuf << dirs[i] << "/";
    
    str dirpath (filestrbuf);
    // check for existence, make if necessary
    struct stat st;
    if (stat (dirpath, &st) < 0) {
      if (mkdir(dirpath, S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) != 0) {
	fatal << "open_file: mkdir (" << dirpath << "): " << strerror (errno) << ".\n";
      }
    }
  }

  FILE *f = fopen (name, "r+");
  if (f == NULL) {
    f = fopen (name, "w+");
  }
  if (f == NULL) {
    fatal << "open_file: fopen (" << name << "): " << strerror (errno) << ".\n"; 
  }
  return f;
}

static int
copy_file (str src, str dst)
{
  int sfd = open (src, O_RDONLY);
  if (sfd < 0) {
    perror ("copy_file: src");
    return sfd;
  }

  str tmpfile = dst << "~";
  unlink (tmpfile);
  int dfd = open (tmpfile, O_CREAT|O_WRONLY, 0644);
  if (dfd < 0) {
    perror ("copy_file: dst");
    close (sfd);
    return dfd;
  }
#define COPY_ERROR(msg) \
  do {						  \
    int oerrno = errno;				  \
    warn ("copy_file: " msg ": %m");		  \
    close (sfd); close (dfd); unlink (tmpfile);	  \
    errno = oerrno;				  \
    return -1;					  \
  } while (0)

  char buf[8192];
  ssize_t nread;
  while ((nread = read (sfd, buf, sizeof (buf))) > 0) {
    ssize_t nwrote = write (dfd, buf, nread);
    if (nwrote < 0) {
      COPY_ERROR ("bad write");
    }
    if (nwrote != nread) {
      COPY_ERROR ("short write");
    }
  }
  if (nread < 0) {
    COPY_ERROR ("read error");
  }
  close (sfd); close (dfd);
  rename (tmpfile, dst);
#undef COPY_ERROR
  return 0;
}

//////////////// merkle_node_disk /////////////////

merkle_node_disk::merkle_node_disk (FILE *internal, FILE *leaf, 
				    MERKLE_DISK_TYPE type, u_int32_t block_no) :
  merkle_node (),
  hashes (NULL),
  children (NULL),
  _internal (internal),
  _leaf (leaf), 
  _type (type),
  _block_no (block_no)
{
  if (isleaf()) {
    merkle_leaf_node leaf;
    int seekval = fseek (_leaf, _block_no*sizeof (merkle_leaf_node), SEEK_SET);
    assert (seekval == 0);
    fread (&leaf, sizeof (merkle_leaf_node), 1, _leaf);

    count = ntohl(leaf.key_count);
    for (uint i = 0; i < count; i++) {
      chordID c;
      mpz_set_rawmag_be (&c, leaf.keys[i].key, sizeof (leaf.keys[i].key));
      keylist.insert (New merkle_key (c));
    }
  } else {
    children = New array<u_int32_t, 64> ();
    hashes = New array<merkle_hash_id, 64> ();

    merkle_internal_node internal;
    int seekval = fseek (_internal, _block_no*sizeof (merkle_internal_node), 
			 SEEK_SET);
    assert (seekval == 0);
    fread (&internal, sizeof (merkle_internal_node), 1, _internal);

    count = ntohl(internal.key_count);
    for (uint i = 0; i < 64; i++) {
      mpz_set_rawmag_be (&((*hashes)[i].id), internal.hashes[i].key, 
			 sizeof(internal.hashes[i].key));
      (*hashes)[i].hash = merkle_hash ((*hashes)[i].id);
      (*children)[i] = ntohl (internal.child_pointers[i]);
    }
  }

  rehash ();
}

merkle_node_disk::~merkle_node_disk () 
{
  keylist.deleteall_correct();
  delete children;
  delete hashes;

  // also delete all children
  for (uint i = 0; i < to_delete.size(); i++) {
    delete to_delete[i];
  }

  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
}

void merkle_node_disk::rehash ()
{
  hash = 0;
  if (count == 0) {
    return;
  }
  
  sha1ctx sc;
  if (isleaf ()) {
    assert (count > 0 && count <= 64);
    merkle_key *k = keylist.first();
    while (k != NULL) {
      merkle_hash h (k->id);
      sc.update (h.bytes, h.size);
      k = keylist.next(k);
    }
  } else {
    for (uint i = 0; i < 64; i++) {
      merkle_hash h = (*hashes)[i].hash;
      sc.update (h.bytes, h.size);
    }
  }
  sc.final (hash.bytes);
}

void
merkle_node_disk::write_out ()
{
  if (isleaf ()) {
    merkle_leaf_node leaf;
    bzero (&leaf, sizeof (leaf));
    leaf.key_count = htonl (count);
    merkle_key *m = keylist.first();
    int i = 0;
    while (m != NULL) {
      mpz_get_rawmag_be (leaf.keys[i].key, sizeof(leaf.keys[i].key), &(m->id));
      m = keylist.next (m);
      i++;
    }

    int seekval = fseek (_leaf, _block_no*sizeof(merkle_leaf_node), SEEK_SET);
    assert (seekval == 0);
    fwrite (&leaf, sizeof(merkle_leaf_node), 1, _leaf);
  } else {
    merkle_internal_node internal;
    internal.key_count = htonl(count);
    for (uint i = 0; i < 64; i++) {
      mpz_get_rawmag_be (internal.hashes[i].key, 
			 sizeof (internal.hashes[i].key), &((*hashes)[i].id));
      internal.child_pointers[i] = htonl ((*children)[i]);
      //      warn << _block_no << ") writing out child " << i << ") " << ((*children)[i] >> 1) << "\n";
    }

    int seekval = fseek (_internal, _block_no*sizeof(merkle_internal_node), 
			 SEEK_SET);
    assert (seekval == 0);
    fwrite (&internal, sizeof(merkle_internal_node), 1, _internal);
  }
}

merkle_node *
merkle_node_disk::child (u_int i)
{
  assert (!isleaf ());
  assert (i >= 0 && i < 64);

  u_int32_t pointer = (*children)[i];
  MERKLE_DISK_TYPE type;
  if (pointer % 2 == 0) {
    type = MERKLE_DISK_INTERNAL;
  } else {
    type = MERKLE_DISK_LEAF;
  }
  merkle_node_disk *n = New merkle_node_disk (_internal, _leaf, type, 
					      pointer >> 1);
  to_delete.push_back (n);
  return n;
}

merkle_hash
merkle_node_disk::child_hash (u_int i)
{
  assert (!isleaf ());
  assert (i >= 0 && i < 64);
  return (*hashes)[i].hash;
}

u_int32_t
merkle_node_disk::child_ptr (u_int i)
{
  assert (!isleaf ());
  assert (i >= 0 && i < 64);
  return (*children)[i];
}

void
merkle_node_disk::set_child (merkle_node_disk *n, u_int i)
{
  assert (!isleaf ());
  assert (i >= 0 && i < 64);

  (*children)[i] = ((n->get_block_no() << 1) | (n->isleaf()?0x00000001:0));
  (*hashes)[i].hash = n->hash;
  (*hashes)[i].id = static_cast<bigint> (n->hash);
}

void
merkle_node_disk::add_key (chordID key)
{
  assert (isleaf () && count < 64);
  count++;
  merkle_key *m = New merkle_key (key);
  keylist.insert (m);
}

void
merkle_node_disk::add_key (merkle_hash key)
{
  assert (isleaf () && count < 64);
  count++;
  merkle_key *m = New merkle_key(key);
  keylist.insert(m);
}

void
merkle_node_disk::internal2leaf ()
{
  assert (!isleaf ());
  _type = MERKLE_DISK_LEAF;
  delete children;
  children = NULL;
  delete hashes;
  hashes = NULL;
}

bool
merkle_node_disk::isleaf () const {
  return (_type == MERKLE_DISK_LEAF);
}

void merkle_node_disk::leaf2internal () {
  assert (isleaf ());
  _type = MERKLE_DISK_INTERNAL;
  children = New array<u_int32_t, 64>();
  hashes = New array<merkle_hash_id, 64>();
  keylist.deleteall_correct();
  assert (!isleaf ());
}

static void
indent (u_int depth)
{
  while (depth-- > 0)
    warnx << " ";
}

void
merkle_node_disk::dump (u_int depth)
{
  warnx << "[NODE "
	<< strbuf ("0x%x", (u_int)this)
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

//////////////// merkle_tree_disk /////////////////

static inline str
safe_fname (str rwname)
{
  strbuf roname ("%s.ro", rwname.cstr ());
  return roname;
}

void
merkle_tree_disk::init ()
{
  if (_writer) {
    _internal = open_file (_internal_name);
    _leaf = open_file (_leaf_name);
    _index = open_file (_index_name);
    // make sure the root is created correctly:
    merkle_node *n = get_root ();
    delete n;
    write_metadata ();
  } else {
    _internal = open_file (safe_fname (_internal_name));
    _leaf = open_file (safe_fname (_leaf_name));
    _index = open_file (safe_fname (_index_name));
  }
}

void
merkle_tree_disk::close ()
{
  if (_internal) { fclose (_internal); _internal = NULL; }
  if (_leaf) { fclose (_leaf); _leaf = NULL; }
  if (_index) { fclose (_index); _index = NULL; }
}

void
merkle_tree_disk::sync (bool reopen = true)
{
  // Use lock to ensure that all copies are completed safely.
  strbuf lfname ("%s.lock", _index_name.cstr ());
  ptr<lockfile> lf = lockfile::alloc (lfname, true);

  close ();
  if (_writer) {
    // Block, copy current files to safe images.
    int r = 0;
    r = copy_file (_index_name, safe_fname (_index_name));
    r = copy_file (_leaf_name, safe_fname (_leaf_name));
    r = copy_file (_internal_name, safe_fname (_internal_name));
    // XXX Check the exit code; roll-back on failure.
  }
  if (reopen)
    init ();

  lf = NULL;
}

merkle_tree_disk::merkle_tree_disk (str path, bool writer) :
  merkle_tree (),
  _index_name (strbuf () << path << "/index.mrk"),
  _internal_name (strbuf () << path << "/internal.mrk"),
  _leaf_name (strbuf () << path << "/leaf.mrk"),
  _writer (writer)
{
  init ();
}

merkle_tree_disk::merkle_tree_disk (str index, str internal, str leaf,
				    bool writer) :
  merkle_tree (),
  _index_name (index), 
  _internal_name (internal),
  _leaf_name (leaf), 
  _writer (writer)
{
  init ();
}

merkle_tree_disk::~merkle_tree_disk ()
{
  if (_writer)
    sync (false);
  close ();
}

void
merkle_tree_disk::write_metadata ()
{
  assert (_writer);

  fseek (_index, 0, SEEK_SET);
  
  assert (_free_leafs.size () == _md.num_leaf_free);
  assert (_free_internals.size () == _md.num_internal_free);

  _md.num_leaf_free += _future_free_leafs.size ();
  _md.num_internal_free += _future_free_internals.size ();

  fwrite (&_md, sizeof(merkle_index_metadata), 1, _index);

  int nfree = _md.num_leaf_free + _md.num_internal_free;
  u_int32_t freelist[nfree];

  // write out both the unused free slots and the newly created free slots
  uint i;
  uint sofar = 0;
  for (i = 0; i < _free_leafs.size(); i++) {
    freelist[i] = ((_free_leafs[i-sofar] << 1) | 0x00000001);
  }
  sofar = i;
  for (; (i-sofar) < _free_internals.size(); i++) {
    freelist[i] = (_free_internals[i-sofar] << 1);
  }
  sofar = i;
  for (; (i-sofar) < _future_free_leafs.size(); i++) {
    freelist[i] = ((_future_free_leafs[i-sofar] << 1) | 0x00000001);
  }
  sofar = i;
  for (; (i-sofar) < _future_free_internals.size(); i++) {
    freelist[i] = (_future_free_internals[i-sofar] << 1);
  }

  fwrite (&freelist, sizeof (u_int32_t), nfree, _index);
  _future_free_leafs.clear ();
  _future_free_internals.clear ();
}

merkle_node *merkle_tree_disk::get_root ()
{
  fseek (_index, 0, SEEK_SET);

  // figure out where the root node is, given the index file
  // the first 32 bytes of the file tells us where it is
  int num_read = fread (&_md, sizeof (merkle_index_metadata), 1, _index);

  if (num_read <= 0) {
    // no root pointer yet, so we have a new tree
    _md.root = 1;
    _md.num_leaf_free = 0;
    _md.num_internal_free = 0;
    _md.next_leaf = 1;
    _md.next_internal = 0;

    // also, make a block there
    merkle_leaf_node new_root;
    bzero (&new_root, sizeof (merkle_leaf_node));
    fseek (_leaf, 0, SEEK_SET);
    fwrite (&new_root, sizeof (merkle_leaf_node), 1, _leaf);
  } else if (_writer) {
    _free_leafs.clear ();
    _free_internals.clear ();

    // read in the free list
    int nfree = _md.num_leaf_free+_md.num_internal_free;
    u_int32_t freelist[nfree];
    int nread = fread (&freelist, sizeof (u_int32_t), nfree, _index);
    assert (nread == nfree);

    for (int i = 0; i < nread; i++) {
      u_int32_t pointer = freelist[i];
      if (pointer % 2 == 0) {
	_free_internals.push_back (pointer >> 1);
      } else {
	_free_leafs.push_back (pointer >> 1);
      }
    }
  }

  return make_node (_md.root);
}

merkle_node *
merkle_tree_disk::make_node (u_int32_t block_no, MERKLE_DISK_TYPE type)
{
  return New merkle_node_disk (_internal, _leaf, type, block_no);
}

merkle_node *
merkle_tree_disk::make_node (u_int32_t pointer)
{
  // even == internal, odd == leaf
  if (pointer % 2 == 0) {
    return make_node (pointer >> 1, MERKLE_DISK_INTERNAL);
  } else {
    return make_node (pointer >> 1, MERKLE_DISK_LEAF);
  }
}

// returns a block_no without the type bit on the end
void
merkle_tree_disk::free_block (u_int32_t block_no, MERKLE_DISK_TYPE type)
{
  assert (_writer);

  // don't want these blocks being used until the root gets switched over
  // in write_metadata, so keep them on a separate list
  if (type == MERKLE_DISK_LEAF) {
    _future_free_leafs.push_back (block_no);
  } else {
    _future_free_internals.push_back (block_no);
  }
}

// returns a block_no without the type bit on the end
u_int32_t
merkle_tree_disk::alloc_free_block (MERKLE_DISK_TYPE type)
{
  assert (_writer);

  u_int32_t ret;
  // if there are any free
  if (type == MERKLE_DISK_LEAF) {
    if (_md.num_leaf_free > 0) {
      ret = _free_leafs.pop_front ();
      _md.num_leaf_free--;
    } else {
      ret = _md.next_leaf;
      _md.next_leaf++; 
    }
  } else {
    if (_md.num_internal_free > 0) {
      ret = _free_internals.pop_front ();
      _md.num_internal_free--;
    } else {
      ret = _md.next_internal;
      _md.next_internal++; 
    }
  }

  return ret;
}

void
merkle_tree_disk::switch_root (merkle_node_disk *n)
{
  assert (_writer);
  u_int32_t newroot = n->get_block_no ();
  newroot <<= 1;
  if (n->isleaf()) {
    newroot |= 0x00000001;
  }
  _md.root = newroot;
  // NOTE: there should only be one thread on the machine that is
  // inserting or removing blocks and writing metadata.  Any others
  // need to be readonly
  write_metadata ();
}

int
merkle_tree_disk::remove (u_int depth, merkle_hash& key, 
			  merkle_node *n)
{
  assert (_writer);

  merkle_node_disk *nd = (merkle_node_disk *) n;
  MERKLE_DISK_TYPE old_type;

  if (n->isleaf ()) {
    chordID k = static_cast<bigint> (key);
    merkle_key *mkey = nd->keylist[k];
    assert(mkey);
    nd->keylist.remove(mkey);
    delete mkey;
    old_type = MERKLE_DISK_LEAF;
  } else {
    u_int32_t branch = key.read_slot(depth);
    merkle_node *child = n->child (branch);
    remove (depth+1, key, child);
    nd->set_child ((merkle_node_disk *) child, branch);
    old_type = MERKLE_DISK_INTERNAL;
  }

  MERKLE_DISK_TYPE new_type = old_type;

  n->count--;
  if (!n->isleaf () && n->count <= 64) {
    // free all the children blocks and copy their keys
    uint added = 0;
    for (uint i = 0; i < 64; i++) {
      merkle_node_disk *child = (merkle_node_disk *) nd->child(i);
      free_block (child->get_block_no(), MERKLE_DISK_LEAF);
      assert (child->isleaf ());
      merkle_key *k = child->keylist.first ();
      uint j = 0;
      while (k != NULL) {
	merkle_key *knew = New merkle_key (k->id);
	nd->keylist.insert (knew);
	added++;
	j++;
	k = child->keylist.next (k);
      }
      assert (j == child->count);
      child->keylist.clear ();
    }
    assert (added == n->count);

    // this will copy the keys around
    n->internal2leaf();
    assert (n->isleaf ());
    new_type = MERKLE_DISK_LEAF;
  }

  nd->rehash ();

  // write this block out to a new place on disk, then switch the root
  // atomically
  u_int32_t block_no = alloc_free_block (new_type);
  free_block (nd->get_block_no(), old_type);
  nd->set_block_no (block_no);
  nd->write_out ();

  return 0;
}

int
merkle_tree_disk::remove (merkle_hash &key)
{
  merkle_node_disk *curr_root = (merkle_node_disk *) get_root();
  int r = remove (0, key, curr_root);
  switch_root (curr_root);
  delete curr_root;
  return r;
}

void
merkle_tree_disk::leaf2internal (uint depth, merkle_node_disk *n)
{
  assert (n->isleaf () && n->count == 64);
  // NOTE: bottom depth may only have 16??
  merkle_key *k = n->keylist.first ();
  array< vec<chordID>, 64> keys;
  uint added = 0;
  while (k != NULL) {
    merkle_hash h (k->id);
    u_int32_t branch = h.read_slot (depth);
    //warn << "picked branch " << branch << " for key " << k->id << ", hash " 
    // << h << "\n";
    keys[branch].push_back(k->id);
    k = n->keylist.next(k);
    added++;
  }
  assert (added == n->count);

  n->leaf2internal ();
  assert (!n->isleaf ());

  added = 0;
  for (uint i = 0; i < 64; i++) {
    // zero the new guy out
    uint block_no = alloc_free_block (MERKLE_DISK_LEAF);
    merkle_leaf_node new_leaf;
    bzero (&new_leaf, sizeof (merkle_leaf_node));
    fseek (_leaf, block_no*sizeof (merkle_leaf_node), SEEK_SET);
    fwrite (&new_leaf, sizeof (merkle_leaf_node), 1, _leaf);

    merkle_node_disk *child = 
      (merkle_node_disk *) make_node (block_no, MERKLE_DISK_LEAF);

    vec<chordID> v = keys[i];
    for (uint j = 0; j < v.size(); j++) {
      child->add_key (v[j]);
    }
    added += child->count;
    child->rehash ();
    n->set_child (child, i);
    child->write_out ();
    delete child;
  }

  assert (added == n->count);
}

int
merkle_tree_disk::insert (u_int depth, merkle_hash& key, merkle_node *n)
{
  int ret = 0;
  merkle_node_disk *nd = (merkle_node_disk *) n;

  MERKLE_DISK_TYPE old_type;
  if (n->isleaf ()) {
    old_type = MERKLE_DISK_LEAF;
    if (n->leaf_is_full ()) {
      leaf2internal (depth, nd);
      assert (!n->isleaf());
    }
  } else {
    old_type = MERKLE_DISK_INTERNAL;
  } 

  MERKLE_DISK_TYPE type;
  if (n->isleaf ()) {
    type = MERKLE_DISK_LEAF;
    nd->add_key (key);
  } else {
    type = MERKLE_DISK_INTERNAL;
    u_int32_t branch = key.read_slot (depth);
    merkle_node *child = n->child (branch);
    //warn << "inserting " << key << " into child " 
    //	 << ((merkle_node_disk *) child)->get_block_no () << " on branch " 
    // 	 << branch << " at depth " << depth << " and leaf " 
    //	 << child->isleaf() << " with count " << child->count << "\n";
    ret = insert (depth+1, key, child);
    nd->set_child ((merkle_node_disk *) child, branch);
    n->count++;
  }

  nd->rehash ();
  
  // write this block out to a new place on disk, then switch the root
  // atomically
  u_int32_t block_no = alloc_free_block(type);
  free_block (nd->get_block_no(), old_type);
  nd->set_block_no (block_no);
  nd->write_out ();

  return ret;
}

int
merkle_tree_disk::insert (merkle_hash &key)
{
  assert (_writer);
  merkle_node_disk *curr_root = (merkle_node_disk *) get_root();
  int ret = insert (0, key, curr_root);
  switch_root (curr_root);
  delete curr_root;
  return ret;
}

void
merkle_tree_disk::lookup_release (merkle_node *n)
{
  delete n;
}

merkle_node *
merkle_tree_disk::lookup (u_int *depth, u_int max_depth,
			  const merkle_hash &key)
{
  *depth = 0;
  merkle_node *curr_root = get_root ();
  merkle_node *ret = merkle_tree::lookup (depth, max_depth, key, 
					  curr_root);
  ret = make_node(((merkle_node_disk *) ret)->get_block_no (),
		  ret->isleaf () ? MERKLE_DISK_LEAF : MERKLE_DISK_INTERNAL);
  delete curr_root;
  return ret;
}

merkle_node *
merkle_tree_disk::lookup (u_int depth, const merkle_hash &key)
{
  u_int depth_ignore = 0;
  merkle_node *curr_root = get_root ();
  merkle_node *ret = merkle_tree::lookup (&depth_ignore, depth, key, 
					  curr_root);
  ret = make_node (((merkle_node_disk *) ret)->get_block_no(),
		   ret->isleaf() ? MERKLE_DISK_LEAF : MERKLE_DISK_INTERNAL);
  delete curr_root;
  return ret;
}

merkle_node *
merkle_tree_disk::lookup_exact (u_int depth, const merkle_hash &key)
{
  u_int realdepth = 0;
  merkle_node *curr_root = get_root ();
  merkle_node *ret = merkle_tree::lookup (&realdepth, depth, key, curr_root);
  assert (realdepth <= depth);
  if (realdepth != depth) {
    ret = NULL;
  } else {
    ret = make_node (((merkle_node_disk *) ret)->get_block_no (), 
		     ret->isleaf () ? MERKLE_DISK_LEAF: MERKLE_DISK_INTERNAL);
  }
  delete curr_root;
  return ret;
}

vec<merkle_hash>
get_all_keys (u_int depth, const merkle_hash &prefix, merkle_node_disk *n)
{
  vec<merkle_hash> keys;
  if (n->isleaf ()) {
    merkle_key *k = n->keylist.first ();
    while (k != NULL) {
      merkle_hash key (k->id);
      if (prefix_match(depth, key, prefix)) {
	keys.push_back (key);
      }
      k = n->keylist.next (k);
    }
  } else {
    for (uint i = 0; i < 64; i++) {
      vec<merkle_hash> child_keys = 
	get_all_keys (depth, prefix, (merkle_node_disk *) n->child (i));
      for (uint j = 0; j < child_keys.size(); j++) {
	keys.push_back (child_keys[j]);
      }
    }
  }

  return keys;
}

vec<merkle_hash>
merkle_tree_disk::database_get_keys (u_int depth, const merkle_hash &prefix)
{
  vec<merkle_hash> keys;

  // find all the keys matching this prefix
  merkle_node *r = get_root ();
  merkle_node *n = r;
  for (u_int i = 0; i < depth && !n->isleaf (); i++) {
    u_int32_t branch = prefix.read_slot (i);
    n = n->child (branch);
  }

  // now we have the node and the right depth that matches the prefix.
  // Read all the keys under this node that match the prefix
  keys = get_all_keys (depth, prefix, (merkle_node_disk *) n);
  delete r;
  return keys;
}

bool
get_keyrange_recurs (merkle_hash min, chordID max, u_int n, 
		     uint depth, vec<chordID> *keys, merkle_node_disk *node,
		     bool start_left)
{
  // go down until you find the leaf responsible for min
  // add all of its keys up to n, less than max.  If you haven't
  // found more than n keys, set min to the last key and keep
  // going

  bool over_max = false;

  if (node->isleaf ()) {
    chordID min_id = static_cast<bigint> (min);
    merkle_key *k = node->keylist.first ();
    merkle_key *last_key = NULL;

    while (k != NULL && keys->size () < n) {
      if (betweenbothincl (min_id, max, k->id)) {
	keys->push_back (k->id);
      }
      last_key = k;
      k = node->keylist.next (k);
    }
    
    // it's only over the maximum if:
    //   a) there's a key in this node, AND
    //   b) some other node has already been tried, AND
    //   c) that key is greater than max
    if (last_key != NULL && start_left && 
	betweenbothincl (min_id, last_key->id, max)) {
      over_max = true;
    } else {
      over_max = false;
    }
    // don't need a special case here, since we look at all the keys
  } else {
    u_int32_t branch = min.read_slot (depth);
    if (start_left) {
      branch = 0;
    }
    bool first_time = true;

    while (keys->size () < n && branch < 64 && !over_max) {
      merkle_node_disk *child = (merkle_node_disk *) node->child (branch);
      bool sl = start_left;
      // if we've already gone down one branch at this depth, don't read the
      // slot for the starting branch on any branch
      if (!first_time) {
	sl = true;
      }
      over_max = get_keyrange_recurs (min, max, n, depth+1, keys, child, sl);
      first_time = false;
      branch++;
    }

    // special case for when we hit the right edge of the ring
    if (depth == 0 && !over_max && keys->size () < n && 
	betweenrightincl (static_cast<bigint> (min), max, 0))
    {
      over_max = get_keyrange_recurs (0, max, n, depth, keys, node, true);
    }
  }

  return over_max;
}

vec<chordID> 
merkle_tree_disk::get_keyrange (chordID min, chordID max, u_int n)
{
  vec<chordID> keys;

  merkle_node *root = get_root ();
  if(root->count > 0) {
    get_keyrange_recurs (min, max, n, 0, &keys, 
			 (merkle_node_disk *) root, false);
  }
  delete root;

  return keys;
}

bool
merkle_tree_disk::key_exists (chordID key)
{
  merkle_node_disk *n = 
    (merkle_node_disk *) merkle_tree::lookup (key);
  merkle_key *mkey = n->keylist[key];
  lookup_release (n);

  return (mkey != NULL);
}
