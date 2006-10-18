#include <chord_types.h>
#include <id_utils.h>
#include "merkle_tree_disk.h"
#include "dhash_common.h"

//////////////// merkle_node_disk /////////////////

FILE *open_file( str name ) {
  FILE *f = fopen( name, "r+" );
  if( f == NULL ) {
    f = fopen( name, "w+" );
  }
  if( f == NULL ) {
    fatal << "Couldn't open or create " << name << " for read/write\n";
  }
  return f;
}

merkle_node_disk::merkle_node_disk (FILE *internal, FILE *leaf, 
				    MERKLE_DISK_TYPE type, uint32 block_no) :
  merkle_node (), _internal(internal), _leaf(leaf), 
  _type(type), _block_no(block_no) {

  if( isleaf() ) {

    merkle_leaf_node leaf;
    int seekval = fseek( _leaf, _block_no*sizeof(merkle_leaf_node), SEEK_SET );
    assert( seekval == 0 );
    fread( &leaf, sizeof(merkle_leaf_node), 1, _leaf );

    count = ntohl(leaf.key_count);
    for( uint i = 0; i < count; i++ ) {
      chordID c;
      mpz_set_rawmag_be( &c, leaf.keys[i].key, sizeof(leaf.keys[i].key) );
      keylist.insert(New merkle_key(c));
    }

  } else {

    children = New array<uint32, 64>();
    hashes = New array<merkle_hash, 64>();

    merkle_internal_node internal;
    int seekval = fseek( _internal, _block_no*sizeof(merkle_internal_node), 
			 SEEK_SET );
    assert( seekval == 0 );
    fread( &internal, sizeof(merkle_internal_node), 1, _internal );

    count = ntohl(internal.key_count);
    for( uint i = 0; i < 64; i++ ) {
      chordID c;
      mpz_set_rawmag_be( &c, internal.hashes[i].key, 
			 sizeof(internal.hashes[i].key) );
      (*hashes)[i] = to_merkle_hash(c);
      (*children)[i] = ntohl( internal.child_pointers[i] );
    }

  }

  rehash();

}

merkle_node_disk::~merkle_node_disk () {
  keylist.deleteall_correct();

  // also delete all children
  for( uint i = 0; i < to_delete.size(); i++ ) {
    delete to_delete[i];
  }

  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
}

void merkle_node_disk::rehash() {

  hash = 0;
  if (count == 0) {
    return;
  }
  
  sha1ctx sc;
  if( isleaf() ) {
    assert( count > 0 && count <= 64 );
    merkle_key *k = keylist.first();
    while( k != NULL ) {
      merkle_hash h = to_merkle_hash(k->id);
      sc.update (h.bytes, h.size);
      k = keylist.next(k);
    }
  } else {
    for( uint i = 0; i < 64; i++ ) {
      merkle_hash h = (*hashes)[i];
      sc.update (h.bytes, h.size);
    }
  }
  sc.final (hash.bytes);

}

void merkle_node_disk::write_out() {

  if( isleaf() ) {

    merkle_leaf_node leaf;
    //bzero( &leaf, sizeof(merkle_leaf_node) );
    leaf.key_count = htonl(count);
    merkle_key *m = keylist.first();
    int i = 0;
    while( m != NULL ) {
      mpz_get_rawmag_be( leaf.keys[i].key, sizeof(leaf.keys[i].key), &(m->id));
      m = keylist.next(m);
      i++;
    }

    int seekval = fseek( _leaf, _block_no*sizeof(merkle_leaf_node), SEEK_SET );
    assert( seekval == 0 );
    fwrite( &leaf, sizeof(merkle_leaf_node), 1, _leaf );

  } else {

    merkle_internal_node internal;
    internal.key_count = htonl(count);
    for( uint i = 0; i < 64; i++ ) {
      merkle_hash h = (*hashes)[i];
      chordID c = tobigint(h);
      mpz_get_rawmag_be( internal.hashes[i].key, 
			 sizeof(internal.hashes[i].key), &c);
      internal.child_pointers[i] = htonl((*children)[i]);
    }

    int seekval = fseek( _internal, _block_no*sizeof(merkle_internal_node), 
			 SEEK_SET );
    assert( seekval == 0 );
    fwrite( &internal, sizeof(merkle_internal_node), 1, _internal );

  }

}

merkle_node *merkle_node_disk::child (u_int i) {
  assert (!isleaf ());
  assert (i >= 0 && i < 64);

  uint32 pointer = (*children)[i];
  MERKLE_DISK_TYPE type;
  if( pointer % 2 == 0 ) {
    type = MERKLE_DISK_INTERNAL;
  } else {
    type = MERKLE_DISK_LEAF;
  }
  merkle_node_disk *n = New merkle_node_disk( _internal, _leaf, type, 
					      pointer >> 1 );
  to_delete.push_back(n);
  return n;

}

merkle_hash merkle_node_disk::child_hash (u_int i) {
  assert (!isleaf ());
  assert (i >= 0 && i < 64);
  return (*hashes)[i];
}

void merkle_node_disk::set_child( merkle_node_disk *n, u_int i) {
  assert (!isleaf ());
  assert (i >= 0 && i < 64);

  (*children)[i] = ((n->get_block_no() << 1) | (n->isleaf()?0x00000001:0));
  (*hashes)[i] = n->hash;

}

void merkle_node_disk::add_key( chordID key ) {
  assert( isleaf() && count < 64 );
  count++;
  merkle_key *m = New merkle_key(key);
  keylist.insert(m);
}

void merkle_node_disk::add_key( merkle_hash key ) {
  assert( isleaf() && count < 64 );
  count++;
  merkle_key *m = New merkle_key(key);
  warn << "add_key called with " << key << "\n";
  keylist.insert(m);
}

void merkle_node_disk::internal2leaf () {
  assert( _type == MERKLE_DISK_INTERNAL );
  _type = MERKLE_DISK_LEAF;
}

bool merkle_node_disk::isleaf () const {
  return (_type == MERKLE_DISK_LEAF);
}

void merkle_node_disk::leaf2internal () {
  assert( isleaf() );
  _type = MERKLE_DISK_INTERNAL;
  children = New array<uint32, 64>();
  hashes = New array<merkle_hash, 64>();
  keylist.clear();
  warn << "node converting to internal\n";
  assert( !isleaf() );
}

//////////////// merkle_tree_disk /////////////////

merkle_tree_disk::merkle_tree_disk( str index, str internal, str leaf,
				    bool writer ) :
  merkle_tree (), _index_name(index), 
  _internal_name(internal), _leaf_name(leaf), _writer(writer) {

  _internal = open_file( _internal_name );
  _leaf = open_file( _leaf_name );

}

merkle_tree_disk::~merkle_tree_disk ()
{
  fclose( _internal );
  fclose( _leaf );
}

void merkle_tree_disk::write_metadata() {

  assert( _writer );

  _index = fopen( _index_name, "w" );
  if( _index == NULL ) {
    fatal << "Couldn't open or create " << _index_name << " for read/write\n";
  }
  
  assert( _free_leafs.size() == _md.num_leaf_free );
  assert( _free_internals.size() == _md.num_internal_free );

  _md.num_leaf_free += _future_free_leafs.size();
  _md.num_internal_free += _future_free_internals.size();

  warn << "writing metadata: " << _md.root << ", " << _md.num_leaf_free 
       << ", " << _md.num_internal_free << ", " << _md.next_leaf
       << ", " << _md.next_internal << "\n";

  fwrite( &_md, sizeof(merkle_index_metadata), 1, _index );

  int nfree = _md.num_leaf_free + _md.num_internal_free;
  uint32 freelist[nfree];

  // write out both the unused free slots and the newly created free slots
  uint i;
  uint sofar = 0;
  for( i = 0; i < _free_leafs.size(); i++ ) {
    freelist[i] = ((_free_leafs[i-sofar] << 1) | 0x00000001);
  }
  sofar = i;
  for( ; (i-sofar) < _free_internals.size(); i++ ) {
    freelist[i] = (_free_internals[i-sofar] << 1);
  }
  sofar = i;
  for( ; (i-sofar) < _future_free_leafs.size(); i++ ) {
    freelist[i] = ((_future_free_leafs[i-sofar] << 1) | 0x00000001);
  }
  sofar = i;
  for( ; (i-sofar) < _future_free_internals.size(); i++ ) {
    freelist[i] = (_future_free_internals[i-sofar] << 1);
  }

  fwrite( &freelist, sizeof(uint32), nfree, _index );
  _future_free_leafs.clear();
  _future_free_internals.clear();

  fclose( _index );

}

merkle_node *merkle_tree_disk::get_root() {

  // figure out where the root node is, given the index file
  // the first 32 bytes of the file tells us where it is
  _index = open_file( _index_name );
  int num_read = fread( &_md, sizeof(merkle_index_metadata), 1, _index );

  if( num_read <= 0 ) {
    // no root pointer yet, so we have a new tree
    _md.root = 1;
    _md.num_leaf_free = 0;
    _md.num_internal_free = 0;
    _md.next_leaf = 1;
    _md.next_internal = 0;

    // also, make a block there
    merkle_leaf_node new_root;
    bzero( &new_root, sizeof(merkle_leaf_node) );
    fseek( _leaf, 0, SEEK_SET );
    fwrite( &new_root, sizeof(merkle_leaf_node), 1, _leaf );

  } else if( _writer ) {

    _free_leafs.clear();
    _free_internals.clear();

    // read in the free list
    int nfree = _md.num_leaf_free+_md.num_internal_free;
    uint32 freelist[nfree];
    int nread = fread( &freelist, sizeof(uint32), nfree, _index );
    assert( nread == nfree );

    for( int i = 0; i < nread; i++ ) {
      uint32 pointer = freelist[i];
      if( pointer % 2 == 0 ) {
	_free_internals.push_back(pointer >> 1);
      } else {
	warn << "found free leaf " << (pointer >> 1) << "\n"; 
	_free_leafs.push_back(pointer >> 1);
      }
    }

  }

  fclose( _index );

  return make_node(_md.root);

}

merkle_node *merkle_tree_disk::make_node( uint32 block_no, 
					  MERKLE_DISK_TYPE type ) {
  return New merkle_node_disk( _internal, _leaf, type, block_no );
}

merkle_node *merkle_tree_disk::make_node( uint32 pointer ) {
  // even == internal, odd == lead
  if( pointer % 2 == 0 ) {
    return make_node( pointer >> 1, MERKLE_DISK_INTERNAL );
  } else {
    return make_node( pointer >> 1, MERKLE_DISK_LEAF );
  }
}

// returns a block_no without the type bit on the end
void merkle_tree_disk::free_block( uint32 block_no, MERKLE_DISK_TYPE type ) {

  assert( _writer );

  // don't want these blocks being used until the root gets switched over
  // in write_metadata, so keep them on a separate list
  if( type == MERKLE_DISK_LEAF ) {
    _future_free_leafs.push_back( block_no );
  } else {
    _future_free_internals.push_back( block_no );
  }

}

// returns a block_no without the type bit on the end
uint32 merkle_tree_disk::alloc_free_block( MERKLE_DISK_TYPE type ) {

  assert( _writer );

  uint32 ret;

  // if there are any free
  if( type == MERKLE_DISK_LEAF ) {
    if( _md.num_leaf_free > 0 ) {
      ret = _free_leafs.pop_back();
      warn << "popped free leaf " << ret << "\n";
      _md.num_leaf_free--;
    } else {
      ret = _md.next_leaf;
      _md.next_leaf++; 
    }
  } else {
    if( _md.num_internal_free > 0 ) {
      ret = _free_internals.pop_back();
      _md.num_internal_free--;
    } else {
      ret = _md.next_internal;
      _md.next_internal++; 
    }
  }

  warn << "allocing block " << ret << " for type " << type << "\n";
  return ret;

}

void
merkle_tree_disk::remove (u_int depth, merkle_hash& key, merkle_node *n)
{

  assert( _writer );

  if (n->isleaf ()) {
    chordID k = tobigint (key);
    merkle_key *mkey = keylist[k];
    assert (mkey);
    keylist.remove (mkey);
  } else {
    u_int32_t branch = key.read_slot (depth);
    remove (depth+1, key, n->child (branch));
  }
  
  assert (n->count != 0);
  n->count -= 1;
  if (!n->isleaf () && n->count <= 64)
    n->internal2leaf ();
  rehash (depth, key, n);
}

void merkle_tree_disk::leaf2internal( uint depth, merkle_node_disk *n ) {

  assert( n->isleaf() );
  merkle_key *k = n->keylist.first();
  array< vec<merkle_key *>, 64> keys;
  while( k != NULL ) {
    merkle_hash h = to_merkle_hash(k->id);
    u_int32_t branch = h.read_slot(depth);
    //warn << "picked branch " << branch << " for key " << k->id << ", hash " 
    // << h << "\n";
    keys[branch].push_back(k);
    k = n->keylist.next(k);
  }

  warn << "converting to internal\n";
  n->leaf2internal();
  assert( !n->isleaf() );

  // NOTE: bottom depth may only have 16??
  for( uint i = 0; i < 64; i++ ) {

    // zero the new guy out
    uint block_no = alloc_free_block( MERKLE_DISK_LEAF );
    merkle_leaf_node new_leaf;
    bzero( &new_leaf, sizeof(merkle_leaf_node) );
    fseek( _leaf, block_no*sizeof(merkle_leaf_node), SEEK_SET );
    fwrite( &new_leaf, sizeof(merkle_leaf_node), 1, _leaf );

    merkle_node_disk *child = 
      (merkle_node_disk *) make_node( block_no, MERKLE_DISK_LEAF );

    vec<merkle_key *> v = keys[i];
    for( uint j = 0; j < v.size(); j++ ) {
      //warn << "adding key " << j << " to node " << i << "\n";
      child->add_key( v[j]->id );
    }
    n->rehash();
    n->set_child( child, i );
    child->write_out();
    delete child;
  }

  assert( !n->isleaf() );

}

int
merkle_tree_disk::insert (u_int depth, merkle_hash& key, merkle_node *n)
{

  int ret = 0;
    
  MERKLE_DISK_TYPE old_type;
  if ( n->isleaf () ) {
    old_type = MERKLE_DISK_LEAF;
    if( n->leaf_is_full () ) {
      leaf2internal( depth, (merkle_node_disk *) n);
      assert( !n->isleaf() );
    }
  } else {
    old_type = MERKLE_DISK_INTERNAL;
  } 

  MERKLE_DISK_TYPE type;
  if (n->isleaf ()) {
    type = MERKLE_DISK_LEAF;
    warn << "adding key " << key << "\n";
    ((merkle_node_disk *) n)->add_key(key);
  } else {
    type = MERKLE_DISK_INTERNAL;
    u_int32_t branch = key.read_slot(depth);
    merkle_node *child = n->child(branch);
    //warn << "inserting into child " 
    //	 << ((merkle_node_disk *) child)->get_block_no() << " on branch " 
    //	 << branch << " and leaf " << child->isleaf() << "\n";
    ret = insert( depth+1, key, child );
    ((merkle_node_disk *) n)->set_child( (merkle_node_disk *) child, branch );
    n->count++;
    warn << "new count " << n->count << "\n";
  }

  rehash( depth, key, n );
  
  // write this block out to a new place on disk, then switch the root
  // atomically
  uint32 block_no = alloc_free_block(type);
  merkle_node_disk *nd = (merkle_node_disk *) n;
  warn << "freeing old block " << nd->get_block_no() << "\n";
  free_block( nd->get_block_no(), old_type );
  nd->set_block_no( block_no );
  nd->write_out();

  return ret;
}

void merkle_tree_disk::switch_root( merkle_node_disk *n ) {
  assert(_writer);
  uint32 newroot = n->get_block_no();
  warn << "new root block num " << newroot << "\n";
  newroot <<= 1;
  if( n->isleaf() ) {
    newroot |= 0x00000001;
  }
  _md.root = newroot;
  // NOTE: there should only be one thread on the machine that is
  // inserting or removing blocks and writing metadata.  Any others
  // need to be readonly
  write_metadata();
}

int merkle_tree_disk::insert (merkle_hash &key) {

  assert( _writer );
  merkle_node_disk *curr_root = (merkle_node_disk *) get_root();
  int ret = insert( 0, key, curr_root );
  switch_root(curr_root);
  delete curr_root;
  return ret;

}

void merkle_tree_disk::lookup_release( merkle_node *n ) {
  delete n;
}

merkle_node *
merkle_tree_disk::lookup( u_int *depth, u_int max_depth,
			  const merkle_hash &key ) {

  *depth = 0;
  merkle_node *curr_root = get_root();
  merkle_node *ret = merkle_tree::lookup( depth, max_depth, key, 
					       curr_root );
  ret = make_node(((merkle_node_disk *) ret)->get_block_no(),
		  ret->isleaf()?MERKLE_DISK_LEAF:MERKLE_DISK_INTERNAL);
  delete curr_root;
  return ret;

}

merkle_node *
merkle_tree_disk::lookup( u_int depth, const merkle_hash &key ) {

  u_int depth_ignore = 0;
  merkle_node *curr_root = get_root();
  merkle_node *ret = merkle_tree::lookup( &depth_ignore, depth, key, 
					       curr_root );
  ret = make_node(((merkle_node_disk *) ret)->get_block_no(),
		  ret->isleaf()?MERKLE_DISK_LEAF:MERKLE_DISK_INTERNAL);
  delete curr_root;
  return ret;

}

merkle_node *
merkle_tree_disk::lookup_exact( u_int depth, const merkle_hash &key ) {

  u_int realdepth = 0;
  merkle_node *curr_root = get_root();
  merkle_node *ret = merkle_tree::lookup( &realdepth, depth, key, curr_root );
  assert( realdepth <= depth );
  if( realdepth != depth ) {
    ret = NULL;
  } else {
    ret = make_node(((merkle_node_disk *) ret)->get_block_no(), 
		    ret->isleaf()?MERKLE_DISK_LEAF:MERKLE_DISK_INTERNAL);
  }
  delete curr_root;
  return ret;

}

vec<merkle_hash>
get_all_keys( u_int depth, const merkle_hash &prefix, merkle_node_disk *n ) {

  vec<merkle_hash> keys;
  if( n->isleaf() ) {
    merkle_key *k = n->keylist.first();
    while( k != NULL ) {
      merkle_hash key = to_merkle_hash(k->id);
      if( prefix_match(depth, key, prefix)) {
	keys.push_back(key);
      }
      k = n->keylist.next(k);
    }
  } else {
    for( uint i = 0; i < 64; i++ ) {
      vec<merkle_hash> child_keys = 
	get_all_keys( depth, prefix, (merkle_node_disk *) n->child(i) );
      for( uint j = 0; j < child_keys.size(); j++ ) {
	keys.push_back( child_keys[j] );
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
  merkle_node *r = get_root();
  merkle_node *n = r;
  for( u_int i = 0; i < depth && !n->isleaf(); i++ ) {
    u_int32_t branch = prefix.read_slot(i);
    n = n->child(branch);
  }

  // now we have the node and the right depth that matches the prefix.
  // Read all the keys under this node that match the prefix
  keys = get_all_keys( depth, prefix, (merkle_node_disk *) n );
  delete r;
  return keys;
}

vec<chordID> 
merkle_tree_disk::get_keyrange (chordID min, chordID max, u_int n)
{
  assert(0); // not implemented yet
  vec<chordID> keys;
  return keys;
}

bool merkle_tree_disk::key_exists (chordID key) {
  assert(0); // not implemented yet
  return false;
}
