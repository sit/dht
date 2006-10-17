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

merkle_node_disk::merkle_node_disk (FILE *index, FILE *internal, FILE *leaf, 
				    MERKLE_DISK_TYPE type, uint32 block_no) :
  merkle_node (), _index(index), 
  _internal(internal), _leaf(leaf), _type(type), _block_no(block_no) {

  if( isleaf() ) {
    merkle_leaf_node leaf;
    int seekval = fseek( _leaf, _block_no*sizeof(merkle_leaf_node), SEEK_SET );
    assert( seekval == 0 );
    fread( &leaf, sizeof(merkle_leaf_node), 1, _leaf );

    count = ntohl(leaf.key_count);
    for( uint i = 0; i < count; i++ ) {
      chordID c;
      mpz_set_rawmag_be( &c, leaf.keys[i].key, sizeof(chordID) );
      keylist.insert(New merkle_key(c));
    }

  } else {

  }

}

merkle_node_disk::~merkle_node_disk () {
  keylist.deleteall_correct();
  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
}

void merkle_node_disk::write_out() {

  if( isleaf() ) {
    merkle_leaf_node leaf;
    //bzero( &leaf, sizeof(merkle_leaf_node) );
    leaf.key_count = htonl(count);
    merkle_key *m = keylist.first();
    int i = 0;
    while( m != NULL ) {
      mpz_get_rawmag_be( leaf.keys[i].key, sizeof(chordID), &(m->id));
      m = keylist.next(m);
      i++;
    }

    int seekval = fseek( _leaf, _block_no*sizeof(merkle_leaf_node), SEEK_SET );
    assert( seekval == 0 );
    fwrite( &leaf, sizeof(merkle_leaf_node), 1, _leaf );

  }

}

merkle_node *merkle_node_disk::child (u_int i) {
  assert (!isleaf ());
  assert (i >= 0 && i < 64);
  return NULL;
}

void merkle_node_disk::add_key( chordID key ) {
  count++;
  assert( isleaf() && count < 64 );
  merkle_key *m = New merkle_key(key);
  keylist.insert(m);
}

void merkle_node_disk::internal2leaf () {
  assert( _type == MERKLE_DISK_INTERNAL );
  _type = MERKLE_DISK_LEAF;
}

bool merkle_node_disk::isleaf () {
  return (_type == MERKLE_DISK_LEAF);
}

void merkle_node_disk::leaf2internal () {
  assert( _type == MERKLE_DISK_LEAF );
  _type = MERKLE_DISK_INTERNAL;
}

//////////////// merkle_tree_disk /////////////////

merkle_tree_disk::merkle_tree_disk (str index, str internal, str leaf) :
  merkle_tree (), _index_name(index), 
  _internal_name(internal), _leaf_name(leaf) {

  _index = open_file( _index_name );
  _internal = open_file( _internal_name );
  _leaf = open_file( _leaf_name );

  // figure out where the root node is, given the index file
  // the first 32 bytes of the file tells us where it is
  uint32 root_pointer;
  int num_read = fread( &root_pointer, sizeof(uint32), 1, _index );
  if( num_read <= 0 ) {
    // no root pointer yet, so we have a new tree
    root_pointer = 1;
    fseek( _index, 0, SEEK_SET );
    fwrite( &root_pointer, sizeof(uint32), 1, _index );

    // also, make a block there
    merkle_leaf_node new_root;
    bzero( &new_root, sizeof(merkle_leaf_node) );
    fseek( _leaf, 0, SEEK_SET );
    fwrite( &new_root, sizeof(merkle_leaf_node), 1, _leaf );

  }

  root = make_node(root_pointer);

}

merkle_tree_disk::~merkle_tree_disk ()
{
  fclose( _index );
  fclose( _internal );
  fclose( _leaf );
}

merkle_node *merkle_tree_disk::make_node( uint32 pointer ) {
  // even == internal, odd == lead
  if( pointer % 2 == 0 ) {
    return New merkle_node_disk( _index, _internal, _leaf, 
				 MERKLE_DISK_INTERNAL, pointer >> 1 );
  } else {
    return New merkle_node_disk( _index, _internal, _leaf, 
				 MERKLE_DISK_LEAF, pointer >> 1 );
  }
}

void
merkle_tree_disk::remove (u_int depth, merkle_hash& key, merkle_node *n)
{
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


int
merkle_tree_disk::insert (u_int depth, merkle_hash& key, merkle_node *n)
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

merkle_node *
merkle_tree_disk::lookup (u_int *depth, u_int max_depth, 
		     const merkle_hash &key, merkle_node *n)
{
  // recurse down as much as possible
  if (*depth == max_depth || n->isleaf ())
    return n;
  u_int32_t branch = key.read_slot (*depth); 
  //the [6*depth, 6*(depth +1) bits determine which branch to follow
  // for a given key
  *depth += 1;
  return lookup (depth, max_depth, key, n->child (branch));
}
