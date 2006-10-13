#include <chord_types.h>
#include <id_utils.h>
#include "merkle_tree_disk.h"
#include "dhash_common.h"

//////////////// merkle_node_disk /////////////////

merkle_node_disk::merkle_node_disk () :
  merkle_node () {

}

merkle_node_disk::~merkle_node_disk () {
  // recursively deletes down the tree
  delete entry_disk;
  // not necessary, but helps catch dangling pointers
  bzero (this, sizeof (*this)); 
}

merkle_node *merkle_node_disk::child (u_int i) {
  assert (!isleaf ());
  assert (entry_disk);
  assert (i >= 0 && i < 64);
  return &(*entry_disk)[i];
}

void merkle_node_disk::internal2leaf () {
  delete entry_disk;
  entry_disk = NULL;
}

bool merkle_node_disk::isleaf () {
  return (entry_disk == NULL);
}

void merkle_node_disk::leaf2internal () {
  assert (entry_disk == NULL);
  entry_disk = New array<merkle_node_disk, 64> ();
}

//////////////// merkle_tree_disk /////////////////

merkle_tree_disk::merkle_tree_disk () :
  merkle_tree () {
}

merkle_tree_disk::~merkle_tree_disk ()
{
  keylist.deleteall_correct ();
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
