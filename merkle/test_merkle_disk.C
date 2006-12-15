#include "merkle_tree_disk.h"
#include <id_utils.h>

int main (int argc, char **argv) {

  uint num_keys = 150;
  if (argc > 1) {
    num_keys = atoi (argv[1]);
  }

  srandom (time (NULL));

  merkle_tree *tree = New merkle_tree_disk ("/tmp/index.mrk", 
					    "/tmp/internal.mrk",
					    "/tmp/leaf.mrk", true);

  //tree->dump ();

  // inserts

  chordID c;
  for (uint i = 0; i < num_keys; i++) {
    c = make_randomID ();
    warn << "inserting " << c << " (" << i << ")\n";
    tree->insert (c);
    tree->check_invariants();
  }

  // lookups

  merkle_node_disk *n = (merkle_node_disk *) tree->lookup (to_merkle_hash (c));

  warn << "found node " << n->count << ": \n";

  if (n->isleaf ()) {
    merkle_key *k = n->keylist.first ();
    while (k != NULL) {
      warn << "\t" << k->id << "\n";
      k = n->keylist.next (k);
    }
  }

  tree->lookup_release (n);

  assert (tree->key_exists (c));

  // remove

  tree->remove (c);

  assert (!tree->key_exists (c));

  //tree->dump ();

  chordID min = c;
  chordID max = ((chordID) 1) << 156;
  vec<chordID> keys = tree->get_keyrange (min, max, 65);
  for (uint i = 0; i < keys.size (); i++) {
    warn << "Found key " << keys[i] << " in range [" 
	 << min << "," << max << "]\n";
  }
  delete tree;
  tree = NULL;

  unlink("/tmp/index.mrk");
  unlink("/tmp/internal.mrk");
  unlink("/tmp/leaf.mrk");
}
