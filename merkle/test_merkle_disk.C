#include "merkle_tree_disk.h"
#include <id_utils.h>

int main (int argc, char **argv) {

  srandom(time(NULL));

  merkle_tree *tree = New merkle_tree_disk( "/tmp/index.mrk", 
					    "/tmp/internal.mrk",
					    "/tmp/leaf.mrk", true );

  tree->dump();

  chordID c;
  for( uint i = 0; i < 500; i++ ) {
    c = make_randomID();
    warn << "\ninserting " << c << " (" << i << ")\n";
    tree->insert( c );
  }

  merkle_node_disk *n = (merkle_node_disk *) tree->lookup(to_merkle_hash(c));

  warn << "found node " << n->count << ": \n";

  if( n->isleaf() ) {
    merkle_key *k = n->keylist.first();
    while( k != NULL ) {
      warn << "\t" << k->id << "\n";
      k = n->keylist.next(k);
    }
  }

  tree->lookup_release(n);

  tree->dump();

}
