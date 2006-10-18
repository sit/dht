#include "merkle_tree_disk.h"
#include <id_utils.h>

int main (int argc, char **argv) {

  merkle_tree *tree = New merkle_tree_disk( "/tmp/index.mrk", 
					    "/tmp/internal.mrk",
					    "/tmp/leaf.mrk", true );

  tree->dump();

  for( uint i = 0; i < 5000; i++ ) {
    chordID c = make_randomID();
    warn << "\ninserting " << c << " (" << i << ")\n";
    tree->insert( c );
  }

    tree->dump();

}
