#include "merkle_tree_disk.h"

int main (int argc, char **argv) {

  merkle_tree *tree = New merkle_tree_disk( "/tmp/index.mrk", 
					    "/tmp/internal.mrk",
					    "/tmp/leaf.mrk", true );


  warn << "\ninserting " << 12 << "\n";
  tree->insert(12);
  warn << "\ninserting " << 100 << "\n";
  tree->insert(100);
  warn << "\ninserting " << 47464 << "\n";
  tree->insert(47464);

  tree->dump();

  ((merkle_node_disk*) tree->root)->write_out();

}
