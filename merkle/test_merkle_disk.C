#include "merkle_tree_disk.h"

int main (int argc, char **argv) {

  merkle_tree_disk *tree = New merkle_tree_disk( "/tmp/index.mrk", 
						 "/tmp/internal.mrk",
						 "/tmp/leaf.mrk" );

  //((merkle_node_disk*) tree->root)->add_key(12);
  //((merkle_node_disk*) tree->root)->add_key(100);
  //((merkle_node_disk*) tree->root)->write_out();

}
