#include "merkle.h"

merkle_tree tree (NULL);

uint64 nGBs = 150;
uint64 blksz = (1 << 13); // 8k
uint64 nblocks = (nGBs * ((uint64)1 << 30)) / blksz;

void
dump ()
{
  static uint last_dump = 99999;
  uint j = (uint) ((uint64)10000 * tree.root.count) / nblocks;
  if (j != last_dump) {
    warn << "\n[ITER " << j << "] " << tree.root.count << " of " << nblocks << " blocks inserted\n";
    tree.compute_stats ();
    last_dump = j;
  }
}

int
main ()
{
  for (uint i = 0; i < nblocks; i++) {
    block *b = New block ();
    tree.insert (b);
    delete b;
    dump ();
  }
}

