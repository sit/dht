#include "merkle.h"
#include "misc_utils.h"

/*
 * Basic basic timing tests of Merkle tree
 * Should probably test removal as well.
 */

void
insert_blocks (merkle_tree *mtree, int upto, bool random)
{
  for (int i = 1; i <= upto; i++) {
    merkle_hash key;
    if (random) 
      key.randomize ();
    else
      key = i;

    mtree->insert (key);
    //mtree->dump ();

    if (i % 10000 == 0) {
      warn << "inserted " << i << " blocks..\n";
      //mtree->dump ();
      //dumpdb ();
    }
  }
}

void
test (int upto, bool rand = true)
{
  warn << "\n=================== test_" << (rand ? "rand" : "incr")
       << " upto " << upto << "\n";

  u_int64_t start = getusec ();
  merkle_tree mtree;
  mtree.set_rehash_on_modification (false);
  insert_blocks (&mtree, upto, rand);
  warn << "completed in: " << (getusec ()-start)/1000 << "ms\n";
  start = getusec ();
  mtree.hash_tree ();
  warn << "rehash completed in: " << (getusec ()-start)/1000 << "ms\n";
  mtree.check_invariants ();
  // mtree.dump ();
}

int
main ()
{
  // XXX call random_init () ???
  //     -- between tests...???
  do {
    test (1, false);
    test (64, false);
    test (65, false);
    test (64 * 64, false);
    test (64 * 64 + 1, false);
    test (10000, false);
//    test_rand (100000);
//    test_rand (1000000);
  } while (0);

  do {
    test (1);
    test (64);
    test (65);
    test (64 * 64);
    test (64 * 64 + 1);
    test (10000);
//    test_rand (100000);
//    test_rand (1000000);
  } while (0);
}

