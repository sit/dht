#include <sys/types.h>
#include <sys/wait.h>

#include <db.h>

#include <chord_types.h>

#include <qhash.h>
#include "merkle.h"
#include "merkle_tree_disk.h"
#include "merkle_tree_bdb.h"
#include <misc_utils.h>
#include <id_utils.h>

typedef bhash<chordID, hashID> keys_t;

static char *indexpath = "./index.mrk";
static char *internalpath = "./internal.mrk";
static char *leafpath  = "./leaf.mrk";
static char *indexpathro = "./index.mrk.ro";
static char *internalpathro = "./internal.mrk.ro";
static char *leafpathro  = "./leaf.mrk.ro";

// Better be careful here!  We call rm -rf on this later.
static char *bdbpath = "./mtree.bdb";

typedef callback<merkle_tree *, bool>::ref merkle_allocator_t;

merkle_tree *
allocate_disk (bool master)
{
  // Master should be read-write
  return
    New merkle_tree_disk (indexpath, internalpath, leafpath, master);
}

merkle_tree *
allocate_bdb (bool master)
{
  return
    New merkle_tree_bdb (bdbpath, !master, !master);
  // slaves should join and be read-only;
  // master should not join and be read-write.
}

void
cleanup ()
{
  unlink (indexpath);
  unlink (internalpath);
  unlink (leafpath);
  unlink (indexpathro);
  unlink (internalpathro);
  unlink (leafpathro);
  char buf[80];
  sprintf (buf, "rm -rf %s", bdbpath);
  system (buf);
}

bool
reap (int pid, bool wait = true)
{
  int wpid, status, options = 0;

  if (!wait)
    options = WNOHANG;
  wpid = waitpid (pid, &status, options);
  if (wpid < 0) {
    perror("waitpid");
    exit (1);
  }
  if (!wait && (wpid == 0))
    return false;
  if (wpid != pid)
    fatal ("unexpected pid reaped: %d\n", wpid);
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
    fatal << "child exited unhappily\n";
  return true;
}

void
insert_blocks (merkle_tree *mtree, int upto, bool random,
    keys_t *keys = NULL)
{
  for (int i = 1; i <= upto; i++) {
    merkle_hash key;
    if (random) 
      key.randomize ();
    else
      key = i;

    if (keys)
      keys->insert (static_cast<bigint> (key));
    int r = mtree->insert (key);
    if (r)
      fatal << "Unexpected Merkle tree error: " << r << " (" << strerror (r) << ")\n";
  }
}

void
test_numkeys (merkle_tree *tree, unsigned int cnt)
{
  merkle_node *root = tree->get_root ();
  assert (root->count == cnt);
  tree->lookup_release (root);
}

void
test_insertions (str msg,
    merkle_tree *mtree, uint nkeys, bool rand, keys_t &keys)
{
  uint okeyssz = keys.size ();
  warn << msg << " bulk insertion... ";
  u_int64_t start = getusec (true);
  mtree->set_rehash_on_modification (false);
  insert_blocks (mtree, nkeys, rand, &keys);
  warn << "completed in: " << (getusec (true)-start)/1000 << "ms... ";
  assert ((keys.size () - okeyssz) == nkeys);
  warn << "OK\n";

  warn << msg << " hash full tree... ";
  start = getusec (true);
  mtree->hash_tree ();
  warn << "completed in: " << (getusec (true)-start)/1000 << "ms... ";
  test_numkeys (mtree, keys.size ());
  warn << "OK\n";

  warn << msg << " check invariants... ";
  mtree->check_invariants ();
  warn << "OK.\n";

  mtree->set_rehash_on_modification (true);
}

const vec<chordID> &
test_reads (merkle_tree *mtree, const keys_t &keys)
{
  static vec<chordID> intree;
  intree.clear ();
  unsigned int nkeys = keys.size ();

  warn << "All key read (get_keyrange)... ";
  intree = mtree->get_keyrange (
      0, (chordID (1) << 160) - 1, nkeys + 1);
  assert (intree.size () == nkeys);
  for (uint i = 0; i < intree.size (); i++) {
    assert (keys[intree[i]]);
  }
  warn << "OK\n";

  warn << "Randomized key lookups... ";
  uint limit = 128 + int(nkeys * 0.2);
  if (limit > nkeys)
    limit = nkeys;
  uint order[nkeys];
  for (uint i=0; i<nkeys; i++) order[i] = i;
  for (uint i = 0; i < limit; i++) {
    uint j = i + random() % (nkeys-i);
    uint key = order[j]; order[j] = order[i]; order[i] = key;
    assert (mtree->key_exists (intree[key]));
    merkle_node *n = mtree->lookup (intree[key]);
    assert (n);
    assert (n->isleaf ());
    mtree->lookup_release (n);
  }
  warn << "OK\n";

  warn << "Missing key lookups... ";
  int todo = limit;
  while (todo) {
    chordID c = make_randomID ();
    if (keys[c])
      continue;
    todo--;
    assert (!mtree->key_exists (c));
    merkle_node *n = mtree->lookup (c);
    assert (n);
    mtree->lookup_release (n);
  }
  warn << "OK\n";

  warn << "Random range reads... ";
  chordID highest = (((chordID) 1) << 160) - 1;
  for (int i = 0; i < 32; i++) {
    chordID min = make_randomID ();
    chordID max = make_randomID ();
    //warn << "  get_keyrange (" << min << ", " << max << ", " << nkeys << ")\n";
    vec<chordID> rangekeys = mtree->get_keyrange (min, max, nkeys);

    keys_t subset;
    for (uint j = 0; j < nkeys; j++)
      if (betweenbothincl (min, max, intree[j]))
	subset.insert (intree[j]);
    assert (subset.size () == rangekeys.size ());
    for (uint j = 0; j < rangekeys.size (); j++)
      assert (subset[rangekeys[j]]);
  }
  warn << "OK\n";
  return intree;
}

void
test (str msg, merkle_tree *mtree, uint nkeys, bool rand = true)
{
  warn << "\n=================== " << msg 
       << " test_" << (rand ? "rand" : "incr")
       << " nkeys " << nkeys << "\n";

  keys_t keys;
  keys.clear ();

  test_insertions ("Initial", mtree, nkeys, rand, keys);
  const vec<chordID> intree = test_reads (mtree, keys);

  uint limit = (nkeys < 128) ? nkeys : (128 + int (nkeys * 0.2));
  warn << "Removing " << limit << " keys... ";
  uint order[nkeys];
  for (uint i=0; i<nkeys; i++) order[i] = i;
  for (uint i = 0; i < limit; i++) {
    // warnx << "  " << i+1 << "/" << limit << ".\n";
    uint j = i + random() % (nkeys-i);
    uint key = order[j]; order[j] = order[i]; order[i] = key;
    int r = mtree->remove (intree[key]);
    if (r)
      fatal << "Unexpected Merkle tree error: " << r << " (" << strerror (r) << ")\n";
    keys.remove (intree[key]);
    test_numkeys (mtree, nkeys - (i + 1));
    assert (keys.size () == (nkeys - (i + 1)));
  }
  warn << "OK\n";

  warn << "Post-remove tree contents check... ";
  for (uint i = 0; i < nkeys; i++) {
    if (i < limit)
      assert (!mtree->key_exists (intree[order[i]]));
    else
      assert (mtree->key_exists (intree[order[i]]));
  }
  mtree->check_invariants ();
  warn << "OK\n";

  if (rand)
    test_insertions ("Post-remove", mtree, 2*limit, rand, keys);
  test_reads (mtree, keys);
}

void
test_merkle_disk_specific (str desc, merkle_allocator_t alloc)
{
  warn << "\n=================== Disk specific tests for " << desc << "\n";
  merkle_tree *mtree = alloc (true);

  keys_t keys;
  keys.clear ();

  test_insertions ("Initial", mtree, 256, true, keys);
  delete mtree;

  warn << "Static child read... \n";
  int pid = fork ();
  if (pid < 0)
    fatal ("fork: %m\n");
  if (pid == 0) {
    // Child
    mtree = alloc (false);
    const vec<chordID> intree = test_reads (mtree, keys);
    delete mtree;
    exit (0);
  }
  reap (pid);
  warn << "Static child read OK\n";

  warn << "Dynamic child reads... \n";
  vec<chordID> tobeinserted;
  for (int i = 0; i < 3200; i++)
    tobeinserted.push_back (make_randomID ());
  pid = fork ();
  if (pid > 0) {
    // Parent
    bool reaped = false;
    mtree = alloc (true);
    for (int i = 0; i < 32; i++) {
      warn << "  Parent insertion burst " << i+1 << "\n";
      for (int j = 0; j < 100; j++) {
	mtree->insert (tobeinserted[100*i + j]);
	keys.insert (tobeinserted[100*i + j]);
      }
      if (!reaped)
	reaped = reap (pid, false);
      mtree->sync ();
      sleep (1);
    }
    delete mtree;
    if (!reaped)
      reap (pid);
  } else {
    sleep (1);
    chordID highest = (((chordID) 1) << 160) - 1;
    mtree = alloc (false);
    for (int i = 0; i < 32; i++) {
      warn << "  Child read-range " << i+1 << "\n";
      chordID min = make_randomID ();
      chordID max = min + make_randomID () % (highest - min);
      vec<chordID> rangekeys = mtree->get_keyrange (min, max, 10000);
      for (uint j = 0; j < rangekeys.size (); j++)
	assert (betweenbothincl (min, max, rangekeys[j]));
      mtree->check_invariants ();
      mtree->sync ();
      sleep (1);
    }
    delete mtree;
    exit (0);
  } 
  warn << "Dynamic child reads seem OK\n";
}

int
main (int argc, char *argv[])
{
  // Make sure no confusion from previous crashes.
  cleanup ();

  // Any arguments mean we skip the "normal" tests.
  if (argc == 1) {
    int sz[] = { 1, 64, 256, 64*64+1, 10000 };

    for (uint i = 0; i < sizeof (sz) / sizeof (sz[0]); i++) {
      merkle_tree *t =
	New merkle_tree_bdb (bdbpath, false, false);
      test ("BDB", t, sz[i], false);
      delete t; t = NULL;
      cleanup ();

      t = New merkle_tree_bdb (bdbpath, false, false);
      test ("BDB", t, sz[i], true);
      delete t; t = NULL;
      cleanup ();
    }

    for (uint i = 0; i < sizeof (sz) / sizeof (sz[0]); i++) {
      merkle_tree *t = New merkle_tree_mem ();
      test ("In-memory", t, sz[i], false);
      delete t; t = NULL;

      t = New merkle_tree_mem ();
      test ("In-memory", t, sz[i], true);
      delete t; t = NULL;
    }
#if 0
    for (uint i = 0; i < sizeof (sz) / sizeof (sz[0]); i++) {
      merkle_tree *t = 
	New merkle_tree_disk (indexpath, internalpath, leafpath, true);
      test ("Disk", t, sz[i]);
      delete t; t = NULL;
      cleanup ();
    }
#endif /* 0 */
  }

  test_merkle_disk_specific ("bdb", wrap (&allocate_bdb));
  cleanup ();

#if 0
  test_merkle_disk_specific ("disk", wrap (&allocate_disk));
  cleanup ();
#endif /* 0 */
}
