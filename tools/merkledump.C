#include <async.h>
#include <aios.h>
#include <db.h>
#include <id_utils.h>
#include <merkle_tree_bdb.h>

int
main (int argc, char *argv[])
{
  setprogname (argv[0]);
  if (argc < 2)
    fatal <<  "Usage: " << progname << " [-c] merkletreebdb\n";
  
  bool check = false;
  char *path = argv[1];
  if (!strcmp (path, "-c")) {
    path = argv[2];
    check = true;
  }

  ptr<merkle_tree_bdb> tree = NULL;
  if (merkle_tree_bdb::tree_exists (path)) {
    // Handle case where path is given directly
    tree = New refcounted<merkle_tree_bdb> (path, true, true);
  } else {
    // Handle case where path may owned by adbd.
    char pathbuf[PATH_MAX];
    sprintf (pathbuf, "%s/mtree", path);
    tree = New refcounted<merkle_tree_bdb> (pathbuf, true, true);
    // Fatal if this tree does not exist.
  }

  if (check)
    tree->check_invariants ();

  chordID lastread = 0;
  chordID maxid = (chordID (1) << 160) - 1;
  vec<chordID> keys = tree->get_keyrange (0, maxid, 64);
  while (keys.size ()) {
    for (size_t i = 0; i < keys.size (); i++)
      aout << keys[i] << "\n";
    lastread = keys.back ();
    if (keys.size () < 64)
      break;
    keys = tree->get_keyrange (incID (lastread), maxid, 64);
  }
}

