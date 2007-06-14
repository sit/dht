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
    fatal <<  "Usage: " << progname << " merkletreebdb\n";

  ptr<merkle_tree_bdb> tree = NULL;
  if (merkle_tree_bdb::tree_exists (argv[1])) {
    // Handle case where path is given directly
    tree = New refcounted<merkle_tree_bdb> (argv[1], true, true);
  } else {
    // Handle case where path may owned by adbd.
    char pathbuf[PATH_MAX];
    sprintf (pathbuf, "%s/mtree", argv[1]);
    tree = New refcounted<merkle_tree_bdb> (pathbuf, true, true);
    // Fatal if this tree does not exist.
  }

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

