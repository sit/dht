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
  char pathbuf[PATH_MAX];
  sprintf (pathbuf, "%s/mtree", argv[1]);
  ptr<merkle_tree_bdb> tree =
    New refcounted<merkle_tree_bdb> (pathbuf, true, true);

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

