#include <id_utils.h>
#include "merkle_tree_disk.h"
#include <rxx.h>

static char *indexpath = "/tmp/index.mrk";
static char *internalpath = "/tmp/internal.mrk";
static char *leafpath  = "/tmp/leaf.mrk";
static char *indexpathro = "/tmp/index.mrk.ro";
static char *internalpathro = "/tmp/internal.mrk.ro";
static char *leafpathro  = "/tmp/leaf.mrk.ro";

void
cleanup ()
{
  unlink (indexpath);
  unlink (internalpath);
  unlink (leafpath);
  unlink (indexpathro);
  unlink (internalpathro);
  unlink (leafpathro);
}

int main (int argc, char **argv)
{
  uint num_keys = 150;
  if (argc > 1) {
    num_keys = atoi (argv[1]);
  }

  srandom (time (NULL));

  merkle_tree *tree = New merkle_tree_disk (indexpath,
					    internalpath,
					    leafpath, true);

  // if a trace is provided, execute the trace
  if (argc > 2) {
    str filename = argv[2];
    str file = file2str (filename);
    rxx newline ("\\n");
    vec<str> lines;
    split (&lines, newline, file);
    
    for (uint i = 0; i < lines.size(); i++) {
      if (i > num_keys) {
	warn << "did enough keys!\n";
	exit(0);
      }
      static const rxx space_rx ("\\s+");
      vec<str> parts;
      split (&parts, space_rx, lines[i]);
      if (parts.size() != 2) {
	continue;
      }
      chordID c;
      str2chordID (parts[1], c);
      if (parts[0] == "I") {
	warn << i << ") going to insert " << c << "\n";
	tree->insert (c);
      } else {
	warn << i << ") going to remove " << c << "\n";
	tree->remove (c);
      }
      tree->check_invariants ();
    }
    exit(0);
  }


  //tree->dump ();

  // inserts

  chordID c;
  for (uint i = 0; i < num_keys; i++) {
    c = make_randomID ();
    warn << "inserting " << c << " (" << i << ")\n";
    tree->insert (c);
    tree->check_invariants();
  }

  // lookups

  merkle_node_disk *n = (merkle_node_disk *) tree->lookup (c);

  warn << "found node " << n->count << ": \n";

  if (n->isleaf ()) {
    merkle_key *k = n->keylist.first ();
    while (k != NULL) {
      warn << "\t" << k->id << "\n";
      k = n->keylist.next (k);
    }
  }

  tree->lookup_release (n);

  assert (tree->key_exists (c));

  // remove

  tree->remove (c);

  assert (!tree->key_exists (c));

  //tree->dump ();

  chordID min = c;
  chordID max = ((chordID) 1) << 156;
  vec<chordID> keys = tree->get_keyrange (min, max, 65);
  for (uint i = 0; i < keys.size (); i++) {
    warn << "Found key " << keys[i] << " in range [" 
	 << min << "," << max << "]\n";
  }
  delete tree;
  tree = NULL;

  cleanup ();
}
