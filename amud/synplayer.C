#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

ptr<dhashclient> dhash;
game_engine *mud;

void done_insert (ptr<avatar> a, chordID rID, mud_stat stat);

void 
new_player (str name, chordID rID)
{
  str pw (0);
  ref<avatar> a = New refcounted<avatar> (name, pw, dhash);
  mud->insert (a, wrap (&done_insert, a, rID));
}

void 
done_insert (ptr<avatar> a, chordID rID, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nAvatar " << a->get_name () << " creation successful!\n";
    //mud->enter_player (a, rID);
  } else
    cout << "Avatar creation error: stat = " << stat << "\n";
}

static void
usage ()
{
  warnx << "usage: " << progname << " sock\n";
  exit (1);
}

int 
main (int argc, char **argv)
{

  setprogname (argv[0]);

  if (argc < 2)
    usage ();

  str control_socket = argv[1];
  dhash = New refcounted<dhashclient> (control_socket);

  mud = New game_engine (dhash);
  str name ("a1");
  str rname ("r1");
  chordID rID = compute_hash (rname.cstr (), rname.len ());
  new_player (name, rID);

  amain ();
}

