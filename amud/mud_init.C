#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

ptr<dhashclient> dhash;
game_engine *mud;
int rcount = 0;

void done_insert_object (ref<mud_obj>, bool, mud_stat);

void
insert_room (str rname)
{
  ref<room> r = New refcounted <room> (rname, dhash);

  for (int i=0; i<3; i++) {
    char tname[50];
    sprintf (tname, "%s%s%d", rname.cstr (), "_t", i);
    cout << "Inserting " << tname << "\n";
    str tn (tname);
    ref<thing> t = New refcounted <thing> (tn, chordID(0));  
    mud->insert (t, wrap (done_insert_object, t, false));
    r->place (t);
  }

  mud->insert (r, wrap (&done_insert_object, r, true));
}

void 
done_insert_object (ref<mud_obj> o, bool room, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nObject " << o->get_name () << " insertion successful!\n";
    if (room && rcount < 100) {
      char rname [50];
      sprintf (rname, "%s%d", "r", ++rcount);
      str rn (rname);
      insert_room (rn);
      cout << "Inserted " << rcount << " rooms.\n";
    }
   } else
    cout << "Object insertion error: stat = " << stat << "\n";
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

  //  for (int i=0; i<100; i++) {
    char rname [50];
    sprintf (rname, "%s%d", "r", 0);
    str rn (rname);

    insert_room (rn);
    //}
  amain ();
}
