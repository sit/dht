#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

#define MAX_ROOMS 100
#define MAX_THINGS 3

ptr<dhashclient> dhash;
game_engine *mud;
int rcount = 0;
vec<ref<room> > rl;

void done_insert_object (ref<mud_obj>, mud_stat);
void done_insert_room (ref<room>, mud_stat);
void connect_rooms ();

void
insert_room (str rname)
{
  ref<room> r = New refcounted <room> (rname, dhash);

  for (int i=0; i<MAX_THINGS; i++) {
    char tname[50];
    sprintf (tname, "%s%s%d", rname.cstr (), "_t", i);
    cout << "Inserting " << tname << "\n";
    str tn (tname);
    ref<thing> t = New refcounted <thing> (tn, chordID(0));  
    mud->insert (t, wrap (done_insert_object, t), true);
    r->place (t);
  }

  mud->insert (r, wrap (&done_insert_room, r), true);
}

void 
done_insert_object (ref<mud_obj> o, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nObject " << o->get_name () << " insertion successful!\n";
  } else
    cout << "Object insertion error: stat = " << stat << "\n";
}

void 
done_insert_room (ref<room> r, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nRoom " << r->get_name () << " insertion successful!\n";
    rl.push_back (r);
    if (rcount < MAX_ROOMS) {
      char rname [50];
      sprintf (rname, "%s%d", "r", ++rcount);
      str rn (rname);
      insert_room (rn);
      cout << "Inserted " << rcount << " rooms.\n";
    } else 
      connect_rooms ();
  } else
    cout << "Room insertion error: stat = " << stat << "\n";
}

void 
insert_next_room (int i, mud_stat stat) 
{
  if (!stat) {
    cout << "Connected room " << i << "\n";
    if (i < MAX_ROOMS) 
      mud->insert (rl[i], wrap (&insert_next_room, i+1));
  } else {
    cout << "Room insertion error: stat = " << stat << "\n";
  }
}

void
connect_rooms ()
{
  for (int s=0; s<MAX_ROOMS; s+=10)
    for (int i=s; i<s+9; i++) {
      rl[i]->east.set_name (rl[i+1]->get_name ().cstr (), rl[i+1]->get_name ().len ());
      rl[i+1]->west.set_name (rl[i]->get_name ().cstr (), rl[i]->get_name ().len ());     
    }
  for (int i=0; i<10; i++) 
    for (int s=i; s<MAX_ROOMS-10; s+=10) {
      rl[s]->south.set_name (rl[s+10]->get_name ().cstr (), rl[s+10]->get_name ().len ());      
      rl[s+10]->north.set_name (rl[s]->get_name ().cstr (), rl[s]->get_name ().len ());          
    }
  insert_next_room (0, mud_stat (0));
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

  char rname [50];
  sprintf (rname, "%s%d", "r", 0);
  str rn (rname);
  
  insert_room (rn);
  
  amain ();
}
