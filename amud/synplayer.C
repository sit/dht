#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

ptr<dhashclient> dhash;
game_engine *mud;

void done_insert (ptr<avatar> a, int, mud_stat stat);
void really_done (ref<avatar> a, int, mud_stat);
void play (ref<avatar> a);
void done_look (mud_stat, ptr<room>);

void 
new_player (str name, int i)
{
  str pw ("");
  ref<avatar> a = New refcounted<avatar> (name, pw, dhash);
  mud->insert (a, wrap (&done_insert, a, i), true);
}

void 
done_insert (ptr<avatar> a, int i, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nAvatar " << a->get_name () << " creation successful!\n";
    mud->enter_player (a, i, wrap (&really_done, a, i));
  } else
    cout << "Avatar creation error: stat = " << stat << "\n";
}

void 
really_done (ref<avatar> a, int i, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "Insert success!!\\n";
    char rname[50];
    sprintf (rname, "%s%d", "r", i);
    ref<room> l = New refcounted<room> (str (rname), dhash);
    a->enter (l);
    play (a);
  } 
}

void 
play (ref<avatar> a) 
{
  //pick from move (dest), look, and touch (sth)

  //look
  mud->lookup (a->loc (), wrap (&done_look));

  //touch 
  
}

void 
done_look (mud_stat stat, ptr<room> r)
{
  if (stat == MUD_OK)
    cout << "done_look at current room Received " << r->size () << " bytes\n";
  else 
    cout << "done_look err mud_stat: " << stat << "\n";
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
  str name ("a0");
  new_player (name, 0);

  amain ();
}

