#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

ptr<dhashclient> dhash;
game_engine *mud;
ptr<avatar> a;

void done_insert (int, mud_stat);
void really_done (int, mud_stat);
void play (mud_stat);
void done_look (mud_stat, ptr<room>);
void done_touch (ref<thing>, mud_stat);

void 
done_insert (int i, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nAvatar " << a->get_name () << " creation successful!\n";
    mud->enter_player (a, i, wrap (&really_done, i));
  } else
    cout << "Avatar creation error: stat = " << stat << "\n";
}

void 
really_done (int i, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "Insert success!!\n";
    char rname[50];
    sprintf (rname, "%s%d", "r", i);
    ref<room> l = New refcounted<room> (str (rname), dhash);
    a->enter (l);
    mud->insert (ref<avatar> (a), wrap (&play));
  } 
}

float 
get_random ()
{
  long rn = random ();
  float rat = rn;
  rat = rat / RAND_MAX;
  return rat;
}

void 
play (mud_stat stat)
{
  assert (stat == 0);
  //pick from move (dest), look, and touch (sth)
  bool look=0, touch=0, move=0;
  float ratio = get_random ();
  if (ratio < 0.5)
    look = 1;
  if (ratio >= 0.5 && ratio < 0.75)
    touch = 1;
  if (ratio >= 0.75)
    move = 1;
  
  if (look)
    mud->lookup (a->loc (), wrap (&done_look));
  if (touch) {
    cout << "Current room " << a->loc ()->to_str () << "\n";
    uint n = a->loc ()->things ().size ();
    if (n > 0) {
      int i = int (get_random () * n);
      cout << "item to touch " << i << "\n";
      ref<thing> t = New refcounted<thing> (a->loc ()->things ()[i]->get_name (),
					    chordID (a->ID ()));
      mud->insert (t, wrap (&done_touch, t));
    } else {
      cout << "Nothing to touch.\n";
      play (mud_stat (0));
    }
  }
  if (move) {
    cout << "moving\n";
    play (mud_stat (0));
  }
}

void 
done_look (mud_stat stat, ptr<room> r)
{
  if (stat == MUD_OK) {
    a->enter (r);
    cout << "done_look at current room Received " << r->size () << " bytes\n";
    play (mud_stat (0));
  } else 
    cout << "done_look err mud_stat: " << stat << "\n";
}

void
done_touch (ref<thing> t, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "done_touch object "<< t->get_name () 
	 << "Inserted " << t->size () << " bytes\n";
    play (mud_stat (0));
  } else 
    cout << "done_touch err mud_stat: " << stat << "\n"; 
}

void 
done_alook (mud_stat stat, ptr<avatar> av)
{
  if (stat == MUD_OK) {
    a = av;
    play (mud_stat (0));
  } else 
    cout << "done_alook err mud_stat: " << stat << "\n";    
}

static void
usage ()
{
  warnx << "usage: " << progname << " sock init[=0]\n";
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
  bool init = 0;
  if (argc > 2)
    init = atoi(argv[2]);

  mud = New game_engine (dhash);
  int i = 0;
  str name ("a0"), pw ("");

  if (init) {
    a = New refcounted<avatar> (name, pw, dhash);
    mud->insert (ref<avatar> (a), wrap (&done_insert, i), true);
  } else
    mud->lookup (name, wrap (&done_alook));

  amain ();
}

