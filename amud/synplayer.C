#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

#define MAX_OPS 100
int op_count = 0;
u_int64_t start_op = 0, end_op = 0;
u_int64_t total_time = 0;

ptr<dhashclient> dhash;
game_engine *mud;
ptr<avatar> a;

void done_insert (int, mud_stat);
void really_done (int, mud_stat);
void play (mud_stat);
void done_look (mud_stat, ptr<room>);
void done_touch (ref<thing>, mud_stat);
void done_move (ref<room> oldroom, int);
void refresh_room (mud_stat, ptr<room>);

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
    mud->lookup (l, wrap (&refresh_room));
  } 
}

void
refresh_room (mud_stat stat, ptr<room> r) {
  if (stat == MUD_OK) {
    a->enter (r);
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

bool
there_is_a_door (int i) 
{
  if (i == 0)
    return (a->loc ()->north.get_name ().len () > 0);
  if (i == 1)
    return (a->loc ()->south.get_name ().len () > 0);
  if (i == 2)
    return (a->loc ()->east.get_name ().len () > 0);
  return (a->loc ()->west.get_name ().len () > 0);
}

void 
report_avg ()
{
  cout << "Total time: " << total_time << " microseconds\n";
  cout << "Number of ops: " << MAX_OPS << "\n";
  cout << "Average time per op: " << total_time / MAX_OPS << " microseconds\n";
  exit (0);
}

void 
play (mud_stat stat)
{
  if (op_count >= MAX_OPS) {
    report_avg (); return;
  }

  assert (stat == 0);
  //pick from move (dest), look, and touch (sth)
  bool look=0, touch=0, move=0;

  float ratio = get_random ();
  if (ratio < 0.5)
    look = 1;
  else 
    if (ratio >= 0.5 && ratio < 0.75)
      touch = 1;
    else
      if (ratio >= 0.75 && ratio < 1)
	move = 1;
      else {
	cout << "No op\n";
	exit (0);
      }

  if (look) {
    op_count++;
    timeval tp;
    gettimeofday (&tp, NULL);
    start_op = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
    
    mud->lookup (a->loc (), wrap (&done_look));
  }
  if (touch) {
    cout << "Current room " << a->loc ()->to_str () << "\n";
    uint n = a->loc ()->things ().size ();
    if (n > 0) {
      op_count++;
      int i = int (get_random () * n);
      cout << "item to touch " << i << "\n";
      timeval tp;
      gettimeofday (&tp, NULL);
      start_op = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;

      ref<thing> t = New refcounted<thing> (a->loc ()->things ()[i]->get_name (),
					    chordID (a->ID ()));
      mud->insert (t, wrap (&done_touch, t));
    } else {
      cout << "Nothing to touch.\n";
      play (mud_stat (0));
    }
  }
  if (move) {
    cout << "moving from " << a->loc ()->to_str ();
    int i;
    do {
      i = int (get_random () * 4);      
    } while (!there_is_a_door (i));

    cout << "to " << i << "\n";

    op_count++;
    timeval tp;
    gettimeofday (&tp, NULL);
    start_op = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;

    chordID id;
    str nextroom;
    if (i == 0) {
      nextroom = str ("NORTH");
      id = compute_hash (a->loc ()->north.get_name ().cstr (), 
			 a->loc ()->north.get_name ().len ());
    }
    if (i == 1) {
      nextroom = str ("SOUTH");
      id = compute_hash (a->loc ()->south.get_name ().cstr (), 
			 a->loc ()->south.get_name ().len ());
    }
    if (i == 2) {
      nextroom = str ("EAST");   
      id = compute_hash (a->loc ()->east.get_name ().cstr (), 
			 a->loc ()->east.get_name ().len ());
    }
    if (i == 3) {
      nextroom = str ("WEST");
      id = compute_hash (a->loc ()->west.get_name ().cstr (), 
			 a->loc ()->west.get_name ().len ());
    }  
    warn << "next room id " << id << "\n";
    a->move (nextroom, wrap (&done_move, a->loc ()));
  }
}

void 
done_move (ref<room> oldroom, int success)
{
  if (success) {
    timeval tp;
    gettimeofday (&tp, NULL);
    end_op = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
    total_time += end_op - start_op;

    cout << "Moved from " << oldroom->get_name () 
	 << " size " << oldroom->size () << " bytes\n"
	 << "        to " << a->loc ()->get_name () 
	 << " size " << a->loc ()->size () << " bytes\n";
    cout << "duration " << end_op - start_op << " microseconds\n";
    //mud->insert (ref<avatar> (a), wrap (&play));
    play (mud_stat (0));
  } else
    cout << "move failed\n";
}

void 
done_look (mud_stat stat, ptr<room> r)
{
  if (stat == MUD_OK) {
    a->enter (r);

    timeval tp;
    gettimeofday (&tp, NULL);
    end_op = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
    total_time += end_op - start_op;

    cout << "done_look at current room Received " << r->size () << " bytes\n";
    cout << "duration " << end_op - start_op << " microseconds\n";    

    play (mud_stat (0));
  } else 
    cout << "done_look err mud_stat: " << stat << "\n";
}

void
done_touch (ref<thing> t, mud_stat stat)
{
  if (stat == MUD_OK) {
    timeval tp;
    gettimeofday (&tp, NULL);
    end_op = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;
    total_time += end_op - start_op;

    cout << "done_touch object "<< t->get_name () 
	 << "Inserted " << t->size () << " bytes\n";
    cout << "duration " << end_op - start_op << " microseconds\n";

    play (mud_stat (0));
  } else 
    cout << "done_touch err mud_stat: " << stat << "\n"; 
}

void 
done_alook (mud_stat stat, ptr<avatar> av)
{
  if (stat == MUD_OK) {
    a = av;
    mud->lookup (a->loc (), wrap (&refresh_room));
  } else 
    cout << "done_alook err mud_stat: " << stat << "\n";    
}

static void
usage ()
{
  warnx << "usage: " << progname << " sock player init[=0]\n";
  exit (1);
}

int 
main (int argc, char **argv)
{
  setprogname (argv[0]);

  if (argc < 3)
    usage ();

  str control_socket = argv[1];
  dhash = New refcounted<dhashclient> (control_socket);
  int p = atoi (argv[2]);
  bool init = 0;
  if (argc > 3)
    init = atoi (argv[3]);

  mud = New game_engine (dhash);
  char an[50];
  sprintf (an, "%s%d", "a", p);
  str name (an), pw ("");

  if (init) {
    float ratio = get_random ();
    int i = int (ratio * T_MAX_ROOMS);
    a = New refcounted<avatar> (name, pw, dhash);
    mud->insert (ref<avatar> (a), wrap (&done_insert, i), true);
  } else
    mud->lookup (name, wrap (&done_alook));

  amain ();
}

