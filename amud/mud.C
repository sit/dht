#include <iostream.h>
#include <string.h>
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include "mud.h"

ptr<dhashclient> dhash;
game_engine *mud;

void main_loop ();
void done_lookup (str, mud_stat, ptr<avatar>);
void done_insert (ptr<avatar>, mud_stat);
void play_done_lookup (ref<str>, mud_stat, ptr<avatar>);

void 
display_welcome_msg () 
{
  cout << "\n\n";
  cout << "********************************************\n";
  cout << "*                                          *\n";
  cout << "*                 A MUD                    *\n";
  cout << "*                                          *\n";
  cout << "*      You are about to be brainwashed.    *\n";
  cout << "*                                          *\n";
  cout << "********************************************\n";
}

void 
display_options ()
{
  cout << "\n\n";
  cout << "    There are xxx people currently online.  \n";
  cout << "\n";
  cout << "      1. Enter the game.\n";
  cout << "      2. Create a new avatar.\n";
  cout << "      3. Read help files.\n";
  cout << "      4. Quit.\n";
  cout << "\n";
  cout << "Enter an option: ";
}

void
create_new_char ()
{
  cout << "\n\nWhat would you like as the name of your new avatar?\n\n";
  //delay (10);
  cout << "Keep in mind that this is a role-playing, fantasy game,\n";
  cout << "and the game keepers reserve the right to prohibit any name\n";
  cout << "considered unfitting.\n\n";

  cout << "Enter your avatar's name: ";
  char *name = (char *) malloc (100);
  cin >> name;
  str avn(name);
  free (name);

  mud->lookup (avn, wrap (&done_lookup, avn), true);
}

void
done_lookup (str name, mud_stat stat, ptr<avatar> a)
{
  if (stat == MUD_NAME_EXISTS) {
    cout << "\n\n An avatar of that name exists.\n";
    main_loop ();
  } else 
    if (stat == MUD_OK) {
      char *passwd = (char *) malloc (200);
      char *passwd_conf = (char *) malloc (200);
    
      cout << "Great! Your name is " << name << "\n\n";
      cout << "Enter your new password: ";
      cin >> passwd; 
      cout << "Confirm new password: ";
      cin >> passwd_conf;
    
      if (strcmp (passwd, passwd_conf) == 0) {
	str pw (passwd);
	ref<avatar> a = New refcounted<avatar> (name, pw, dhash);
	mud->insert (a, wrap (&done_insert, a));
      } else {
	cout << "\nPassword mismatch!\n";
	main_loop ();
      }

      free (passwd);
      free (passwd_conf);
    }
}
 
void
done_insert (ptr<avatar> a, mud_stat stat)
{
  if (stat == MUD_OK) {
    cout << "\nAvatar creation successful!\n";
    mud->enter_player (a);
  }
}

void 
play_game ()
{
  cout << "Enter the name of your avatar: ";
  char *input = (char *) malloc (100);
  cin >> input;
  str avn(input);
  cout << "Password: " ;
  cin >> input;
  ref<str> passwd = New refcounted<str> (input);
  free (input);
  
  mud->lookup (avn, wrap (&play_done_lookup, passwd));
}

void
play_done_lookup (ref<str> passwd, mud_stat stat, ptr<avatar> a)
{
  if (stat == MUD_OK) {
    warn << "play_done_lookup " << a->to_str ();
    if (a->pw () == *passwd)
      mud->enter_player (a);
    else {
      cout << "Wrong password.\n";
      main_loop ();
    }
  } else {
    if (stat == MUD_NOENT) {
      cout << "An avatar of that name does not exist.\n";
      main_loop ();
    }
  }
}

void 
done_insert_object (ref<mud_obj> o, mud_stat stat)
{
  if (stat == MUD_OK)
    cout << "\n" << o->get_name () << " inserted.\n";
  else 
    cout << "\n" << o->get_name () << " insert error stat " << stat << "\n";
}

ref<room> insert_things ()
{
  ref<thing> t1 = New refcounted <thing> (str("Cranberry Muffin"));
  mud->insert (t1, wrap (&done_insert_object, t1));
  ref<thing> t2 = New refcounted <thing> (str("Boston Creme Pie"));
  mud->insert (t2, wrap (&done_insert_object, t2));

  ref<room> r1 = New refcounted <room> (str("First room"));
  r1->place (t1);
  r1->place (t2);
  //cout << r1->to_str ();

  ref<room> r2 = New refcounted <room> (str("Second room"));
  r2->east.set_name (r1->get_name ().cstr (), r1->get_name ().len ());
  r1->west.set_name (r2->get_name ().cstr (), r2->get_name ().len ());  

  mud->insert (r1, wrap (&done_insert_object, r1));
  mud->insert (r2, wrap (&done_insert_object, r2));

  return r1;
}

void 
main_loop ()
{

  display_options ();
  
  int i;
  cin >> i;
  
  switch (i) {
  case 1:
    cout << "Great you want to play!\n";
    play_game ();
    break;
  case 2:
    create_new_char ();
    break;
  case 3:
    cout << "Unimplemented. Would you like to help?\n";
    break;
  case 4:
    cout << "We are freezing your brain for whackey experiments...\n";
    exit (0);
  default:
    cout << "Invalid option\n";
    //main_loop ();
  };

}

void
start_game (mud_stat stat, ptr<room> r)
{
  if (stat == MUD_OK) {
    mud->set_mainroom (r);
    display_welcome_msg ();
    main_loop ();
  }
}

static void
usage ()
{
  warnx << "usage: " << progname << " sock [init?=0]\n";
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
    init = atoi(argv[2]) ? 1 : 0;

  mud = New game_engine (dhash);

  if (init) {
    ref<room> mainroom = insert_things ();
    mud->set_mainroom (mainroom);
    display_welcome_msg ();
    main_loop ();
  } else {
    ref<room> mr = New refcounted<room> (str("First room"));
    mud->lookup (mr, wrap (&start_game));
  }

  amain ();

}


