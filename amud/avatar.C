#include <iostream.h>
#include <dhashclient.h>
#include "avatar.h"

avatar::avatar (str n, str p, ref<dhashclient> d, ptr<room> l=NULL) : 
  mud_obj (n), dhash (d), passwd (p), buf (NULL), location (l)
{
  if (!location)
    location = New refcounted <room> (str("First room"));
}

avatar::avatar (char *bytes, uint size, ref<dhashclient> d) : 
  dhash (d), buf (NULL)
{
  uint offst = 0;
  uint slen;
  bcopy (bytes, &slen, USZ);
  offst += USZ;
  set_name (bytes + offst, slen);
  offst += slen;
  bcopy (bytes + offst, &slen, USZ);
  offst += USZ;
  set_desc (bytes + offst, slen);
  offst += slen;
  bcopy (bytes + offst, &slen, USZ);
  offst += USZ;
  passwd.setbuf (bytes + offst, slen);
  offst += slen;
  uint num_inv;
  bcopy (bytes + offst, &num_inv, USZ);
  offst += USZ;

  uint64 ver;
  chordID writer;
  for (uint i=0; i<num_inv; i++) {
    bcopy (bytes + offst, &ver, USZ);
    offst += USZ;
    bcopy (bytes + offst, &writer, ID_SIZE);
    offst += ID_SIZE;
    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    str n (bytes + offst, slen);

    ptr<thing> inv = New refcounted<thing> (n, writer, ver);
    inventory.push_back (inv);
    offst += slen;
  }

  bcopy (bytes + offst, &slen, USZ);
  offst += USZ;
  str n (bytes + offst, slen);
  location = New refcounted<room> (n);
}

char *
avatar::bytes ()
{
  if (buf) free (buf);

  uint offst = 0;
  uint slen;
  buf = (char *) malloc (size ());

  slen = get_name ().len ();
  bcopy (&slen , buf + offst, USZ);
  offst += USZ;
  bcopy (get_name ().cstr (), buf + offst, slen);
  offst += slen;

  slen = describe ().len ();
  bcopy (&slen, buf + offst, USZ);
  offst += USZ;
  bcopy (describe ().cstr (), buf + offst, slen);
  offst += slen;

  slen = passwd.len ();
  bcopy (&slen, buf + offst, USZ);
  offst += USZ;
  bcopy (passwd.cstr (), buf + offst, slen);
  offst += slen;
  slen = inventory.size ();
  bcopy (&slen, buf + offst, USZ);
  offst += USZ;

  for (uint i=0; i<inventory.size (); i++) {
    bcopy (&inventory[i]->ctag.ver, buf + offst, USZ);
    offst += USZ;
    bcopy (&inventory[i]->ctag.writer, buf + offst, ID_SIZE);
    offst += ID_SIZE;

    slen = inventory[i]->get_name ().len ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;
    bcopy (inventory[i]->get_name ().cstr (), buf + offst, slen);
    offst += slen;
  }
  
  slen = location->get_name ().len ();
  bcopy (&slen, buf + offst, USZ);
  offst += USZ;
  bcopy (location->get_name ().cstr (), buf + offst, slen);
  
  return buf;
}

uint 
avatar::inv_size ()
{
  uint sz = 0;
  for (uint i=0; i<inventory.size (); i++)
    sz += USZ + inventory[i]->get_name ().len ();
  return sz;
}

uint
avatar::size () 
{
  return ( 5*USZ + get_name ().len () + describe ().len () + passwd.len () + 
	   inv_size () + location->get_name ().len () );
}

str
avatar::to_str ()
{
  strbuf ret;
  ret << "\n"
      << "Avatar Name: " << get_name () << "\n"
      << "         ID: " << ID () << "\n"
      << "Description: " << describe () << "\n"
      << "   Password: " << passwd << "\n"
      << "  Inventory: " << inventory.size () << " items.\n";

  for (uint i=0; i<inventory.size (); i++) {
    ret << "      item " << i << ": " << inventory[i]->get_name () << "\n";
  }

  ret << "   Location: " << location->get_name () << "\n";
  return str (ret);
}

void
avatar::enter (ref<room> r)
{
  location = r;
}

void 
avatar::play ()
{
  //TODO: Check status if there are any incomplete ops,
  //      like moving rooms, taking things, ...

  str command = read_input ();
  if (!strncasecmp (command.cstr (), "LOOK", 4))
    dhash->retrieve (location->ID (), DHASH_NOAUTH, 
		     wrap (this, &avatar::look));
  else 
    if (!strncasecmp (command.cstr (), "GET", 3))
      get (command);
    else 
      if (!strncasecmp (command.cstr (), "WEST", 4) || 
	  !strncasecmp (command.cstr (), "EAST", 4) || 
	  !strncasecmp (command.cstr (), "NORTH", 5) || 
	  !strncasecmp (command.cstr (), "SOUTH", 5)) 
	move (command);
      else {
	cout << "Huh??\n";
	play ();
      }
}

void
avatar::look (dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path)
{
  //Later should change so that gets location info from cache,
  //and cache is refreshed often.

  if (stat == DHASH_OK) {
    location = New refcounted<room> (blk->data, blk->len);
    cout << "You are in the " << location->get_name () << ".\n";
    if (location->north.get_name ().len () > 0)
      cout << "To the north, is the " << location->north.get_name () << "\n";    
    if (location->south.get_name ().len () > 0)
      cout << "To the south, is the " << location->south.get_name () << "\n";    
    if (location->east.get_name ().len () > 0)
      cout << "To the east, is the " << location->east.get_name () << "\n";    
    if (location->west.get_name ().len () > 0)
      cout << "To the west, is the " << location->west.get_name () << "\n";

    for (uint i=0; i<location->avatars ().size (); i++) 
      cout << location->avatars ()[i]->get_name () << " is here.\n";

    for (uint i=0; i<location->things ().size (); i++)
      cout << "There is a " << location->things ()[i]->get_name () << ".\n";

    play ();
  } else {

  }
}

void 
avatar::move (str command)
{
  ref<room> next = New refcounted<room> (str(""));
  if (!strncasecmp (command.cstr (), "WEST", 4) && 
      location->west.get_name ().len ())
    next->set_name (location->west.get_name ().cstr (), 
		    location->west.get_name ().len ());    
  else 
    if (!strncasecmp (command.cstr (), "EAST", 4) && 
	location->east.get_name ().len ())
      next->set_name (location->east.get_name ().cstr (), 
		      location->east.get_name ().len ());    
    else 
      if (!strncasecmp (command.cstr (), "NORTH", 5) && 
	  location->north.get_name ().len ())
	next->set_name (location->north.get_name ().cstr (), 
			location->north.get_name ().len ());    
      else 
	if (!strncasecmp (command.cstr (), "SOUTH", 5) && 
	    location->south.get_name ().len ())
	  next->set_name (location->south.get_name ().cstr (),
			  location->south.get_name ().len ());    

  if (next->get_name ().len ()) {
    //TODO: change state to limbo
    dhash->retrieve (location->ID (), DHASH_NOAUTH, 
		     wrap (this, &avatar::done_move_lookup, next));
  } else {
    cout << "You can't go that way!\n";
    play ();
  }
}

void
avatar::done_move_lookup (ref<room> next, dhash_stat stat, ptr<dhash_block> blk, 
			  vec<chordID> path)
{
  if (stat == DHASH_OK) {
    location = New refcounted<room> (blk->data, blk->len);
    ref<mud_obj> a = New refcounted<mud_obj> (get_name ());
    location->leave (a);
  
    dhash->insert (location->ID (), location->bytes (), location->size (),
		   wrap (this, &avatar::done_remove, next), NULL, DHASH_NOAUTH);
  } 
}

void
avatar::done_remove (ref<room> next, dhash_stat stat, ptr<insert_info> i)
{
  if (stat == DHASH_OK) {
    location = next;
    dhash->retrieve (next->ID (), DHASH_NOAUTH, 
		     wrap (this, &avatar::done_enter_lookup));
  }
}

void 
avatar::done_enter_lookup (dhash_stat stat, ptr<dhash_block> blk, 
			   vec<chordID> path)
{
  if (stat == DHASH_OK) {
    ref<room> next = New refcounted<room> (blk->data, blk->len);
    ref<mud_obj> a = New refcounted<mud_obj> (get_name ());
    next->enter (a);
    location = next;
    dhash->insert (location->ID (), location->bytes (), location->size (),
		   wrap (this, &avatar::done_enter), NULL, DHASH_NOAUTH);
  }
} 

void
avatar::done_enter (dhash_stat stat, ptr<insert_info> i)
{
  if (stat == DHASH_OK)
    dhash->insert (ID (), bytes (), size (),
		   wrap (this, &avatar::done_enter_cb), NULL, DHASH_NOAUTH);
}

void
avatar::done_enter_cb (dhash_stat stat, ptr<insert_info> i)
{
  if (stat == DHASH_OK)
    play ();
}

void 
avatar::get (str command)
{
  char num[100];
  strcpy (num, command.cstr () + 4);
  int k = atoi (num);

  cout << "Getting object " << k << ".\n";
  //location->remove (); do a set delete operation on room and if success,
  //then add thing to inventory.
  
}

str
avatar::read_input ()
{
  cout << "\nType something: ";
  char input[100]; 
  cin.getline (input, 100);
  str command(input);
  return command;
}
