#include <chord_types.h>
#include <location.h>
#include <id_utils.h>
#include <configurator.h>
#include "block_status.h"
#include "misc_utils.h"

u_long
num_efrags ()
{
  static bool initialized = false;
  static int v = 0;
  if (!initialized) {
    initialized = Configurator::only ().get_int ("dhash.efrags", v);
    assert (initialized);
  }
  return v;
}

void
block_status::missing_on (ptr<location> l)
{
  for (size_t i = 0; i < missing.size (); i++)
    if (missing[i]->id () == l->id ()) {
      assert (missing[i] == l);
      return;
    }
  missing.push_back (l);
}

void
block_status::found_on (ptr<location> l)
{
  size_t last = missing.size () - 1;
  for (size_t i = 0; i < missing.size (); i++) {
    assert (missing[i]);
    if (missing[i]->id () == l->id ()) {
      missing[i] = missing[last];
      missing.pop_back ();
      return;
    }
  }
}

bool
block_status::is_missing_on (chordID n) 
{
  for (size_t i = 0; i < missing.size (); i++)
    if (missing[i]->id () == n) 
      return true;
  return false;
}

void
block_status::print ()
{
  warn << "status of " << id << ": missing on ";
  for (u_int i = 0; i < missing.size (); i++)
    {
      warn << missing[i]->id() << " ";
    }
  warn << "\n";
}

block_status_manager::block_status_manager (chordID me) : my_id (me)
{
}

void
block_status_manager::add_block (const chordID &b)
{
  if (blocks[b] != NULL)
    return;

  block_status *bs = New block_status (b);
  blocks.insert (bs);
  sblocks.insert (bs);
}

void
block_status_manager::del_block (const chordID &b)
{
  block_status *bs = blocks[b];
  if (bs == NULL)
    return;

  blocks.remove (bs);
  sblocks.remove (bs->id);
  delete bs;
}

void
block_status_manager::missing (ptr<location> remote, const chordID &b)
{
  if (blocks[b] == NULL) {
    add_block (b);
  }
  blocks[b]->missing_on (remote);
}

void
block_status_manager::unmissing (ptr<location> remote, const chordID &b)
{
  if (blocks[b] == NULL)
    return;
  blocks[b]->found_on (remote);
}

chordID
block_status_manager::first_block ()
{
  if (sblocks.size () == 0)
    return chordID (0);
  
  block_status *bs = sblocks.closestsucc (my_id);
  return bs->id;
}

chordID
block_status_manager::next_block (const chordID &b)
{
  if (sblocks.size () == 0)
    return chordID (0);

  block_status *bs = sblocks.closestsucc (incID(b));
  if (!bs)
    bs = sblocks.first ();
  return bs->id;
}

const vec<ptr<location> >
block_status_manager::where_missing (const chordID &b)
{
  block_status *bs = blocks[b];
  if (bs == NULL) {
    vec<ptr<location> > nothing;
    return nothing; /* silently fail... */
  }

  return bs->missing;
}

bool 
block_status_manager::missing_on (const chordID &b, chordID &n)
{
  block_status *bs = blocks[b];
  if (bs == NULL) return false;
  vec<ptr<location> > m = bs->missing;
   for (u_int i = 0; i < m.size (); i++)
     if (m[i]->id () == n) return true;
   return false;
}

const vec<chordID>
block_status_manager::missing_what (const chordID &n)
{
  vec<chordID > ret;
  block_status *bs = blocks.first ();
  while (bs) {
    if (bs->is_missing_on (n)) {
      ret.push_back (bs->id);
    }
    bs = blocks.next(bs);
  }
  return ret;
}

const ptr<location> 
block_status_manager::best_missing (const chordID &b, vec<ptr<location> > succs)
{
  vec<ptr<location> > m = where_missing (b);
  assert (m.size () > 0);
  chordID best  = m[0]->id ();
  ptr<location> l = m[0];

  for (u_int i = 0; i < m.size (); i++)
    if (betweenbothincl (my_id, best, m[i]->id ())
	&& in_vector (succs, b)) {
      best = m[i]->id ();
      l = m[i];
    }

  return l;
}

u_int 
block_status_manager::pcount (const chordID &b, vec<ptr<location> > succs)
{
  int present = 0;
  //for every successor, assume present if not in missing list
  // and if succ # < dhash::efrags
  block_status *bs = blocks[b];
  if (!bs) return num_efrags ();
  vec<ptr<location> > missing = bs->missing;

  char is_missing;
  // FED: removed below from for termination clause; allow holes by 
  //      testing all successors
  //      && s < num_efrags ()

  for (u_int s = 0; 
       s < succs.size ();
       s++) 
    {      
      is_missing = 0;
      for (u_int m=0; m < missing.size (); m++)
	{
	  if (missing[m]->id () == succs[s]->id ()) is_missing = 1;
	}
      if (!is_missing) present++;
    }

  return present;
}

u_int
block_status_manager::mcount (const chordID &b)
{
  block_status *bs = blocks[b];
  if (bs == NULL)
    return 0;
  else
    return bs->missing.size ();
}

void
block_status_manager::print ()
{
  block_status *bs = blocks.first ();
  while (bs) 
    {
      bs->print ();
      bs = blocks.next (bs);
    }
}
