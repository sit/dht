#include <chord_types.h>
#include <location.h>
#include <id_utils.h>
#include "block_status.h"

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
  size_t last = missing.size ();
  for (size_t i = 0; i < missing.size (); i++)
    if (missing[i]->id () == l->id ()) {
      missing[i] = missing[last];
      missing.pop_back ();
      return;
    }
}

block_status_manager::block_status_manager ()
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
block_status_manager::next_block (const chordID &b)
{
  if (sblocks.size () == 0)
    return chordID (0);

  block_status *bs = sblocks.closestsucc (b);
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

int
block_status_manager::mcount (const chordID &b)
{
  block_status *bs = blocks[b];
  if (bs == NULL)
    return 0;
  else
    return bs->missing.size ();
}
