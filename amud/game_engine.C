#include <iostream.h>
#include "mud.h"
#include "verify.h"
#include "dhash_common.h"

void 
game_engine::lookup (str name, mud_lookup_cb_t cb, bool create)
{
  bigint key = compute_hash (name.cstr (), name.len ());
  ptr<option_block> opt = New refcounted<option_block>;
  opt->flags = DHASHCLIENT_NO_RETRY_ON_LOOKUP;
  if (create)
    dhash->retrieve (key, DHASH_NOAUTH, 
		     wrap (this, &game_engine::create_lookup_cb, cb), opt); 
  else 
    dhash->retrieve (key, DHASH_NOAUTH, 
		     wrap (this, &game_engine::lookup_cb, cb), opt);
}

void 
game_engine::create_lookup_cb (mud_lookup_cb_t cb, dhash_stat stat, ptr<dhash_block> blk, 
			       vec<chordID> path)
{
  if (stat == DHASH_NOENT) {
    (*cb) (MUD_OK, NULL);
  } else 
    if (stat == DHASH_OK)
      (*cb) (MUD_NAME_EXISTS, NULL);
    else 
      (*cb) (MUD_UNDEF_ERR, NULL);
}

void 
game_engine::lookup_cb (mud_lookup_cb_t cb, dhash_stat stat, ptr<dhash_block> blk, 
			vec<chordID> path)
{
  if (stat == DHASH_OK) {
    ptr<avatar> a = New refcounted<avatar> (blk->data, blk->len, dhash);
    (*cb) (MUD_OK, a);
  } else 
    if (stat == DHASH_NOENT) 
      (*cb) (MUD_NOENT, NULL);
    else 
      (*cb) (MUD_UNDEF_ERR, NULL);
}

void 
game_engine::lookup (ref<room> r, mud_rlookup_cb_t cb)
{
  dhash->retrieve (r->ID (), DHASH_NOAUTH, 
		   wrap (this, &game_engine::room_lookup_cb, cb));
}

void 
game_engine::room_lookup_cb (mud_rlookup_cb_t cb, dhash_stat stat, ptr<dhash_block> blk, 
			     vec<chordID> path)
{
  if (stat == DHASH_OK) {
    ptr<room> r = New refcounted<room> (blk->data, blk->len, dhash);
    (*cb) (MUD_OK, r);
  } else 
    if (stat == DHASH_NOENT) 
      (*cb) (MUD_NOENT, NULL);
    else 
      (*cb) (MUD_UNDEF_ERR, NULL);
}

void 
game_engine::insert (ref<avatar> a, mud_cb_t cb)
{
  ptr<option_block> opt = New refcounted <option_block>;
  opt->flags = DHASHCLIENT_NEWBLOCK;
  warn << "game_engine::insert" << a->to_str ();

  dhash->insert (a->ID (), a->bytes (), a->size (), 
		 wrap (this, &game_engine::done_insert, cb), opt, DHASH_NOAUTH);
}

void 
game_engine::insert (ref<thing> t, mud_cb_t cb)
{
  ptr<option_block> opt = New refcounted <option_block>;
  opt->flags = DHASHCLIENT_NEWBLOCK; // + DHASHCLIENT_RMW;

  //warn << "game_engine::insert" << t->to_str ();

  dhash->insert (t->ID (), t->bytes (), t->size (), 
		 wrap (this, &game_engine::done_insert, cb), opt, DHASH_NOAUTH);
}

void
game_engine::insert (ref<room> r, mud_cb_t cb)
{
  ptr<option_block> opt = New refcounted <option_block>;
  opt->flags = DHASHCLIENT_NEWBLOCK; // + DHASHCLIENT_RMW;

  warn << "game_engine::insert" << r->to_str ();

  dhash->insert (r->ID (), r->bytes (), r->size (), 
		 wrap (this, &game_engine::done_insert, cb), opt, DHASH_NOAUTH);
}

void 
game_engine::done_insert (mud_cb_t cb, dhash_stat stat, ptr<insert_info> i)
{
  if (stat == DHASH_OK) 
    (*cb) (MUD_OK);
  else 
    (*cb) (MUD_UNDEF_ERR);
}

void 
game_engine::done_enter_player (ref<avatar> a, dhash_stat stat, ptr<insert_info> i)
{
  if (stat == DHASH_OK) { 
    //a->enter (main_room);
    a->play ();
  } else {
    //should have latest version of room returned
    //retry the room insertion with 
  } 
}

void
game_engine::enter_player (ref<avatar> a)
{
  cout << "Entering first room\n";
  main_room->enter (a);
  dhash->insert (main_room->ID (), main_room->bytes (), main_room->size (),
		 wrap (this, &game_engine::done_enter_player, a), NULL, DHASH_NOAUTH);
}



