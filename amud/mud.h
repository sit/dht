#ifndef _MUD_H_
#define _MUD_H_

#include <dbfe.h>
#include <dhashclient.h>
#include "avatar.h"
#include "room.h"

enum mud_stat {
  MUD_OK = 0,
  MUD_NAME_EXISTS = 1,
  MUD_NOENT = 2,
  MUD_UNDEF_ERR = 3
};

typedef callback<void, mud_stat>::ref mud_cb_t;
typedef callback<void, mud_stat, ptr<avatar> >::ref mud_lookup_cb_t;
typedef callback<void, mud_stat, ptr<room> >::ref mud_rlookup_cb_t;

class game_engine {

  ref<dhashclient> dhash;
  ptr<room> main_room;

  void lookup_cb (mud_lookup_cb_t, dhash_stat, ptr<dhash_block>, vec<chordID>);
  void create_lookup_cb (mud_lookup_cb_t, dhash_stat, ptr<dhash_block>, 
			 vec<chordID>);
  void room_lookup_cb (mud_rlookup_cb_t, dhash_stat, ptr<dhash_block>, 
		       vec<chordID>);
  void done_insert (mud_cb_t, dhash_stat, ptr<insert_info>);
  void done_enter_player (ref<avatar>, dhash_stat, ptr<insert_info>);
  
 public:

  game_engine (ref<dhashclient> dh) : 
    dhash (dh) {};
  ~game_engine () {};

  void set_mainroom (ref<room> r) { main_room = r; };
  void lookup (ref<room>, mud_rlookup_cb_t);
  void lookup (str name, mud_lookup_cb_t, bool create = 0);
  void insert (ref<avatar>, mud_cb_t);
  void insert (ref<thing>, mud_cb_t);
  void insert (ref<room>, mud_cb_t);
  void enter_player (ref<avatar>);

};

#endif

