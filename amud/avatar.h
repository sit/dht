#ifndef _AVATAR_H_
#define _AVATAR_H_

#include "dhash_common.h"
#include "str.h"
#include "chord.h"
#include "room.h"
#include "thing.h"
#include "mud_obj.h"

class avatar: public mud_obj {

  ref<dhashclient> dhash;
  str passwd;
  char *buf;
  vec<ptr<thing> > inventory;
  ptr<room> location;
  
  uint inv_size ();
  void look (dhash_stat, ptr<dhash_block>, vec<chordID>);
  void get (str);
  void move (str);
  void done_remove (ref<room>, dhash_stat, ptr<insert_info>);
  void done_enter_lookup (dhash_stat stat, ptr<dhash_block> blk, 
			  vec<chordID> path);
  void done_enter (dhash_stat, ptr<insert_info>);
  void done_enter_cb (dhash_stat, ptr<insert_info>);

 public:
  avatar (str, str, ref<dhashclient>, ptr<room> l=NULL);
  avatar (char *, uint, ref<dhashclient>);
  ~avatar () { if (buf) free (buf); }
  char *bytes ();
  uint size ();

  str pw () { return passwd; };
  void enter (ref<room>);
  void play ();
  str read_input ();
  str to_str ();
};

#endif
