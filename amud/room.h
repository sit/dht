#ifndef _ROOM_H_
#define _ROOM_H_

#include <vec.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include "mud_obj.h"
#include "thing.h"

typedef callback<void, dhash_stat>::ref lookup_cb_t;
typedef callback<void, dhash_stat, ptr<mud_obj> >::ref tlookup_cb_t;

class room: public mud_obj {
  
  ref<dhashclient> dhash;
  chordID tID;
  vec<ref<mud_obj> > tlist;
  chordID aID;
  vec<ref<mud_obj> > alist; //List of avatars
  char *buf;

  uint nsize () {    
    uint sz = 4*USZ + north.get_name ().len () 
      + south.get_name ().len ()
      + east.get_name ().len ()
      + west.get_name ().len ();
    return sz;
  }

#if 1
  uint tsize () {
    uint sz = 0;
    for (uint i=0; i<tlist.size (); i++)
      sz += USZ + tlist[i]->get_name ().len ();
    return USZ + sz;
  };

  uint asize () {
    uint sz = 0;
    for (uint i=0; i<alist.size (); i++)
      sz += USZ + alist[i]->get_name ().len ();
    return USZ + sz;
  };
#endif

  void init_tlist_lookup_cb (lookup_cb_t, dhash_stat, ptr<dhash_block>,
			     vec<chordID>);

 public:
  mud_obj north;
  mud_obj south;
  mud_obj east;
  mud_obj west;

  room (str n, ref<dhashclient> d) : mud_obj (n), dhash (d), buf (NULL), 
    north (str("")), south (str("")), east (str("")), west (str("")) { 
#if 0
    str tname, aname;
    tname << this->get_name () << ".tlist";
    tID = compute_hash (tname.cstr (), tname.len ());
    aname << this->get_name () << ".alist";
    aID = compute_hash (aname.cstr (), aname.len ());
#endif
  };
  
  room (char *, uint, ref<dhashclient> d);
  ~room () { };
  
  void init (lookup_cb_t); 
  char *bytes ();
  uint size () {
    return (2*USZ + get_name ().len () + describe ().len () + nsize ()
	    + tsize () + asize ());
  };
  
  void place (ref<mud_obj> t); 
  void remove (ref<mud_obj> t);
  void enter (ref<mud_obj> a) { alist.push_back (a); };
  void leave (ref<mud_obj> a);

  vec<ref<mud_obj> > avatars () { return alist; };
  vec<ref<mud_obj> > things () { return tlist; };

  str to_str ();
};

#endif


