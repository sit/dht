#ifndef _ROOM_H_
#define _ROOM_H_

#include "vec.h"
#include "dhash_common.h"
#include "mud_obj.h"

class room: public mud_obj {
  
  vec<ref<mud_obj> > tlist; //List of things
  vec<ref<mud_obj> > alist; //List of avatars
  char *buf;

  uint nsize () {    
    uint sz = 4*USZ + north.get_name ().len () 
      + south.get_name ().len ()
      + east.get_name ().len ()
      + west.get_name ().len ();
    return sz;
  }

  uint tsize () {
    uint sz = 0;
    for (uint i=0; i<tlist.size (); i++)
      sz += USZ + tlist[i]->get_name ().len ();
    return sz;
  };

  uint asize () {
    uint sz = 0;
    for (uint i=0; i<alist.size (); i++)
      sz += USZ + alist[i]->get_name ().len ();
    return sz;
  };
  
 public:
  mud_obj north;
  mud_obj south;
  mud_obj east;
  mud_obj west;

  room (str n) : mud_obj (n), buf (NULL), north(str("")), south(str("")), 
    east(str("")), west(str("")) { };
  
  room (char *, uint);
  ~room () { };

  char *bytes ();
  uint size () {
    return (4*USZ + get_name ().len () + describe ().len () + 
	    tsize () + asize () + nsize ());
  };
  
  void place (ref<mud_obj> t) { tlist.push_back (t); };
  void remove (ref<mud_obj> t) {};
  void enter (ref<mud_obj> a) { alist.push_back (a); };
  void leave (ref<mud_obj> a);

  vec<ref<mud_obj> > avatars () { return alist; };
  vec<ref<mud_obj> > things () { return tlist; };

  str to_str ();
};

#endif
