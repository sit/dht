#ifndef _ROOM_H_
#define _ROOM_H_

#include "vec.h"
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

  room (str n) : mud_obj (n), north(str("")), south(str("")), 
    east(str("")), west(str("")) { };
  
  room (char *bytes, uint size) : buf (NULL) {
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

    uint n;
    bcopy (bytes + offst, &n, USZ);
    offst += USZ;
    for (uint i=0; i<n; i++) {
      bcopy (bytes + offst, &slen, USZ);
      offst += USZ;
      str m (bytes + offst, slen);
      ref<mud_obj> t = New refcounted<mud_obj> (m);
      tlist.push_back (t);
      offst += slen;
    }

    bcopy (bytes + offst, &n, USZ);
    offst += USZ;
    for (uint i=0; i<n; i++) {
      bcopy (bytes + offst, &slen, USZ);
      offst += USZ;
      str m (bytes + offst, slen);
      ref<mud_obj> a = New refcounted<mud_obj> (m);
      alist.push_back (a);
      offst += slen;
    }
    
    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    north.set_name (bytes + offst, slen);
    offst += slen;

    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    south.set_name (bytes + offst, slen);
    offst += slen;

    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    east.set_name (bytes + offst, slen);
    offst += slen;

    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    west.set_name (bytes + offst, slen);

  };

  uint size () {
    return (4*USZ + get_name ().len () + describe ().len () + 
	    tsize () + asize () + nsize ());
  };

  char *bytes () {
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

    slen = tlist.size ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;

    for (uint i=0; i<tlist.size (); i++) {
      slen = tlist[i]->get_name ().len ();
      bcopy (&slen, buf + offst, USZ);
      offst += USZ;
      bcopy (tlist[i]->get_name ().cstr (), buf + offst, slen);
      offst += slen;
    }

    slen = alist.size ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;

    for (uint i=0; i<alist.size (); i++) {
      slen = alist[i]->get_name ().len ();
      bcopy (&slen, buf + offst, USZ);
      offst += USZ;
      bcopy (alist[i]->get_name ().cstr (), buf + offst, slen);
      offst += slen;
    }    
    
    slen = north.get_name ().len ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;
    bcopy (north.get_name ().cstr (), buf + offst, slen);
    offst += slen;

    slen = south.get_name ().len ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;
    bcopy (south.get_name ().cstr (), buf + offst, slen);
    offst += slen;

    slen = east.get_name ().len ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;
    bcopy (east.get_name ().cstr (), buf + offst, slen);
    offst += slen;

    slen = west.get_name ().len ();
    bcopy (&slen, buf + offst, USZ);
    offst += USZ;
    bcopy (west.get_name ().cstr (), buf + offst, slen);
    offst += slen;

    return buf;
  };

  ~room () { };
  
  void place (ref<mud_obj> t) { tlist.push_back (t); };
  void enter (ref<mud_obj> a) { alist.push_back (a); };
  void remove (ref<mud_obj> a) { /*alist.remove (a);*/ }

  vec<ref<mud_obj> > avatars () { return alist; };
  vec<ref<mud_obj> > things () { return tlist; };

  str to_str () {
    strbuf ret;
    ret << "\n"
	<< "  Room Name: " << get_name () << "\n"
	<< "         ID: " << ID () << "\n"
	<< "Description: " << describe () << "\n"
	<< "    Objects: " << tlist.size () << " items\n";

    for (uint i=0; i<tlist.size (); i++)
      ret << "      item " << i << ": " << tlist[i]->get_name () << "\n";
    
    ret << "     People: " << alist.size () << " avatars\n";
    for (uint i=0; i<alist.size (); i++)
      ret << "       kid " << i << ": " << alist[i]->get_name () << "\n";

    return str (ret);
  };
};

#endif
