#ifndef _THING_H_
#define _THING_H_

#include "mud_obj.h"
#include "room.h"

class thing: public mud_obj {
  
  ptr<room> location;
  char *buf;

 public:
  thing (str n, ptr<room> l=NULL) : mud_obj (n), buf (NULL) { };

  thing (char *bytes, uint size) : buf (NULL) {
    uint offst = 0;
    uint slen;
    bcopy (bytes, &slen, USZ);
    offst += USZ;
    set_name (bytes + offst, slen);
    offst += slen;
    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    set_desc (bytes + offst, slen);
  };

  uint size () {
    return ( 2*USZ + get_name ().len () + describe ().len ());
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
  
    return buf;
  }; 

  ~thing () {};
  
  str to_str () {
    strbuf ret;
    ret << "\n"
	<< "Object Name: " << get_name () << "\n"
	<< "         ID: " << ID () << "\n"
	<< "Description: " << describe () << "\n";

    return str (ret);
  };
};

#endif
