#ifndef _THING_H_
#define _THING_H_

#include "mud_obj.h"
#include "room.h"

class thing: public mud_obj {
  
  ptr<room> location;
  char *buf;

 public:
  tag_t ctag; //current version, for rmw updates

  thing (str n, chordID writer, uint64 ver=0, ptr<room> l=NULL) : 
    mud_obj (n), buf (NULL) {
    ctag.ver = ver;
    ctag.writer = writer;
  };

  thing (char *bytes, uint size) : buf (NULL) {
    uint offst = 0;
    uint slen;

    bcopy (bytes, &ctag.ver, USZ);
    offst += USZ;
    bcopy (bytes + offst, &ctag.writer, ID_SIZE);
    offst += ID_SIZE;

    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    set_name (bytes + offst, slen);
    offst += slen;
    bcopy (bytes + offst, &slen, USZ);
    offst += USZ;
    set_desc (bytes + offst, slen);
  };

  uint size () {
    return ( 3*USZ + ID_SIZE + get_name ().len () + describe ().len ());
  }; 

  char *bytes () {
    if (buf) free (buf);

    uint offst = 0;
    uint slen;
    buf = (char *) malloc (size ());

    bcopy (&ctag.ver, buf + offst, USZ);
    offst += USZ;
    bcopy (&ctag.writer, buf + offst, ID_SIZE);
    offst += ID_SIZE;
    
    slen = get_name ().len ();
    bcopy (&slen, buf + offst, USZ);
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
	<< "        tag: <" << ctag.ver << ", " << ctag.writer << ">\n"
	<< "Description: " << describe () << "\n";

    return str (ret);
  };
};

#endif
