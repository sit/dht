#ifndef _MUD_OBJ_H_
#define _MUD_OBJ_H_

#include "chord.h"
#include "verify.h"

#define ID_SIZE sha1::hashsize
#define USZ sizeof (uint)

class mud_obj {

  bigint id;
  str name;
  str description;

 public:
  mud_obj () : id (0), description (str("A brilliant thug")) {};
  mud_obj (str n) : name (n), description (str("Another brilliant thug")) { 
    id = compute_hash (name.cstr (), name.len ());
  };
  ~mud_obj () {};
  
  str get_name () { return name; };
  void set_name (const char *bytes, uint len) { 
    name.setbuf (bytes, len); 
    id = compute_hash (name.cstr (), name.len ());
  };
  bigint ID () { return id; };
  str describe () { return description; };
  void set_desc (const char *bytes, uint len) { 
    description.setbuf (bytes, len); 
  }

  uint sz () { return (name.len () + description.len ()); } 

  str to_str () {
    strbuf ret;
    ret << "       Name: " << name << "\n"
	<< "         ID: " << id << "\n"
	<< "Description: " << description << "\n";

    return str (ret);
  };
};

#endif
