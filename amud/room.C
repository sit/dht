#include "room.h"

room::room (char *bytes, uint size) : buf (NULL) {
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

}

char *
room::bytes () {
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
}
  
str 
room::to_str () 
{
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
}

void
room::leave (ref<mud_obj> a)
{
  //warn << "Room before delete: " << to_str ();

  uint i;
  for (i=0; i<alist.size (); i++)
    if (alist[i]->get_name () == a->get_name ())
      break;
  vec<ref<mud_obj> > tmp;
  for (uint j=alist.size ()-1; j>i; j--) {
    tmp.push_back (alist.pop_back ());
  }
  if (i < alist.size ())
    alist.pop_back ();
  for (uint j=0; j<tmp.size (); j++)
    alist.push_back (tmp[j]);

  //warn << "Room after delete: " << to_str ();
}
