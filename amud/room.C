#include "room.h"

room::room (char *bytes, uint size, ref<dhashclient> d) : dhash (d), buf (NULL) {
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
#if 1
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
#endif

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
#if 0
  str tname, aname;
  tname << this->get_name () << ".tlist";
  tID = compute_hash (tname.cstr (), tname.len ());
  aname << this->get_name () << ".alist";
  aID = compute_hash (aname.cstr (), aname.len ());
#endif
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
#if 1
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
#endif    
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

void 
room::init (lookup_cb_t cb)
{
  //fetch tlist and alist
  dhash->retrieve (tID, DHASH_NOAUTH, 
		   wrap (this, &room::init_tlist_lookup_cb, cb));
}
 
void 
room::init_tlist_lookup_cb (lookup_cb_t cb, dhash_stat stat, ptr<dhash_block> b,
			    vec<chordID> path)
{
  if (stat == DHASH_OK) {
    
  } else {

  }
}
#if 0
void 
room::init_alist_lookup_cb ()
{

}
#endif 
str 
room::to_str () 
{
  strbuf ret;
  ret << "\n"
      << "  Room Name: " << get_name () << "\n"
      << "         ID: " << ID () << "\n"
      << "Description: " << describe () << "\n"
      << "      North: " << north.get_name () << "\n"
      << "      South: " << south.get_name () << "\n"
      << "       East: " << east.get_name () << "\n"
      << "       West: " << west.get_name () << "\n"
      << "    Objects: " << tlist.size () << " items\n";

  for (uint i=0; i<tlist.size (); i++)
    ret << "      item " << i << ": " << tlist[i]->get_name () << "\n";
    
  ret << "     People: " << alist.size () << " avatars\n";
  for (uint i=0; i<alist.size (); i++)
    ret << "       kid " << i << ": " << alist[i]->get_name () << "\n";
  
  return str (ret);
}

void 
room::place (ref<mud_obj> t)
{
  //insert object into tlist in DHash first
  tlist.push_back (t);
}

void 
room::remove (ref<mud_obj> t)
{
  //remove object from tlist in DHash first
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





