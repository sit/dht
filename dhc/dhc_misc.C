#include "dhc_misc.h"

void
open_db (ptr<dbfe> mydb, str name, dbOptions opts, str desc)
{
  if (int err = mydb->opendb (const_cast <char *> (name.cstr ()), opts)) {
    warn << desc << ": " << name <<"\n";
    warn << "open_db returned: " << strerror (err) << "\n";
    exit (-1);
  }
}

void 
print_error (str where, int err, int dhc_err)
{
  if (err)
    warn << where << ": " << strerror (err) << "\n";
  else
    warn << where << ": " << dhc_err << "\n";
  exit (-1);
}

void
set_new_config (ptr<dhc_propose_arg> arg, ptr<vnode> myNode, uint k)
{
  ptr<vec<chordID> > nodes = New refcounted<vec<chordID> >;
  vec<ptr<location> > replicas = myNode->succs ();

  if (replicas.size () < k) {
    warn << "dhc_misc: succ list smaller than" << k << "replicas\n";
    k = replicas.size ();
  }

  for (uint i=0; i<k; i++)
    nodes->push_back (replicas[i]->id ());
  arg->new_config.set (nodes->base (), nodes->size ());
}
