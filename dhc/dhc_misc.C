#include <id_utils.h>
#include <location.h>
#include <locationtable.h>
#include "dhc_misc.h"

void
ID_put (char *buf, chordID id)
{
  bzero (buf, ID_size);
  mpz_get_rawmag_be (buf, ID_size, &id);
};


void
ID_get (chordID *id, char *buf)
{
  mpz_set_rawmag_be (id, (const char *) buf, ID_size);
};

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
    warn << where << ": " << strerror (errno) << "\n";
  else
    warn << where << ": " << dhc_err << "\n";
  exit (-1);
}

void
set_new_config (dhc_soft *b, ptr<dhc_propose_arg> arg, ptr<vnode> myNode, 
		uint k)
{
  if (b->new_config.size () == k)
    return; // Already setup new_config
  else b->new_config.clear ();
  
  vec<ptr<location> > replicas = myNode->succs ();

  if (replicas.size () < k) {
    warn << "dhc_misc: succ list smaller than" << k << "replicas\n";
    k = replicas.size ();
  }

  arg->new_config.setsize (k);
  
  for (uint i=0; i<k; i++) {
    arg->new_config[i] = replicas[i]->id ();
    // Also set the new_config in b's meta data. 
    b->new_config.push_back (replicas[i]);
  }
}

void
set_locations (vec<ptr<location> > *locs, ptr<vnode> myNode, vec<chordID> ids)
{
  ptr<location> l;
  locs->clear ();
  for (uint i=0; i<ids.size (); i++)
    if (l = myNode->locations->lookup (ids[i]))
      locs->push_back (l);
    else warn << "Node " << ids[i] << " does not exist !!!!\n";
}

void
set_new_config (ptr<dhc_newconfig_arg> arg, vec<ptr<location> > new_config)
{
  arg->new_config.setsize (new_config.size ());

  for (uint i=0; i<new_config.size (); i++)
    arg->new_config[i] = new_config[i]->id ();
}

int
paxos_cmp (paxos_seqnum_t a, paxos_seqnum_t b)
{
  if (a.seqnum > b.seqnum)
    return 1;
  if (a.seqnum < b.seqnum)
    return -1;
  if (a.proposer > b.proposer)
    return 1;
  if (a.proposer < b.proposer)
    return -1;
  return 0;
}

int 
tag_cmp (tag_t a, tag_t b)
{
  if (a.ver > b.ver)
    return 1;
  if (a.ver < b.ver)
    return -1;
  if (a.writer > b.writer)
    return 1;
  if (a.writer < b.writer)
    return -1;
  return 0;
}
