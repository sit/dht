#ifndef _DHC_MISC_H
#define _DHC_MISC_H

#include "dhc.h"

void open_db (ptr<dbfe>, str, dbOptions, str);

void print_error (str, int, int);

void set_new_config (ptr<dhc_propose_arg>, ptr<vnode>, int);

void set_new_config (ptr<dhc_newconfig_arg>, vec<ptr<location> >);

int paxos_cmp (paxos_seqnum_t, paxos_seqnum_t);

static inline ptr<dhc_block> 
to_dhc_block (ptr<dbrec> rec)
{
  ptr<dhc_block> b = New refcounted<dhc_block>;
  //bcopy (rec->value, b->get_bytes, rec->size);
  //reverse as in merkle_misc.h??
  return b;
};

static inline ptr<dbrec> 
to_dbrec (ptr<dhc_block> block)
{
  return NULL;
};

static inline bool
set_ac (vec<chordID> ap, dhc_prepare_resok res)
{
  if (ap.size () == 0) {
    for (uint i=0; i<res.new_config.size (); i++) 
      ap.push_back (res.new_config[i]);
    return true;
  }
  if (ap.size () != res.new_config.size ())
    return false;
  for (uint i=0; i<ap.size (); i++) {
    if (ap[i] != res.new_config[i])
      return false;
  }
  return true;
};

static inline bool
set_ac (vec<chordID> ap, dhc_propose_arg arg)
{
  if (ap.size () == 0) {
    for (uint i=0; i<arg.new_config.size (); i++) 
      ap.push_back (arg.new_config[i]);
    return true;
  }
  if (ap.size () != arg.new_config.size ())
    return false;
  for (uint i=0; i<ap.size (); i++) {
    if (ap[i] != arg.new_config[i])
      return false;
  }
  return true;
};

#endif /* _DHC_MISC_H */



