#ifndef _DHC_MISC_H
#define _DHC_MISC_H

#include "dhc.h"

void open_db (ptr<dbfe>, str, dbOptions, str);
void print_error (str, int, int);
void set_new_config (dhc_soft *, ptr<dhc_propose_arg>, ptr<vnode>, uint);
void set_new_config (ptr<dhc_newconfig_arg>, vec<chordID>);
void set_locations (vec<ptr<location> > *, ptr<vnode>, vec<chordID>);
int paxos_cmp (paxos_seqnum_t, paxos_seqnum_t);
int tag_cmp (tag_t, tag_t);
void ID_put (char *, chordID);
void ID_get (chordID, char *);

static inline ptr<dhc_block> 
to_dhc_block (ptr<dbrec> rec)
{
  ptr<dhc_block> b = New refcounted<dhc_block> (rec->value, rec->len);
  return b;
};

static inline ptr<dbrec> 
to_dbrec (ptr<dhc_block> block)
{
  ptr<dbrec> rec = New refcounted<dbrec> (block->bytes (), block->size ());
  return rec;
};

static inline bool
set_ac (vec<chordID> *ap, dhc_prepare_resok res)
{
  if (ap->size () == 0) {
    for (uint i=0; i<res.new_config.size (); i++) 
      ap->push_back (res.new_config[i]);
    return true;
  }
  if (ap->size () != res.new_config.size ())
    return false;
  for (uint i=0; i<ap->size (); i++) {
    if ((*ap)[i] != res.new_config[i])
      return false;
  }
  return true;
};

static inline bool
set_ac (vec<chordID> *ap, dhc_propose_arg arg)
{
  if (ap->size () == 0) {
    for (uint i=0; i<arg.new_config.size (); i++) 
      ap->push_back (arg.new_config[i]);
    return true;
  }
  if (ap->size () != arg.new_config.size ())
    return false;
  for (uint i=0; i<ap->size (); i++) {
    if ((*ap)[i] != arg.new_config[i])
      return false;
  }
  return true;
};

static inline bool
set_ac (vec<chordID> *ap, vec<chordID> src)
{
  if (ap->size () == 0) {
    for (uint i=0; i<src.size (); i++) 
      ap->push_back (src[i]);
    return true;
  }
  if (ap->size () != src.size ())
    return false;
  for (uint i=0; i<ap->size (); i++) {
    if ((*ap)[i] != src[i])
      return false;
  }
  return true;  
}
#endif /* _DHC_MISC_H */



