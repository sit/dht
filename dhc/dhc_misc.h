#ifndef _DHC_MISC_H
#define _DHC_MISC_H

#include "dhc.h"

void open_db (ptr<dbfe>, str, dbOptions, str);

void print_error (str, int, int);

void set_new_config (ptr<dhc_propose_arg>, ptr<vnode>, int);

void set_new_config (ptr<dhc_newconfig_arg>, vec<ptr<location> >);

static inline ptr<dhc_block> 
to_dhc_block (ptr<dbrec> rec)
{
  return NULL;
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

#endif /* _DHC_MISC_H */



