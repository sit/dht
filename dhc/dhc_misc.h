#ifndef _DHC_MISC_H
#define _DHC_MISC_H

#include "dhc.h"

static void
open_db (ptr<dbfe> mydb, str name, dbOptions opts, str desc)
{
  if (int err = mydb->opendb (const_cast <char *> (name.cstr ()), opts)) {
    warn << desc << ": " << name <<"\n";
    warn << "open_db returned: " << strerror (err) << "\n";
    exit (-1);
  }
};

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

#endif /* _DHC_MISC_H */



