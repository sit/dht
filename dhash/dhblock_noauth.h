#ifndef __DHBLOCK_NOAUTH__
#define __DHBLOCK_NOAUTH__

#include "dhash_common.h"
#include "dhblock.h"
#include "dhblock_replicated.h"

struct dhblock_noauth : public dhblock_replicated {
  
  static vec<str> get_payload (str data);
  static vec<str> get_payload (const char *buf, u_int len);
  static str marshal_block (str data);
  static str marshal_block (vec<str> data);

};

#endif
