#ifndef __DHBLOCK_REPLICATED__
#define __DHBLOCK_REPLICATED__

#include "dhblock.h"

struct dhblock_replicated : public dhblock {
  static u_int num_replica ();

  str result_str;
  bool done_flag;

  dhblock_replicated () : done_flag (false) {};

  chordID id_to_dbkey (const chordID &k);

  int process_download (blockID k, str frag);
  str produce_block_data ();
  bool done () { return done_flag; };
  
  str generate_fragment (ptr<dhash_block> block, int n);

  u_int min_put () { return 1; };
  u_int num_put () { return num_replica (); };
  u_int num_fetch () { return num_replica (); };
  u_int min_fetch () { return 1; };
};

#endif
