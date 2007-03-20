#include "dhblock.h"
#include "dhash.h"

struct dhblock_chash : public dhblock {
  static u_int num_efrags ();
  static u_int num_dfrags ();

  vec<str> frags;
  str result_str;
  bool done_flag;

  dhblock_chash () : done_flag (false) {};

  chordID id_to_dbkey (const chordID &k) { return k; }

  int process_download (blockID k, str frag);
  str produce_block_data ();
  bool done () { return done_flag; };
  
  str generate_fragment (ptr<dhash_block> block, int n);

  u_int min_put () { return num_dfrags (); };
  u_int num_put () { return num_efrags (); };
  u_int num_fetch ();
  u_int min_fetch () { return num_dfrags (); };

  static bool verify (chordID key, str data);
  static vec<str> get_payload (str data);
  static str marshal_block (str data);
};
