#ifndef __DHBLOCK__
#define __DHBLOCK__

class dhash_block;

struct dhblock {
  static u_long dhash_mtu ();

  virtual ~dhblock () = 0;

  virtual chordID id_to_dbkey (const chordID &k) = 0;

  virtual str generate_fragment (ptr<dhash_block> b, int i) = 0;
  
  virtual int process_download (blockID k, str frag) = 0;
  virtual str produce_block_data () = 0;
  virtual bool done () = 0;

  virtual u_int min_put () = 0;
  virtual u_int num_put () = 0;
  virtual u_int num_fetch () = 0;
  virtual u_int min_fetch () = 0;
};

ptr<dhblock> allocate_dhblock (dhash_ctype c);
vec<str> get_block_contents (str data, dhash_ctype c);
bool verify (chordID key, str data, dhash_ctype c);
bigint compute_hash (const void *buf, size_t buflen);
store_status get_store_status (dhash_ctype ctype);
#endif
