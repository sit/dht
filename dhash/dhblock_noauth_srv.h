#ifndef __DHBLOCK_NOAUTH_SRV__
#define __DHBLOCK_NOAUTH_SRV__

#include "dhblock_replicated_srv.h"
#include "merkle_hash.h"

class dhblock_noauth_srv : public dhblock_replicated_srv
{
public:
  
  dhblock_noauth_srv (ptr<vnode> node, 
		      ptr<dhashcli> cli,
		      str desc, str dbname, str dbext);

  bool is_block_stale (str prev, str d) { return false; };
  void bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg);
  void store (chordID key, str new_data, cbi cb);
  void fetch (chordID k, cb_fetch cb);

  static chordID get_merkle_key (chordID key, str data);
  static chordID get_database_key (chordID key);
private:

  qhash<chordID, vec<cbv> *, hashID> _paused_stores;

  void iterate_cb (adb_status stat, vec<chordID> keys);  
  void iterate_fetch_cb (adb_status stat, chordID key, str data);
  str merge_data (chordID key, str new_data, str old_data);

  void repair_retrieve_cb (blockID dbkey,  
			   dhash_stat err, 
			   ptr<dhash_block> b, 
			   route r);

  void repair_send_cb (dhash_stat err, bool something);
  bool repair (blockID k, ptr<location> to);
  void store_after_fetch_cb (str new_data, cbi cb, adb_status err,
			     chordID dbkey, str old_data);
  void after_delete (chordID key, str data, cbi cb, int err);
  void finish_store (chordID key);
};

#endif
