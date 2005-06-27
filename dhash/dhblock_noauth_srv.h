#ifndef __DHBLOCK_NOAUTH_SRV__
#define __DHBLOCK_NOAUTH_SRV__

#include "dhblock_replicated_srv.h"
#include "merkle_hash.h"

class dhblock_noauth_srv : public dhblock_replicated_srv
{
public:
  
  dhblock_noauth_srv (ptr<vnode> node, str desc, str dbname, dbOptions opts);

  bool is_block_stale (ref<dbrec> prev, ref<dbrec> d) { return false; };
  void bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg);
  dhash_stat store (chordID key, ptr<dbrec> new_data);
  ptr<dbrec> fetch (chordID k);

  static ptr<dbrec> get_merkle_key (chordID key, ptr<dbrec> data);
  static ptr<dbrec> get_database_key (chordID key);
private:
  
  ptr<dbrec> merge_data (chordID key, ptr<dbrec> new_data);

  void repair_retrieve_cb (chordID dbkey,  
			   dhash_stat err, 
			   ptr<dhash_block> b, 
			   route r);

  void repair_send_cb (dhash_stat err, bool something);

};

#endif
