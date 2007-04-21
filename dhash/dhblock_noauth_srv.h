#ifndef __DHBLOCK_NOAUTH_SRV__
#define __DHBLOCK_NOAUTH_SRV__

#include "dhblock_replicated_srv.h"

class dhblock_noauth_srv : public dhblock_replicated_srv
{
  void localqueue (clnt_stat err, adb_status stat, vec<block_info> blocks);
  void after_delete (chordID key, str data, cb_dhstat cb, adb_status err);

  void real_store (chordID key, str od, str nd, cb_dhstat cb);
  void real_repair (blockID key, ptr<location> me, u_int32_t *myaux, ptr<location> them, u_int32_t *theiraux);

public:
  dhblock_noauth_srv (ptr<vnode> node, 
		      ptr<dhashcli> cli,
		      str msock, str dbsock, str dbname,
		      ptr<chord_trigger_t> t);
};

#endif
