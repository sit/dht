#include <dhblock_replicated_srv.h>

class dhblock_keyhash_srv : public dhblock_replicated_srv
{
  void real_store (chordID key, str od, str nd, cb_dhstat cb);
  void real_repair (blockID key, ptr<location> me, u_int32_t *myaux, ptr<location> them, u_int32_t *theiraux);
  void delete_cb (chordID k, str d, u_int32_t v, cb_dhstat cb, adb_status stat);

public:
  dhblock_keyhash_srv (ptr<vnode> node, ptr<dhashcli> cli,
		       str desc, str dbname, str dbext, cbv donecb);
};
