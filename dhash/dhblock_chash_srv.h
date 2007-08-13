#include <dhblock_srv.h>

struct block_info;

struct adb_keyaux_t;
enum adb_status;

// Internal implementation of content hash repair_job logic.
class rjchash;
class rjchashsend;

class dhblock_chash_srv : public dhblock_srv {
  friend class rjchash;
  friend class rjchashsend;

  ptr<adb> cache_db;
  timecb_t *cache_sync_tcb;
  void cache_sync_timer ();
  void cache_sync_timer_cb (adb_status stat);

  void maintqueue (const vec<maint_repair_t> &repairs);

public:
  dhblock_chash_srv (ptr<vnode> node, ptr<dhashcli> cli,
      str msock, str dbsock, str dbname, ptr<chord_trigger_t> t);
  ~dhblock_chash_srv ();

  void store (chordID k, str d, u_int32_t expire, cb_dhstat cb);
  void generate_repair_jobs ();
};

