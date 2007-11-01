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

  chordID last_repair;
  bool maint_pending;

  u_int64_t cache_hits;
  u_int64_t cache_misses;

  ptr<adb> cache_db;

  void maintqueue (const vec<maint_repair_t> &repairs);

public:
  dhblock_chash_srv (ptr<vnode> node, ptr<dhashcli> cli,
      str msock, str dbsock, str dbname, ptr<chord_trigger_t> t);
  ~dhblock_chash_srv ();

  void stats (vec<dstat> &s);
  void store (chordID k, str d, u_int32_t expire, cb_dhstat cb);
  void generate_repair_jobs ();
};

