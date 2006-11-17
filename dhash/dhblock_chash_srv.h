#include <dhblock_srv.h>

class pmaint;
struct block_info;

struct adb_keyaux_t;
enum adb_status;

// Internal implementation of content hash repair_job logic.
class rjchash;

class dhblock_chash_srv : public dhblock_srv {
  friend class rjchash;
  ptr<adb> cache_db;

  pmaint *pmaint_obj;

  void localqueue (u_int32_t frags, clnt_stat err, adb_status stat, vec<block_info> keys);

public:
  dhblock_chash_srv (ptr<vnode> node, ptr<dhashcli> cli, str dbname, str dbext,
      str desc, cbv donecb);
  ~dhblock_chash_srv ();

  void start (bool randomize);
  void stop  ();

  void store (chordID k, str d, cb_dhstat cb);
  void offer (user_args *sbp, dhash_offer_arg *arg);
  void stats (vec<dstat> &s);
  void generate_repair_jobs ();
};

