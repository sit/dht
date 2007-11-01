#ifndef _DHBLOCK_SRV_H_
#define _DHBLOCK_SRV_H_

#include "dhash.h"
#include <dhash_common.h>

#include <qhash.h>
#include <libadb.h>
#include <maint_prot.h>

class vnode;
class dhashcli;
class chord_trigger_t;

struct user_args;

typedef callback<void, dhash_stat>::ptr cb_dhstat;
typedef callback<void, const vec<maint_repair_t> &>::ptr cb_maintrepairs_t;

// Encapsulate logic for repair jobs; when jobs complete
// they should just lose reference count which can trigger
// an optional callback set by setdonecb.
struct repair_job : virtual public refcount {
  const blockID key;
  const ptr<location> where;
  str desc;
  cbv::ptr donecb;
  u_int32_t timeout;
  repair_job (blockID key, ptr<location> w, u_int32_t to = 0);
  virtual ~repair_job () { if (donecb) (*donecb) (); };
  void setdonecb (cbv cb, u_int32_t to = 0);
  void start ();
  virtual void execute () = 0;
};

/** This class serves as the parent for new block storage types */
class dhblock_srv : virtual public refcount {
  timecb_t *repair_tcb;
  void repair_timer ();

  vec<ptr<repair_job> > repair_q;
  void repair_done (str desc);
  void repair_flush_q ();

  // Generic cb_adbstat to cb_dhstat translator
  void adbcb (cb_dhstat cb, adb_status astat); 
  // Maintenance callbacks
  void maintinitcb (ptr<chord_trigger_t> t, maint_status *res, clnt_stat err);
  void maintgetrepairscb (maint_getrepairsres *res,
    cb_maintrepairs_t cbr, clnt_stat err);

 protected:
  const dhash_ctype ctype;
  str prefix () const;

  ptr<adb> db;
  ptr<aclnt> maint;

  const ptr<vnode> node;

  ptr<dhashcli> cli;

  enum {
    REPAIR_OUTSTANDING_MAX = 16,
    REPAIR_QUEUE_MAX = 64
  };

  u_int64_t repair_read_bytes;
  u_int64_t repair_sent_bytes;
  u_int64_t repairs_completed;

  // Repairs that, upon reading the metadata, were for expired objects.
  u_int64_t expired_repairs;

  bhash<str> repairs_queued;
  bhash<str> repairs_inprogress;
  // RepInv:
  //   for i in repair_q: repairs_queued[i.key]
  //   and there are no other repairs queued.
  bool repair_add (ptr<repair_job> job);
  u_int32_t repair_qlength ();

  virtual void generate_repair_jobs () = 0;
  virtual void db_store (chordID k, str d, cb_dhstat cb);
  virtual void db_store (chordID k, str d,
      u_int32_t aux, u_int32_t expire, cb_dhstat cb);

  // Maint RPC helpers and stubs 
  static ptr<aclnt> get_maint_aclnt (str msock);
  void maint_initspace (int efrags, int dfrags, ptr<chord_trigger_t> t);
  void maint_getrepairs (int thresh, int count, chordID start,
    cb_maintrepairs_t cbr);
  ptr<location> maintloc2location (u_int32_t a, u_int32_t b);

  // stats common to all block_srvs
  void base_stats (vec<dstat> &s);

 public:
  dhblock_srv (ptr<vnode> node, ptr<dhashcli> cli,
    dhash_ctype c, str msock, str dbsock, str dbname, bool hasaux,
    ptr<chord_trigger_t> donecb);
  virtual ~dhblock_srv ();

  virtual void start (bool randomize);
  virtual void stop  ();

  virtual void stats (vec<dstat> &s);

  virtual void store (chordID k, str d, u_int32_t expire, cb_dhstat cb) = 0;
  virtual void fetch (chordID k, cb_fetch cb);
  virtual ptr<adb> get_db () { return db; };
};

#endif /* _DHBLOCK_SRV_H_ */
