#ifndef _DHBLOCK_SRV_H_
#define _DHBLOCK_SRV_H_

#include "dhash.h"
#include <dhash_common.h>

#include <qhash.h>
#include <libadb.h>

class vnode;
class dhashcli;

struct user_args;
struct dhash_offer_arg;

typedef callback<void, dhash_stat>::ptr cb_dhstat;

// Encapsulate logic for repair jobs; when jobs complete
// they should just lose reference count which can trigger
// an optional callback set by setdonecb.
struct repair_job : virtual public refcount {
  const blockID key;
  const ptr<location> where;
  cbv::ptr donecb;
  u_int32_t timeout;
  repair_job (blockID key, ptr<location> w, u_int32_t to = 0) :
    key (key), where (w), donecb (NULL), timeout (to) {};
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
  void repair_done (blockID key);
  void repair_flush_q ();

  // Generic cb_adbstat to cb_dhstat translator
  void adbcb (cb_dhstat cb, adb_status astat); 

 protected:
  ptr<adb> db;

  const ptr<vnode> node;
  const str desc;

  enum {
    REPAIR_OUTSTANDING_MAX = 16,
    REPAIR_QUEUE_MAX = 64
  };

  ptr<dhashcli> cli;

  bhash<blockID, bhashID> repairs_queued;
  bhash<blockID, bhashID> repairs_inprogress;
  // RepInv:
  //   for i in repair_q: repairs_queued[i.key]
  //   and there are no other repairs queued.
  bool repair_add (ptr<repair_job> job);
  u_int32_t repair_qlength ();

  virtual void generate_repair_jobs () = 0;
  virtual void db_store (chordID k, str d, cb_dhstat cb);
  virtual void db_store (chordID k, str d, u_int32_t aux, cb_dhstat cb);

  cbv donecb;

 public:
  dhblock_srv (ptr<vnode> node, ptr<dhashcli> cli,
	       str desc, str dbpath, str dbtype, bool hasaux, cbv donecb);
  virtual ~dhblock_srv ();

  virtual void start (bool randomize);
  virtual void stop  ();

  virtual void stats (vec<dstat> &s);

  virtual void store (chordID k, str d, cb_dhstat cb) = 0;
  virtual void fetch (chordID k, cb_fetch cb);
  virtual ptr<adb> get_db () { return db; };
  virtual void offer (user_args *sbp, dhash_offer_arg *arg);
};

#endif /* _DHBLOCK_SRV_H_ */
