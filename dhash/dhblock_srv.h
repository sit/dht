#ifndef _DHBLOCK_SRV_H_
#define _DHBLOCK_SRV_H_

#include "dhash.h"
#include "libadb.h"
#include <dhash_common.h>

class vnode;
class dhashcli;

struct user_args;
struct dhash_offer_arg;
struct dhash_bsmupdate_arg;
struct merkle_server;

struct repair_state {
  ihash_entry <repair_state> link;
  bigint hashkey;
  const blockID key;
  const ptr<location> where;
  repair_state (blockID key, ptr<location> w) :
    hashkey (key.ID), key (key), where (w) {};
};

/** This class serves as the parent for new block storage types */
class dhblock_srv : virtual public refcount {
 protected:
  ptr<adb> db;
  const ptr<vnode> node;
  const str desc;

  enum { REPAIR_OUTSTANDING_MAX = 15 };

  virtual void sync_cb ();
  timecb_t *synctimer;

  ptr<dhashcli> cli;

  u_int32_t repair_outstanding;
  ihash<bigint, repair_state, &repair_state::hashkey, &repair_state::link, 
    hashID> repair_q;

 public:
  dhblock_srv (ptr<vnode> node, ptr<dhashcli> cli,
	       str desc, str dbname, str dbext);
  virtual ~dhblock_srv ();

  virtual void start (bool randomize);
  virtual void stop  ();

  virtual void stats (vec<dstat> &s);
  virtual const strbuf &key_info (const strbuf &sb);

  //DDD  virtual bool key_present (const blockID &n);

  virtual void store (chordID k, str d, cbi cb) = 0;
  virtual void fetch (chordID k, cb_fetch cb);
  virtual ptr<adb> get_db () { return db; };
  virtual void offer (user_args *sbp, dhash_offer_arg *arg);
  virtual void bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg);

  virtual merkle_server *mserv () { return NULL; };

  virtual void repair_done ();
  virtual bool repair (blockID k, ptr<location> to);
};

#endif /* _DHBLOCK_SRV_H_ */
