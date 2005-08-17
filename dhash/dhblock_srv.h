#ifndef _DHBLOCK_SRV_H_
#define _DHBLOCK_SRV_H_

#include "dhash.h"
#include "libadb.h"

class vnode;
class dhashcli;

struct user_args;
struct dhash_offer_arg;
struct dhash_bsmupdate_arg;
struct merkle_server;

/** This class serves as the parent for new block storage types */
class dhblock_srv {
 protected:
  ptr<adb> db;
  const ptr<vnode> node;
  const str desc;

  virtual void sync_cb ();
  timecb_t *synctimer;

  ptr<dhashcli> cli;

 public:
  dhblock_srv (ptr<vnode> node, str desc, str dbname, str dbext);
  virtual ~dhblock_srv ();

  virtual void start (bool randomize);
  virtual void stop  ();

  virtual void stats (vec<dstat> &s);
  virtual const strbuf &key_info (const strbuf &sb);

  //DDD  virtual bool key_present (const blockID &n);

  virtual void store (chordID k, str d, cbi cb) = 0;
  virtual void fetch (chordID k, cb_fetch cb);

  virtual void offer (user_args *sbp, dhash_offer_arg *arg);
  virtual void bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg);

  virtual merkle_server *mserv () { return NULL; };
};

#endif /* _DHBLOCK_SRV_H_ */
