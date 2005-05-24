#ifndef _DHBLOCK_SRV_H_
#define _DHBLOCK_SRV_H_

class dbfe;
class dbrec;
struct dbOptions;

class vnode;
class dhashcli;

struct user_args;
struct dhash_offer_arg;
struct dhash_bsmupdate_arg;

/** This class serves as the parent for new block storage types */
class dhblock_srv {
 protected:
  ptr<dbfe> db;
  const ptr<vnode> node;
  const str desc;

  virtual void sync_cb ();
  timecb_t *synctimer;

  ptr<dhashcli> cli;

 public:
  dhblock_srv (ptr<vnode> node, str desc, str dbname,
               dbOptions opts);
  virtual ~dhblock_srv ();

  virtual void start (bool randomize);
  virtual void stop  ();

  virtual void stats (vec<dstat> &s);
  virtual const strbuf &key_info (const strbuf &sb);

  virtual bool key_present (const blockID &n);

  virtual dhash_stat store (chordID k, ptr<dbrec> d) = 0;
  virtual ptr<dbrec> fetch (chordID k);

  virtual void offer (user_args *sbp, dhash_offer_arg *arg);
  virtual void bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg);
};

#endif /* _DHBLOCK_SRV_H_ */
