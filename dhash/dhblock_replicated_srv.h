#ifndef __DHBLOCK_REPLICATED_SRV__
#define __DHBLOCK_REPLICATED_SRV__

#include <dhblock_srv.h>
#include <chord.h>
#include <adb_prot.h>

class location;
class merkle_tree;
class merkle_server;

class dhblock_replicated_srv : public dhblock_srv
{
private:
  
  void  delete_cb (chordID k, str d, cbi cb, int stat);
  void store_fetch_cb (cbi cb, str d,
			adb_status stat, 
			chordID key,
			str prev);  
protected:
  const dhash_ctype ctype;
  unsigned nrpcsout;

  merkle_server *msrv;
  merkle_tree *mtree;

  vec<ptr<location> > replicas;
  void update_replica_list ();

  timecb_t *checkrep_tcb;
  int checkrep_interval;
  void checkrep_timer ();
  void checkrep_lookup (chordID key,
			vec<chord_node> hostsl, route r,
			chordstat err);
  void checkrep_sync_done (dhash_stat stat, chordID k, bool present);

  virtual bool is_block_stale (str prev, str d) = 0;

  merkle_server * mserv () { return msrv; };

public:
  dhblock_replicated_srv (ptr<vnode> node, str dbname, str dbext, str desc,
                          dhash_ctype ctype);
  ~dhblock_replicated_srv ();

  void start (bool randomize) {};
  void stop  () {};

  void store (chordID key, str d, cbi cb);

};

#endif
