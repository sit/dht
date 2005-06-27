#ifndef _MERKLE_SYNCER_H_
#define _MERKLE_SYNCER_H_

#include "merkle_hash.h"
#include "merkle_node.h"
#include "merkle_tree.h"
#include "merkle_sync_prot.h"
#include <bigint.h>

// see comment in merkle_syncer::doRPC for this work around.
struct RPC_delay_args;
typedef callback<void, RPC_delay_args *>::ref rpcfnc_t;
typedef callback<void, bigint, bool, bool>::ref missingfnc_t;

class merkle_syncer {
 private:
  strbuf log;

  void setdone ();
  void error (str err);
  void missing (const merkle_hash &key);

 public:
  dhash_ctype ctype;
  merkle_tree *ltree; // local tree
  rpcfnc_t rpcfnc;
  missingfnc_t missingfnc;

  str fatal_err;
  bool sync_done;

  bigint local_rngmin;
  bigint local_rngmax;

  bigint remote_rngmin;
  bigint remote_rngmax;

  vec<pair<merkle_rpc_node, int> > st;

  merkle_syncer (dhash_ctype ctype,
		 merkle_tree *ltree, rpcfnc_t rpcfnc, missingfnc_t missingfnc);
  ~merkle_syncer () {}
  void dump ();
  str getsummary ();
  void doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb);
  void send_some ();
  void next (void);

  bool done () { return sync_done; }
  void sync (bigint rngmin, bigint rngmax);
  void sendnode (u_int depth, const merkle_hash &prefix);
  void sendnode_cb (ref<sendnode_arg> arg, ref<sendnode_res> res, 
		    clnt_stat err);
};


// ---------------------------------------------------------------------------

class merkle_getkeyrange {
private:
  dhash_ctype ctype;
  dbfe *db;
  bigint rngmin;
  bigint rngmax;
  bigint current;
  missingfnc_t missing;
  rpcfnc_t rpcfnc;
  vec<merkle_hash> lkeys;


  void go ();
  void getkeys_cb (ref<getkeys_arg> arg, ref<getkeys_res> res, clnt_stat err);
  void doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb);

public:
  ~merkle_getkeyrange () {}
  merkle_getkeyrange (dhash_ctype ctype, dbfe *db, 
		      bigint rngmin, bigint rngmax, 
		      vec<merkle_hash> plkeys,
		      missingfnc_t missing, rpcfnc_t rpcfnc)
    : ctype (ctype), db (db), rngmin (rngmin), 
      rngmax (rngmax), current (rngmin), 
      missing (missing), rpcfnc (rpcfnc), lkeys (plkeys)
    { go (); }
};


// ---------------------------------------------------------------------------

void
format_rpcnode (merkle_tree *ltree, u_int depth, const merkle_hash &prefix,
		const merkle_node *node, merkle_rpc_node *rpcnode);

void
compare_nodes (merkle_tree *ltree, bigint rngmin, bigint rngmax, 
	       merkle_node *lnode, merkle_rpc_node *rnode,
	       dhash_ctype ctype, missingfnc_t missingfnc, rpcfnc_t rpcfnc);

void
compare_keylists (vec<merkle_hash> lkeys, vec<merkle_hash> rkeys,
		  chordID rngmin, chordID rngmax,
		  missingfnc_t missingfnc);

#endif /* _MERKLE_SYNCER_H_ */
