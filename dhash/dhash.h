#ifndef _DHASH_H_
#define _DHASH_H_
/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <arpc.h>
#include <async.h>
#include <nfs3_prot.h>
#include <dhash_prot.h>
#include <chord_prot.h>
#include <dbfe.h>
#include <callback.h>
#include <refcnt.h>
#include <chord.h>
#include <qhash.h>
#include <sys/time.h>
#include <chord.h>
#include <route.h>
#include <sfscrypt.h>

#include <merkle.h>
#include <merkle_server.h>
#include <merkle_misc.h>

/*
 *
 * dhash.h
 *
 * Include file for the distributed hash service
 */

struct store_cbstate;

typedef callback<void, int, ptr<dbrec>, dhash_stat>::ptr cbvalue;
typedef callback<void, struct store_cbstate *,dhash_stat>::ptr cbstat;
typedef callback<void,dhash_stat>::ptr cbstore;
typedef callback<void,dhash_stat>::ptr cbstat_t;
typedef callback<void, s_dhash_block_arg *>::ptr cbblockuc_t;
typedef callback<void, s_dhash_storecb_arg *>::ptr cbstorecbuc_t;

extern unsigned int MTU;

struct store_chunk {
  store_chunk *next;
  unsigned int start;
  unsigned int end;

  store_chunk (unsigned int s, unsigned int e, store_chunk *n) : next(n), start(s), end(e) {};
  ~store_chunk () {};
};

struct store_state {
  chordID key;
  unsigned int size;
  store_chunk *have;
  char *buf;
  route path;

  ihash_entry <store_state> link;
   
  store_state (chordID k, unsigned int z) : key(k), 
    size(z), have(0), buf(New char[z]) { };

  ~store_state () { 
    delete[] buf; 
    store_chunk *cnext;
    for (store_chunk *c=have; c; c=cnext) {
      cnext = c->next;
      delete c;
    }
  };

  bool addchunk (unsigned int start, unsigned int end, void *base);
  bool iscomplete ();
};

struct dhash_block {
  char *data;
  size_t len;
  long version;
  int hops;
  int errors;
  int retries;
  chordID source;

  ~dhash_block () {  delete [] data; }

  dhash_block (const char *buf, size_t buflen)
    : data (New char[buflen]), len (buflen)
  {
    if (buf)
      memcpy (data, buf, len);
  }
};

struct pk_partial {
  ptr<dbrec> val;
  int bytes_read;
  int cookie;
  ihash_entry <pk_partial> link;

  pk_partial (ptr<dbrec> v, int c) : val (v), 
		bytes_read (0),
		cookie (c) {};
};

struct keyhash_meta {
  long version;
};

class dhashcli;

// This is a big hack to work around the limit on the
// number of args in a wrap.
//
struct XXX_SENDBLOCK_ARGS {
  bigint destID;
  bigint blockID;
  bool last;
  callback<void>::ref cb;

  XXX_SENDBLOCK_ARGS (bigint destID, bigint blockID, bool last, callback<void>::ref cb)
    : destID (destID), blockID (blockID), last (last), cb (cb)
  {}
};

class merkle_server;
class merkle_syncer;

class dhash {
  
  u_int nreplica;
  int kc_delay;
  int rc_delay;
  int ss_mode;
  int pk_partial_cookie;
  
  ptr<dbfe> db;
  ptr<dbfe> keyhash_db;
  ptr<vnode> host_node;
  dhashcli *cli;
  ptr<route_factory> r_factory;

  merkle_server *msrv;
  merkle_tree *mtree;
  qhash<chordID, ptr<merkle_syncer>, hashID> active_syncers;

  chordID replica_syncer_dstID;
  ptr<merkle_syncer> replica_syncer;

  chordID partition_left;
  chordID partition_right;
  ptr<merkle_syncer> partition_syncer;
  ptr<dbEnumeration> partition_enumeration;

  ihash<chordID, store_state, &store_state::key, 
    &store_state::link, hashID> pst;

  ihash<int, pk_partial, &pk_partial::cookie, 
    &pk_partial::link> pk_cache;
  
  qhash<int, cbblockuc_t> bcpt;
  qhash<int, cbstorecbuc_t> scpt;

  unsigned keyhash_mgr_rpcs;

  void sendblock_XXX (XXX_SENDBLOCK_ARGS *a);
  void sendblock (bigint destID, bigint blockID, bool last, callback<void>::ref cb);
  void sendblock_cb (callback<void>::ref cb, dhash_stat err, chordID blockID);

  void keyhash_mgr_timer ();
  void keyhash_mgr_lookup (chordID key, dhash_stat err, chordID host);
  void keyhash_sync_done ();
  void replica_maintenance_timer (u_int index);
  void partition_maintenance_timer ();
  void partition_maintenance_lookup_cb (dhash_stat err, chordID hostID);
  void partition_maintenance_pred_cb (chordID predID, net_address addr, chordstat stat);
  void doRPC_unbundler (chordID ID, RPC_delay_args *args);


  void route_upcall (int procno, void *args, cbupcalldone_t cb);

  void doRPC (chordID ID, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb);

  void dispatch (svccb *sbp);
  void sync_cb ();

  void storesvc_cb (svccb *sbp, s_dhash_insertarg *arg, dhash_stat err);
  void fetch_cb (int cookie, cbvalue cb,  ptr<dbrec> ret);
  dhash_fetchiter_res * block_to_res (dhash_stat err, s_dhash_fetch_arg *arg,
				      int cookie, ptr<dbrec> val);
  void fetchiter_gotdata_cb (cbupcalldone_t cb, s_dhash_fetch_arg *farg,
			     int cookie, ptr<dbrec> val, dhash_stat stat);
  void fetchiter_sbp_gotdata_cb (svccb *sbp, s_dhash_fetch_arg *farg,
				 int cookie, ptr<dbrec> val, dhash_stat stat);
  void sent_block_cb (dhash_stat *s, clnt_stat err);

  void append (ref<dbrec> key, ptr<dbrec> data,
	       s_dhash_insertarg *arg,
	       cbstore cb);
  void append_after_db_store (cbstore cb, chordID k, int stat);
  void append_after_db_fetch (ref<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      int cookie, ptr<dbrec> data, dhash_stat err);
  
  void store (s_dhash_insertarg *arg, cbstore cb);
  void store_cb (store_status type, chord_node sender, chordID key, chordID srcID,
                 int32 nonce, cbstore cb, dhash_stat stat);
  void store_repl_cb (cbstore cb, chord_node sender, chordID srcID,
                      int32 nonce, dhash_stat err);
  void send_storecb (chord_node sender, chordID srcID, uint32 nonce,
                     dhash_stat stat);
  void send_storecb_cacheloc (chordID srcID, uint32 nonce, dhash_stat status,
	                      chordID ID, bool ok, chordstat stat);

  void sent_storecb_cb (dhash_stat *s, clnt_stat err);
  
  void get_keys_traverse_cb (ptr<vec<chordID> > vKeys,
			     chordID mypred,
			     chordID predid,
			     const chordID &key);
  
  void init_key_status ();
  void transfer_initial_keys ();
  void transfer_initial_keys_range (chordID start, chordID succ);
  void transfer_init_getkeys_cb (chordID succ,
				 dhash_getkeys_res *res, 
				 clnt_stat err);
  void transfer_init_gotk_cb (dhash_stat err);

  void update_replica_list ();
  bool isReplica(chordID id);
  void replicate_key (chordID key, cbstat_t cb);
  void replicate_key_cb (int* replicas, int *replica_err,
                         cbstat_t cb, chordID key, dhash_stat err);

  void install_replica_timer ();
  void check_replicas_cb ();
  void check_replicas ();
  void check_replicas_traverse_cb (chordID to, const chordID &key);
  void fix_replicas_txerd (dhash_stat err);

  void change_status (chordID key, dhash_stat newstatus);

  void transfer_key (chordID to, chordID key, store_status stat, 
		     callback<void, dhash_stat>::ref cb);
  void transfer_fetch_cb (chordID to, chordID key, store_status stat, 
			  callback<void, dhash_stat>::ref cb,
			  int cookie, ptr<dbrec> data, dhash_stat err);
  void transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_stat status, chordID blockID);

  void get_key (chordID source, chordID key, cbstat_t cb);
  void get_key_got_block (chordID key, cbstat_t cb, dhash_stat err, ptr<dhash_block> block, route path);
  void get_key_stored_block (cbstat_t cb, int err);
  
  void store_flush (chordID key, dhash_stat value);
  void store_flush_cb (int err);
  void cache_flush (chordID key, dhash_stat value);
  void cache_flush_cb (int err);

  void transfer_key_cb (chordID key, dhash_stat err);

  char responsible(const chordID& n);

  void printkeys ();
  void printkeys_walk (const chordID &k);
  void printcached_walk (const chordID &k);

  void block_cached_loc (ptr<s_dhash_block_arg> arg, 
			 chordID ID, bool ok, chordstat stat);

  void dbwrite (ref<dbrec> key, ref<dbrec> data);


  chordID pred;
  vec<chordID> replicas;
  timecb_t *check_replica_tcb;
  timecb_t *merkle_rep_tcb;
  timecb_t *merkle_part_tcb;
  timecb_t *keyhash_mgr_tcb;

  /* statistics */
  long bytes_stored;
  long keys_stored;
  long keys_replicated;
  long keys_cached;
  long bytes_served;
  long keys_served;
  long rpc_answered;

 public:
  static ref<dbrec> id2dbrec(chordID id);
  static chordID dbrec2id (ptr<dbrec> r);

  dhash (str dbname, ptr<vnode> node, ptr<route_factory> r_fact,
	 u_int nreplica = 0, int ss_mode = 0);
  void accept(ptr<axprt_stream> x);

  void print_stats ();
  void stop ();
  void fetch (chordID id, int cookie, cbvalue cb);
  void register_block_cb (int nonce, cbblockuc_t cb);
  void unregister_block_cb (int nonce);
  void register_storecb_cb (int nonce, cbstorecbuc_t cb);
  void unregister_storecb_cb (int nonce);

  dhash_stat key_status(const chordID &n);



};

/* verify.C */
bool verify (chordID key, dhash_ctype t, char *buf, int len);
bool verify_content_hash (chordID key,  char *buf, int len);
bool verify_key_hash (chordID key, char *buf, int len);
bool verify_dnssec ();
ptr<dhash_block> get_block_contents (ref<dbrec> d, dhash_ctype t);
ptr<dhash_block> get_block_contents (ptr<dbrec> d, dhash_ctype t);
ptr<dhash_block> get_block_contents (ptr<dhash_block> block, dhash_ctype t);
ptr<dhash_block> get_block_contents (char *data, 
				     unsigned int len, 
				     dhash_ctype t);
dhash_ctype block_type (ptr<dbrec> d);
dhash_ctype block_type (ref<dbrec> d);
dhash_ctype block_type (ref<dhash_block> d);
dhash_ctype block_type (ptr<dhash_block> d);
dhash_ctype block_type (char *value, unsigned int len);

long keyhash_version (ptr<dbrec> data);
long keyhash_version (ref<dbrec> data);
long keyhash_version (ptr<dhash_block> data);
long keyhash_version (ref<dhash_block> data);
long keyhash_version (char *value, unsigned int len);

struct insert_info {
  chordID key;
  chordID destID;
  insert_info (chordID k, chordID d) :
    key (k), destID (d) {};
};

typedef callback<void, dhash_stat, chordID>::ref cbinsert_t;
typedef callback<void, dhash_stat, ptr<insert_info> >::ref cbinsertgw_t;
typedef callback<void, ptr<dhash_block> >::ref cbretrieve_t;
typedef callback<void, dhash_stat, ptr<dhash_block>, route>::ptr cb_ret;
typedef callback<void, dhash_stat, chordID>::ref dhashcli_lookupcb_t;
typedef callback<void, dhash_stat, chordID, route>::ref dhashcli_routecb_t;

#define DHASHCLIENT_USE_CACHED_SUCCESSOR 0x1
#define DHASHCLIENT_NO_RETRY_ON_LOOKUP   0x2

class route_dhash : public virtual refcount {
public:
  route_dhash (ptr<route_factory> f, chordID key, dhash *dh, int options = 0);
  ~route_dhash ();

  void execute (cb_ret cbi, chordID first_hop_guess, u_int retries = 10);
  void execute (cb_ret cbi, u_int retries = 10);
  dhash_stat status () { return result; }
  chordID key () { return blockID; }
  route path ();
  
 private:
  dhash *dh;
  u_int retries;
  route_iterator *chord_iterator;
  int options;
  dhash_stat result;
  chordID blockID;
  cb_ret cb;
  ptr<route_factory> f;
  timecb_t *dcb;
  int nonce;
  int retries_done;
  void block_cb (s_dhash_block_arg *arg);
  void reexecute ();
  void timed_out ();
  void walk (vec<chord_node> succs);
  void walk_cachedloc (vec<chord_node> succs, chordID id, bool ok, chordstat stat);
  void walk_gotblock (vec<chord_node> succs, ptr<dhash_block> block);
  void gotblock (ptr<dhash_block> block);
};

class dhashcli {
  ptr<vnode> clntnode;
  bool do_cache;
  dhash *dh;
  ptr<route_factory> r_factory;

private:
  void doRPC (chordID ID, rpc_program prog, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);

  void lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb,
			   chordID succID, route path, chordstat err);
  void retrieve_hop_cb (cb_ret cb, chordID key, dhash_stat status,
			ptr<dhash_block> block, route path);
  void cache_block (ptr<dhash_block> block, route search_path, chordID key);
  void finish_cache (dhash_stat status, chordID dest);
  void retrieve_with_source_cb (cb_ret cb, dhash_stat status, 
				ptr<dhash_block> block, route path);
  void insert_lookup_cb (chordID blockID, ref<dhash_block> block,
			 cbinsert_t cb, int trial,
			 dhash_stat status, chordID destID);
  void insert_stored_cb (chordID blockID, ref<dhash_block> block,
			 cbinsert_t cb, int trial,
			 dhash_stat stat, chordID retID);
    
 public:
  dhashcli (ptr<vnode> node, dhash *dh, ptr<route_factory> r_factory, 
	    bool do_cache);
  void retrieve (chordID blockID, int options, cb_ret cb);

  void retrieve (chordID source, chordID blockID, cb_ret cb);
  void insert (chordID blockID, ref<dhash_block> block, 
               int options, cbinsert_t cb);
  void storeblock (chordID dest, chordID blockID, ref<dhash_block> block,
		   bool last, cbinsert_t cb, store_status stat = DHASH_STORE);

  void lookup (chordID blockID, int options, dhashcli_lookupcb_t cb);
};



class dhashclient {
private:
  ptr<aclnt> gwclnt;

  // inserts under the specified key
  // (buf need not remain involatile after the call returns)
  void insert (bigint key, const char *buf, size_t buflen, 
	       cbinsertgw_t cb,  dhash_ctype t, int options);
  void insertcb (cbinsertgw_t cb, bigint key, 
		 ptr<dhash_insert_res>, clnt_stat err);
  void retrievecb (cb_ret cb, bigint key,  
		   ref<dhash_retrieve_res> res, clnt_stat err);

public:
  // sockname is the unix path (ex. /tmp/chord-sock) used
  // to communicate to lsd. 
  dhashclient(str sockname);

  void append (chordID to, const char *buf, size_t buflen, cbinsertgw_t cb);

  // inserts under the contents hash. 
  // (buf need not remain involatile after the call returns)
  void insert (const char *buf, size_t buflen, cbinsertgw_t cb, int options = 0);
  void insert (bigint key, const char *buf, size_t buflen, cbinsertgw_t cb,
               int options = 0);

  // insert under hash of public key
  void insert (ptr<sfspriv> key, const char *buf, size_t buflen, long ver,
               cbinsertgw_t cb, int options = 0);
  void insert (sfs_pubkey2 pk, sfs_sig2 sig, const char *buf, size_t buflen,
               long ver, cbinsertgw_t cb, int options = 0);
  void insert (bigint hash, sfs_pubkey2 pk, sfs_sig2 sig,
               const char *buf, size_t buflen, long ver,
	       cbinsertgw_t cb, int options = 0);

  // retrieve block and verify
  void retrieve (bigint key, cb_ret cb, int options = 0);

  // synchronouslly call setactive.
  // Returns true on error, and false on success.
  bool sync_setactive (int32 n);
};

class dhashgateway {
  ptr<asrv> clntsrv;
  ptr<chord> clntnode;
  ptr<dhashcli> dhcli;
  dhash *dh;

  void dispatch (svccb *sbp);
  void insert_cb (svccb *sbp, dhash_stat status, chordID blockID);
  void retrieve_cb (svccb *sbp, dhash_stat status, ptr<dhash_block> block, route path);
  
public:
  dhashgateway (ptr<axprt_stream> x, ptr<chord> clnt, dhash *dh,
		ptr<route_factory> f, bool do_cache = false);
};

bigint compute_hash (const void *buf, size_t buflen);


static inline str dhasherr2str (dhash_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}

#endif
