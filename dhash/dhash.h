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
#include <dhash_prot.h>
#include <chord_prot.h>
#include <dbfe.h>
#include <callback.h>
#include <refcnt.h>
#include <chord.h>
#include <qhash.h>
#include <sys/time.h>
#include <chord.h>
/*
 *
 * dhash.h
 *
 * Include file for the distributed hash service
 */

struct store_cbstate;

typedef callback<void, ptr<dbrec>, dhash_stat>::ptr cbvalue;
typedef callback<void, struct store_cbstate *,dhash_stat>::ptr cbstat;
typedef callback<void,dhash_stat>::ptr cbstore;
typedef callback<void,dhash_stat>::ptr cbstat_t;

#define MTU 1024

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

  ~dhash_block () {  delete [] data; }

  dhash_block (const char *buf, size_t buflen)
    : data (New char[buflen]), len (buflen)
  {
    if (buf)
      memcpy (data, buf, len);
  }
};



class dhash {

  int nreplica;
  int kc_delay;
  int rc_delay;
  int ss_mode;

  dbfe *db;
  vnode *host_node;

  ihash<chordID, store_state, &store_state::key, &store_state::link, hashID> pst;

  int qnonce;
  qhash<int, unsigned long> rqc;

  void doRPC (chordID ID, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb);

  void dispatch (svccb *sbp);
  void sync_cb ();

  void storesvc_cb (svccb *sbp, s_dhash_insertarg *arg, dhash_stat err);
  void fetch_cb (cbvalue cb,  ptr<dbrec> ret);
  void fetchiter_svc_cb (svccb *sbp, s_dhash_fetch_arg *farg,
			 ptr<dbrec> val, dhash_stat stat);

  void append (ptr<dbrec> key, ptr<dbrec> data,
	       s_dhash_insertarg *arg,
	       cbstore cb);
  void append_after_db_store (cbstore cb, chordID k, int stat);
  void append_after_db_fetch (ptr<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      ptr<dbrec> data, dhash_stat err);

  void store (s_dhash_insertarg *arg, cbstore cb);
  void store_cb(store_status type, chordID id, cbstore cb, int stat);
  void store_repl_cb (cbstore cb, dhash_stat err);

  void get_keys_traverse_cb (ptr<vec<chordID> > vKeys,
			     chordID mypred,
			     chordID predid,
			     const chordID &key);

  void init_key_status ();
  void transfer_initial_keys ();
  void transfer_init_getkeys_cb (dhash_getkeys_res *res, clnt_stat err);
  void transfer_init_gotk_cb (dhash_stat err);

  void update_replica_list ();
  bool isReplica(chordID id);
  void replicate_key (chordID key, cbstat_t cb);
  void replicate_key_cb (unsigned int replicas_done, cbstat_t cb, chordID key,
			 dhash_stat err);


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
			  ptr<dbrec> data, dhash_stat err);
  void transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_storeres *res, ptr<s_dhash_insertarg> i_arg,
			  chordID to, clnt_stat err);

  void get_key (chordID source, chordID key, cbstat_t cb);
  void get_key_initread_cb (cbstat_t cb, dhash_fetchiter_res *res, 
			    chordID source, 
			    chordID key, clnt_stat err);
  void get_key_read_cb (chordID key, char *buf, unsigned int *read, 
			dhash_fetchiter_res *res, cbstat_t cb, clnt_stat err);
  void get_key_finish (char *buf, unsigned int size, chordID key, cbstat_t cb);
  void get_key_finish_store (cbstat_t cb, int err);

  void store_flush (chordID key, dhash_stat value);
  void store_flush_cb (int err);
  void cache_flush (chordID key, dhash_stat value);
  void cache_flush_cb (int err);

  void transfer_key_cb (chordID key, dhash_stat err);

  char responsible(const chordID& n);

  void printkeys ();
  void printkeys_walk (const chordID &k);
  void printcached_walk (const chordID &k);

  int dbcompare (ref<dbrec> a, ref<dbrec> b);

  ref<dbrec> id2dbrec(chordID id);
  chordID dbrec2id (ptr<dbrec> r);

  chordID pred;
  vec<chordID> replicas;
  timecb_t *check_replica_tcb;

  /* statistics */
  long bytes_stored;
  long keys_stored;
  long keys_replicated;
  long keys_cached;
  long bytes_served;
  long keys_served;
  long rpc_answered;

 public:
  dhash (str dbname, vnode *node, 
	 int nreplica = 0, int ss = 10000, int cs = 1000, int ss_mode = 0);
  void accept(ptr<axprt_stream> x);

  void print_stats ();
  void stop ();
  void fetch (chordID id, cbvalue cb);
  dhash_stat key_status(const chordID &n);

  static bool verify (chordID key, dhash_ctype t, char *buf, int len);
  static bool verify_content_hash (chordID key,  char *buf, int len);
  static bool verify_key_hash (chordID key, char *buf, int len);
  static bool verify_dnssec ();
  static ptr<dhash_block> get_block_contents (ptr<dbrec> d, dhash_ctype t);
  static ptr<dhash_block> get_block_contents (ptr<dhash_block> block, dhash_ctype t);
  static ptr<dhash_block> get_block_contents (char *data, unsigned int len, dhash_ctype t);


  static dhash_ctype block_type (ptr<dbrec> d);

};



typedef callback<void, bool, chordID>::ref cbinsert_t;
typedef cbinsert_t cbstore_t;
typedef callback<void, ptr<dhash_block> >::ref cbretrieve_t;
typedef callback<void, ptr<dhash_res> >::ref dhashcli_lookup_itercb_t;
typedef callback<void, ptr<dhash_storeres> >::ref dhashcli_storecb_t;
typedef callback<void, dhash_stat, chordID>::ref dhashcli_lookupcb_t;


class dhashcli {
  ptr<chord> clntnode;

 private:
  void doRPC (chordID ID, rpc_program prog, int procno, ptr<void> in, void *out, aclnt_cb cb);
  void lookup_iter_with_source_cb (ptr<dhash_fetchiter_res> fres, dhashcli_lookup_itercb_t cb, clnt_stat err);
  void lookup_iter_cb (chordID blockID, dhashcli_lookup_itercb_t cb, ref<dhash_fetchiter_res> res, 
		       route path, int nerror,  clnt_stat err);
  void lookup_iter_chalok_cb (ptr<s_dhash_fetch_arg> arg,
			      dhashcli_lookup_itercb_t cb,
			      route path,
			      int nerror,
			      chordID next, bool ok, chordstat s);
  void lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb, chordID succID, route path, chordstat err);
  void store_cb (dhashcli_storecb_t cb, ref<dhash_storeres> res,  clnt_stat err);

 public:
  dhashcli (ptr<chord> node) : clntnode (node) {}
  void retrieve (chordID blockID, cbretrieve_t cb);
  void insert (chordID blockID, ref<dhash_block> block, dhash_ctype ctype, cbinsert_t cb);
  void lookup_iter (chordID sourceID, chordID blockID, uint32 off, uint32 len, dhashcli_lookup_itercb_t cb);
  void lookup_iter (chordID blockID, uint32 off, uint32 len, dhashcli_lookup_itercb_t cb);
  void store (chordID destID, chordID blockID, char *data, size_t len, size_t off, size_t totsz, 
	      dhash_ctype ctype, dhashcli_storecb_t cb);
  void lookup (chordID blockID, dhashcli_lookupcb_t cb);
};



class dhashclient {
private:
  ptr<aclnt> gwclnt;

  // inserts under the specified key
  // (buf need not remain involatile after the call returns)
  void insert (bigint key, const char *buf, size_t buflen, 
	       cbinsert_t cb, dhash_ctype t);
  void insertcb (cbinsert_t cb, bigint key, dhash_stat *res, clnt_stat err);
  void retrievecb (cbretrieve_t cb, bigint key, dhash_ctype ctype, 
		   ref<dhash_retrieve_res> res, clnt_stat err);

public:
  // sockname is the unix path (ex. /tmp/chord-sock) used
  // to communicate to lsd. 
  dhashclient(str sockname);

  void append (chordID to, const char *buf, size_t buflen, cbinsert_t cb);

  // inserts under the contents hash. 
  // (buf need not remain involatile after the call returns)
  void insert (const char *buf, size_t buflen, cbinsert_t cb);
  void insert (bigint key, const char *buf, size_t buflen, cbinsert_t cb);

  //insert under hash of public key
  void insert (const char *buf, size_t buflen, rabin_priv key, cbinsert_t cb);
  void insert (const char *buf, size_t buflen, bigint sig, rabin_pub key, cbinsert_t cb);
  void insert (bigint hash, const char *buf, size_t buflen, bigint sig, rabin_pub key, cbinsert_t cb);


  // retrieve block and verify
  void retrieve (bigint key, dhash_ctype type, cbretrieve_t cb);

  // synchronouslly call setactive.
  // Returns true on error, and false on success.
  bool sync_setactive (int32 n);
};





class dhashgateway {
  ptr<asrv> clntsrv;
  ptr<chord> clntnode;
  ptr<dhashcli> dhcli;

  void dispatch (svccb *sbp);
  void insert_cb (svccb *sbp, bool err, chordID blockID);
  void retrieve_cb (svccb *sbp, ptr<dhash_block> block);

public:
  dhashgateway (ptr<axprt_stream> x, ptr<chord> clnt);
};


bigint compute_hash (const void *buf, size_t buflen);


static inline str dhasherr2str (dhash_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}

#endif
