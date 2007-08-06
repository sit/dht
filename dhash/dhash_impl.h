#ifndef __DHASH_IMPL_H__
#define __DHASH_IMPL_H__

#include "dhash.h"
#include <qhash.h>
#include "adb_prot.h"

// Forward declarations.
class RPC_delay_args;
class chord_trigger_t;

class dhashcli;
class dhblock_srv;

class location;
struct hashID;

// Callback typedefs
typedef callback<void,bool,dhash_stat>::ptr cbstore;

// Helper structs/classes
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

  ihash_entry <store_state> link;
   
  store_state (chordID k, unsigned int z) : key(k), 
    size(z), have(0), buf(New char[z]) { };

  ~store_state ()
  { 
    delete[] buf; 
    store_chunk *cnext;
    for (store_chunk *c=have; c; c=cnext) {
      cnext = c->next;
      delete c;
    }
  };
  bool gap ();
  bool addchunk (unsigned int start, unsigned int end, void *base);
  bool iscomplete ();
};

struct fcb_state {
  long nonce;
  cbfetch cb;
  ihash_entry <fcb_state> link;

  fcb_state (cbfetch cb) :  cb (cb) { while ((nonce = random ()) == 0);};
};

class dhash_impl : public dhash, public virtual refcount {
  
  qhash<dhash_ctype, ref<dhblock_srv> > blocksrv;
  
  ihash<long, fcb_state, &fcb_state::nonce, &fcb_state::link> fetch_cbs;

  ptr<vnode> host_node;
  ptr<dhashcli> cli;

  ihash<chordID, store_state, &store_state::key, 
    &store_state::link, hashID> pst;
  
  void route_upcall (int procno, void *args, cbupcalldone_t cb);

  void doRPC (ptr<location> n, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb, 
	      cbtmo_t cb_tmo = NULL);
  void doRPC (const chord_node &n, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb,
	      cbtmo_t cb_tmo = NULL);
  void doRPC (const chord_node_wire &n, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb,
	      cbtmo_t cb_tmo = NULL);
  void dispatch (user_args *a);

  void storesvc_cb (user_args *sbp, s_dhash_insertarg *arg, 
		    bool already_present, dhash_stat err);
  void store (s_dhash_insertarg *arg, cbstore cb);

  void dofetchrec (user_args *sbp, dhash_fetchrec_arg *arg);
  void dofetchrec_nexthop (user_args *sbp, dhash_fetchrec_arg *arg,
			   ptr<location> p);
  void dofetchrec_nexthop_cb (user_args *sbp, dhash_fetchrec_arg *arg,
			      ptr<dhash_fetchrec_res> res,
			      timespec t,
			      clnt_stat err);
  void dofetchrec_local (user_args *sbp, dhash_fetchrec_arg *arg);
  void dofetchrec_assembler (user_args *sbp, dhash_fetchrec_arg *arg,
			     vec<ptr<location> > succs);
  void dofetchrec_assembler_cb (user_args *sbp, dhash_fetchrec_arg *arg,
				dhash_stat s, ptr<dhash_block> b, route r);

  void fetchcomplete_done (int nonce, chord_node sender,
			   dhash_stat status, bool present, u_int32_t sz);

  void srv_ready (ptr<chord_trigger_t> t);
  void start_maint ();

  /* statistics */
  u_int64_t bytes_stored;
  u_int64_t bytes_served;
  u_int64_t objects_stored;
  u_int64_t objects_served;

 public:
  dhash_impl (ptr<vnode> v, str dbsock, str msock, ptr<chord_trigger_t> t);
  ~dhash_impl ();

  vec<dstat> stats ();
  void print_stats ();
  
  void stop ();
  void start (bool randomize = false);

  long register_fetch_callback (cbfetch cb);
  void unregister_fetch_callback (long nonce);

};

#endif
