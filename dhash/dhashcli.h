#ifndef __DHASH_CLI_H_
#define __DHASH_CLI_H_

#include <sys/types.h>

// SFS includes
#include <refcnt.h>
#include <vec.h>

typedef callback<void, dhash_stat, chordID>::ref cbinsert_t;
typedef callback<void, dhash_stat, vec<chordID> >::ref cbinsert_path_t;
typedef callback<void, dhash_stat, vec<chord_node>, route>::ref
  dhashcli_lookupcb_t;
typedef	callback<void, dhash_stat, bool>::ref sendblockcb_t;

#include "download.h" // for cbretrieve_t
#include <dhc.h>

// Forward declarations
class dbfe;
class vnode;
class dhash;
class dhash_block;
class route_iterator;

class dhashcli {
  ptr<vnode> clntnode;
  bool ordersucc_;
  ptr<dhc> dhc_mgr;

  struct rcv_state {
    blockID key;
    route r;
    vec<timespec> times;
    int errors;
    
    bool succopt;
    int incoming_rpcs;
    
    vec<chord_node> succs;
    size_t nextsucc;
    
    vec<str> frags;
    vec<cb_ret> callbacks;

    bool completed;

    void timemark () {
      timespec x;
      clock_gettime (CLOCK_REALTIME, &x);
      times.push_back (x);
    }
    
    void complete (dhash_stat s, ptr<dhash_block> b) {
      completed = true;
      for (u_int i = 0; i < callbacks.size (); i++)
	(callbacks[i]) (s, b, r);
    }

    rcv_state (blockID key) :
      key (key),
      errors (0),
      succopt (false),
      incoming_rpcs (0),
      nextsucc (0),
      completed (false)
    {
      timemark ();
    }
  };

  // State for a fragment store
  struct sto_state {
    ref<dhash_block> block;
    vec<chord_node> succs;
    cbinsert_path_t cb;
    
    u_int out;
    u_int good;
    
    sto_state (ref<dhash_block> b, cbinsert_path_t x) :
      block (b), cb (x), out (0), good (0) {}
  };

  void lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb,
			   vec<chord_node> s, route path, chordstat err);
    
  void insert_lookup_cb (ref<dhash_block> block, cbinsert_path_t cb, int options, 
			 dhash_stat status, vec<chord_node> succs, route r);
  void insert_store_cb (ref<sto_state> ss, route r, u_int i, 
			u_int nstores, u_int min_needed,
			dhash_stat err, chordID id, bool present);
  void insert_dhc_cb (ptr<location> dest, route r, 
		      cbinsert_path_t cb, dhc_stat err);
  
  void fetch_frag (ptr<rcv_state> rs);

  void retrieve_lookup_cb (ptr<rcv_state> rs, vec<chord_node> succs, route r,
			   chordstat status);
  void retrieve_fetch_cb (ptr<rcv_state> rs, u_int i,
			  ptr<dhash_block> block);

  void insert_succlist_cb (ref<dhash_block> block, cbinsert_path_t cb,
			   chordID guess, int options,
			   vec<chord_node> succs, chordstat status);
  void retrieve_block_hop_cb (ptr<rcv_state> rs, route_iterator *ci,
			     int options, int retries, ptr<chordID> guess,
			     bool done);
  void retrieve_dl_or_walk_cb (ptr<rcv_state> rs, dhash_stat status,
                               int options, int retries, ptr<chordID> guess,
			       ptr<dhash_block> blk);

  void sendblock_cb (callback<void, dhash_stat, bool>::ref cb, 
		     dhash_stat err, chordID dest, bool present);
  void on_timeout (ptr<rcv_state> rs, 
		   chord_node dest,
		   int retry_num);
public:
  dhashcli (ptr<vnode> node, str dhcs = str("default"), uint nreplica = 5);

  void retrieve (blockID blockID, cb_ret cb, 
		 int options = 0, 
		 ptr<chordID> guess = NULL);

  void insert (ref<dhash_block> block, cbinsert_path_t cb, 
	       int options = 0, 
	       ptr<chordID> guess = NULL);

  void sendblock (ptr<location> dst, blockID bid,
		  ptr<dbfe> db, sendblockcb_t cb);

  void lookup (chordID blockID, dhashcli_lookupcb_t cb);
};

#endif
