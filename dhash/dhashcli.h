
#include <sys/types.h>

// SFS includes
#include <ihash.h>
#include <refcnt.h>
#include <vec.h>

typedef callback<void, dhash_stat, chordID>::ref cbinsert_t;
typedef callback<void, dhash_stat, vec<chord_node>, route>::ref dhashcli_lookupcb_t;

// Forward declarations
class vnode;
class dhash;
class route_factory;
class dhash_block;
class route_iterator;

class dhashcli {
  ptr<vnode> clntnode;
  bool do_cache;
  int server_selection_mode;
  dhash *dh;
  ptr<route_factory> r_factory;

  struct rcv_state {
    ihash_entry <rcv_state> link;
    blockID key;
    route r;
    vec<timespec> times;
    
    int incoming_rpcs;
    
    vec<chord_node> succs;
    size_t nextsucc;
    
    vec<str> frags;
    vec<cb_ret> callbacks;

    void timemark () {
      timespec x;
      clock_gettime (CLOCK_REALTIME, &x);
      times.push_back (x);
    }
    
    void complete (dhash_stat s, ptr<dhash_block> b) {
      for (u_int i = 0; i < callbacks.size (); i++)
	(callbacks[i]) (s, b, r);
      delete this;
    }
      
    rcv_state (blockID key) : key (key), incoming_rpcs (0), nextsucc (0) {
      timemark ();
    }
  };

  ihash<blockID, rcv_state, &rcv_state::key, &rcv_state::link, bhashID> rcvs;

  // State for a fragment store
  struct sto_state {
    ref<dhash_block> block;
    // route r;
    vec<chord_node> succs;
    cbinsert_t cb;
    
    u_int out;
    u_int good;
    
    sto_state (ref<dhash_block> b, cbinsert_t x) :
      block (b), cb (x), out (0), good (0) {}
  };


private:
  void doRPC (chordID ID, const rpc_program &prog, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);

  void lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb,
			   vec<chord_node> s, route path, chordstat err);
  void retrieve_hop_cb (cb_ret cb, chordID key, dhash_stat status,
			ptr<dhash_block> block, route path);
  void cache_block (ptr<dhash_block> block, route search_path, chordID key);
  void finish_cache (dhash_stat status, chordID dest);
  void retrieve_with_source_cb (cb_ret cb, dhash_stat status, 
				ptr<dhash_block> block, route path);
  void insert_lookup_cb (chordID blockID, ref<dhash_block> block,
			 cbinsert_t cb, int trial,
			 dhash_stat status, vec<chord_node> succs, route r);
  void insert_stored_cb (chordID blockID, ref<dhash_block> block,
			 cbinsert_t cb, int trial,
			 dhash_stat stat, chordID retID);
    
  void insert2_lookup_cb (ref<dhash_block> block, cbinsert_t cb, 
			  dhash_stat status, vec<chord_node> succs, route r);
  void insert2_store_cb (ref<sto_state> ss, u_int i, ref<dhash_storeres> res,
			 clnt_stat err);

  void fetch_frag (rcv_state *rs);

  void retrieve2_hop_cb (blockID blockID, route_iterator *ci, bool done);
  void retrieve2_lookup_cb (blockID blockID,
			    dhash_stat status, vec<chord_node> succs, route r);
  void retrieve2_fetch_cb (blockID blockID, u_int i,
			   ref<dhash_fetchiter_res> res,
			   clnt_stat err);


 public:
  dhashcli (ptr<vnode> node, dhash *dh, ptr<route_factory> r_factory, 
	    bool do_cache, int ss_mode = 1);
  void retrieve (chordID blockID, int options, cb_ret cb);

  void retrieve2 (blockID blockID, int options, cb_ret cb);
  void retrieve (chordID source, chordID blockID, cb_ret cb);
  void insert (chordID blockID, ref<dhash_block> block, 
               int options, cbinsert_t cb);
  void insert2 (ref<dhash_block> block, int options, cbinsert_t cb);
  void storeblock (ptr<location> l, chordID blockID, ref<dhash_block> block,
		   bool last, cbinsert_t cb, store_status stat = DHASH_STORE);

  void lookup (chordID blockID, int options, dhashcli_lookupcb_t cb);
};

