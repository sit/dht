#ifndef _CHORD_H_
#define _CHORD_H_

#include "sfsmisc.h"
#include <chord_prot.h>
#include "arpc.h"
#include "crypt.h"
#include "sys/time.h"
#include "vec.h"
#include "qhash.h"

#define NBIT     160
#define NIMM     NBIT

typedef int cb_ID;

struct hashID {
  hashID () {}
  hash_t operator() (const sfs_ID &ID) const {
    return ID.getui ();
  }
};

struct chord_stats {
  long insert_path_len;
  long insert_ops;
  long lookup_path_len;
  long lookup_ops;
  long lookup_lat;
  long lookup_max_lat;
  long lookup_bytes_fetched;
  long total_rpcs;
  qhash<sfs_ID, int, hashID> *balance;
};

extern chord_stats stats;

template<class KEY, class VALUE>
class vs_cache {
  struct cache_entry {
    vs_cache    *c;
     KEY    k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;

    cache_entry (vs_cache<KEY, VALUE> *cc,
	        KEY &kk,  VALUE *vv)
      : c (cc), k (kk)
    {      v = *vv;
    c->lrulist.insert_tail (this);
    c->entries.insert (this);
    c->num_cache_entries++;
    while (c->num_cache_entries > implicit_cast<u_int> (c->max_cache_entries)) {
      if (c->fcb) (c->fcb) (c->lrulist.first->k, c->lrulist.first->v);
      delete c->lrulist.first;
    }
    }

    ~cache_entry ()
    {
      c->lrulist.remove (this);
      c->entries.remove (this);
      c->num_cache_entries--;
    }

    void touch ()
    {
      c->lrulist.remove (this);
      c->lrulist.insert_tail (this);
    }
  };

  typedef callback<void, KEY, VALUE>::ptr flushcb_t;
  
private:
  friend class cache_entry;   //XXX hashid is a hack that ruins the generic nature of the cache
  ihash<KEY, cache_entry, &cache_entry::k, &cache_entry::fhlink, hashID> entries;
  u_int num_cache_entries;
  tailq<cache_entry, &cache_entry::lrulink> lrulist;
  u_int max_cache_entries;
  flushcb_t fcb;
public:
  vs_cache (u_int max_entries = 250) : num_cache_entries (0), 
    max_cache_entries (max_entries), 
    fcb (NULL) { };

  ~vs_cache () { entries.deleteall (); }
  void flush () { entries.deleteall (); }
  void enter ( KEY& kk,  VALUE *vv)
    {
      cache_entry *ad = entries[kk];
      if (!ad)
	vNew cache_entry (this, kk, vv);
      else 
	ad->touch ();
    }
  
  void remove ( KEY& k) 
    {
      
      entries.remove(entries[k]);
    }

   VALUE *lookup ( KEY& kk)
    {
      cache_entry *ad = entries[kk];
      if (ad) {
	ad->touch ();
	return &ad->v;
      }
      return NULL;
    }
  
   void traverse ( callback<void, KEY>::ref cb ) 
     {
       cache_entry *e = entries.first ();
       while (e) 
	 {
	   cb (e->k);
	   e = entries.next (e);
	 }
     }

   VALUE *peek ( KEY& k) {
    cache_entry *ad = entries[k];
    if (ad) {
      return &ad->v;
    }
    return NULL;
  }
  
  void set_flushcb (flushcb_t cb ) {
    fcb = cb;
  }
};

struct rpc_args {
  sfs_ID ID;
  rpc_program progno;
  int procno;
  const void *in;
  void *out;
  aclnt_cb cb;
  rpc_args(sfs_ID id, rpc_program progn, int proc, const void *i, void *o, 
	   aclnt_cb c) : ID (id), progno (progn), procno (proc),in (i),
    out (o), cb (c){};
};


struct location;

struct doRPC_cbstate {
  rpc_program progno;
  int procno;
  ptr<void> in;
  void *out;
  aclnt_cb cb;
  tailq_entry<doRPC_cbstate> connectlink;
  
  doRPC_cbstate (rpc_program ro, int pi, ptr<void> ini, void *outi,
		 aclnt_cb cbi) : progno (ro), procno (pi), in (ini),  
    out (outi), cb (cbi) {};
  
};

typedef vec<sfs_ID> route;
typedef callback<void,sfs_ID,net_address,sfsp2pstat>::ref cbsfsID_t;
typedef callback<void,sfs_ID,route,sfsp2pstat>::ref cbroute_t;
typedef callback<void,sfs_ID,char>::ref cbaction_t;
typedef callback<void,sfs_ID,sfs_ID,callback<void,int>::ref >::ref cbsearch_t;
typedef callback<void,int>::ref cbtest_t;

#define ACT_NODE_JOIN 1
#define ACT_NODE_UPDATE 2
#define ACT_NODE_LEAVE 3

struct findpredecessor_cbstate {
  sfs_ID x;
  sfs_ID nprime;
  route search_path;
  cbroute_t cb;
  findpredecessor_cbstate (sfs_ID xi, sfs_ID npi, route spi, cbroute_t cbi) :
    x (xi), nprime (npi), search_path (spi), cb (cbi) {};
};

struct searchcb_entry {
  cbsearch_t cb;
  list_entry<searchcb_entry> link;
  searchcb_entry (cbsearch_t scb) : cb (scb) {  };
};

struct location {
  sfs_ID n;
  net_address addr;
  sfs_ID source;
  ptr<axprt_stream> x;
  bool connecting;
  tailq<doRPC_cbstate, &doRPC_cbstate::connectlink> connectlist;
  ihash_entry<location> fhlink;
  bool alive;
  long total_latency;
  long num_latencies;
  long max_latency;
  int nout;
  timecb_t *timeout_cb;
  
  location (sfs_ID &_n, net_address &_r, sfs_ID _source) : 
    n (_n), addr (_r), source (_source) {
    connecting = false; 
    alive = true;
    x = NULL;
    total_latency = 0;
    num_latencies = 0;
    max_latency = 0;
    nout = 0;
    timeout_cb = NULL;
  };
  location (sfs_ID &_n, sfs_hostname _s, int _p, sfs_ID &_source) : n (_n) {
    addr.hostname = _s;
    addr.port = _p;
    source = _source;
    connecting = false;
    alive = true;
    x = NULL;
    nout = 0;
    timeout_cb = NULL;
  }
};


// For successor finger: first is lowest node in the interval
// For predecessor finger: first is the highest node in the interval
struct finger {
  sfs_ID start;
  sfs_ID first;
  bool alive;
};

class p2p : public virtual refcount  {
  static const int stabilize_timer = 1;      // seconds
  static const int max_retry = 5;

  bool lsd_location_lookup;
  
  net_address wellknownhost;
  sfs_ID wellknownID;
  net_address myaddress;
  sfs_ID myID;
  ptr<aclnt> wellknownclnt;

  finger finger_table[NBIT+1];
  sfs_ID immediate[NIMM];
  sfs_ID predecessor;

  ihash<sfs_ID,location,&location::n,&location::fhlink,hashID> locations;
 
  timecb_t *stabilize_tmo;
  
  int lookup_ops;
  int lookups_outstanding;
  int lookup_RPCs;

  vec<cbaction_t> actionCallbacks;
  list<searchcb_entry, &searchcb_entry::link> searchCallbacks;

 public:
  bool insert_or_lookup;
  int rpcdelay;

  p2p (str host, int hostport, const sfs_ID &hostID, int myport, str myname,
       const sfs_ID &ID);

  ~p2p(); // added to help do RPCs/lookup & simulate
  

  sfs_ID my_ID () { return myID; };
  sfs_ID my_pred () { return predecessor; };
  sfs_ID my_succ () { return finger_table[1].first; };


  void updateall (sfs_ID &x);
  sfs_ID findclosestpred (sfs_ID &x);

  void deleteloc (sfs_ID &n);
  void updateloc (sfs_ID &x, net_address &r, sfs_ID &source);
  bool lookup_anyloc (sfs_ID &n, sfs_ID *r);
  bool lookup_closeloc (sfs_ID &n, sfs_ID *r);
  void set_closeloc (finger &w);
  void print ();

  void timeout(location *l);
  void connect_cb (callback<void, ptr<axprt_stream> >::ref cb, int fd);
  void doRPC (sfs_ID &n, rpc_program progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);
  void doRealRPC (rpc_args *a);
  void dorpc_connect_cb(location *l, ptr<axprt_stream> x);
  void chord_connect(sfs_ID ID, callback<void, ptr<axprt_stream> >::ref cb);

  void stabilize (int i);
  void stabilize_getpred_cb (sfs_ID s, net_address r, sfsp2pstat status);
  void stabilize_findsucc_cb (int i, sfs_ID s, route path, sfsp2pstat status);
  void stabilize_findpred_cb (sfs_ID p, route r, sfsp2pstat status);

  void join ();
  void join_getsucc_cb (sfs_ID s, route r, sfsp2pstat status);

  void find_predecessor (sfs_ID &n, sfs_ID &x, cbroute_t cb);
  void find_predecessor_restart (sfs_ID &n, sfs_ID &x, route search_path,
				 cbroute_t cb);
  void find_pred_test_cache_cb (sfs_ID n, sfs_ID x, cbroute_t cb, int found);
  void find_closestpred_cb (sfs_ID n, cbroute_t cb, sfsp2p_findres *res, 
			    route search_path, clnt_stat err);
  void test_and_find_cb (sfsp2p_testandfindres *res, 
			 findpredecessor_cbstate *st, clnt_stat err);
  void find_successor (sfs_ID &n, sfs_ID &x, cbroute_t cb);
  void find_successor_restart (sfs_ID &n, sfs_ID &x, route search_path, 
			       cbroute_t cb);
  void find_predecessor_cb (cbroute_t cb, sfs_ID p, route search_path, 
			  sfsp2pstat status);
  void find_closestpred_succ_cb (findpredecessor_cbstate *st, sfs_ID s,
				 net_address r, sfsp2pstat status);
  void find_closestpred_test_cache_cb (sfs_ID node, findpredecessor_cbstate *st, int found);
  void find_successor_cb (cbroute_t cb, route sp, sfs_ID s, net_address r,
			    sfsp2pstat status);

  void get_successor (sfs_ID n, cbsfsID_t cb);
  void get_successor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
			 clnt_stat err);

  void get_succ (sfs_ID n, callback<void, sfs_ID, sfsp2pstat>::ref cb);
  void get_succ_cb (callback<void, sfs_ID, sfsp2pstat>::ref cb, 
		    sfs_ID succ,
		    net_address r,
		    sfsp2pstat err);

  void get_predecessor (sfs_ID n, cbsfsID_t cb);
  void get_predecessor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
			 clnt_stat err);

  void notify (sfs_ID &n, sfs_ID &x);
  void notify_cb (sfsp2pstat *res, clnt_stat err);
  void alert (sfs_ID &n, sfs_ID &x);
  void alert_cb (sfsp2pstat *res, clnt_stat err);

  void doget_successor (svccb *sbp);
  void doget_predecessor (svccb *sbp);
  void dofindclosestsucc (svccb *sbp, sfsp2p_findarg *fa);  
  void dofindclosestpred (svccb *sbp, sfsp2p_findarg *fa);
  void dotestandfind (svccb *sbp, sfsp2p_testandfindarg *fa);
  void donotify (svccb *sbp, sfsp2p_notifyarg *na);
  void doalert (svccb *sbp, sfsp2p_notifyarg *na);
  void dofindsucc (sfs_ID &n, cbroute_t cb);
  void dofindsucc_cb (cbroute_t cb, sfs_ID n, sfs_ID x,
		      route search_path, sfsp2pstat status);

  void timing_cb(aclnt_cb cb, location *l, ptr<struct timeval> start, int procno, rpc_program progno, clnt_stat err);


  searchcb_entry * registerSearchCallback(cbsearch_t cb);
  void removeSearchCallback(searchcb_entry *scb);
  void testSearchCallbacks(sfs_ID id, sfs_ID target, cbtest_t cb);
  void tscb (sfs_ID id, sfs_ID x, searchcb_entry *scb, cbtest_t cb);
  void tscb_cb (sfs_ID id, sfs_ID x, searchcb_entry *scb, cbtest_t cb, int result);

  void registerActionCallback(cbaction_t cb);
  void doActionCallbacks(sfs_ID id, char action);

  void stats_cb ();

  sfs_ID query_location_table (sfs_ID x);
  
};

extern ptr<p2p> defp2p;

class client {
  ptr<asrv> p2psrv;
  void dispatch (svccb *sbp);
 public:
  client (ptr<axprt_stream> x);
};

#endif _CHORD_H_

