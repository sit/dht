#ifndef _CHORD_H_
#define _CHORD_H_

#include "sfsmisc.h"
#include <chord_prot.h>
#include "arpc.h"
#include "crypt.h"
#include "sys/time.h"
#include "vec.h"

#define NBIT 160

struct hashID {
  hashID () {}
  hash_t operator() (const sfs_ID &ID) const {
    return ID.getui ();
  }
};


template<class KEY, class VALUE, u_int max_cache_entries>
class cache {
  struct cache_entry {
    cache *const c;
    const KEY    k;
    VALUE        v;

    ihash_entry<cache_entry> fhlink;
    tailq_entry<cache_entry> lrulink;

    cache_entry (cache<KEY, VALUE, max_cache_entries> *cc,
	       const KEY &kk, const VALUE *vv)
      : c (cc), k (kk)
    {      v = *vv;
      c->lrulist.insert_tail (this);
      c->entries.insert (this);
      c->num_cache_entries++;
      while (c->num_cache_entries > implicit_cast<u_int> (max_cache_entries))
	delete c->lrulist.first;
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

private:
  friend class cache_entry;                      //XXX hashid is a hack that ruins the generic nature of the cache
  ihash<const KEY, cache_entry, &cache_entry::k, &cache_entry::fhlink, hashID> entries;
  u_int num_cache_entries;
  tailq<cache_entry, &cache_entry::lrulink> lrulist;

public:
  cache () { num_cache_entries = 0; }
  ~cache () { entries.deleteall (); }
  void flush () { entries.deleteall (); }
  void enter (const KEY& kk, const VALUE *vv)
  {
    cache_entry *ad = entries[kk];
    if (!ad)
      vNew cache_entry (this, kk, vv);
    else 
      ad->touch ();
  }

  const VALUE *lookup (const KEY& kk)
  {
    cache_entry *ad = entries[kk];
    if (ad) {
      ad->touch ();
      return &ad->v;
    }
    return NULL;
  }
};


struct location;

struct doRPC_cbstate {
  rpc_program progno;
  int procno;
  const void *in;
  void *out;
  aclnt_cb cb;
  tailq_entry<doRPC_cbstate> connectlink;
  
  doRPC_cbstate (rpc_program ro, int pi, const void *ini, void *outi,
		 aclnt_cb cbi) : progno (ro), procno (pi), in (ini),  
    out (outi), cb (cbi) {};
  
};

typedef vec<sfs_ID> route;
typedef callback<void,sfs_ID,net_address,sfsp2pstat>::ref cbsfsID_t;
typedef callback<void,sfs_ID,route,sfsp2pstat>::ref cbroute_t;
typedef callback<void,sfs_ID,char>::ref cbaction_t;
typedef callback<int,sfs_ID>::ref cbsearch_t;

#define ACT_NODE_JOIN 1
#define ACT_NODE_UPDATE 2
#define ACT_NODE_LEAVE 3

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
  int nout;
  timecb_t *timeout_cb;
  
  location (sfs_ID &_n, net_address &_r, sfs_ID _source) : 
    n (_n), addr (_r), source (_source) {
    connecting = false; 
    alive = true;
    x = NULL;
    total_latency = 0;
    num_latencies = 0;
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


// For successor wedge: first is lowest node in the wedge
// For predecessor wedge: first is the highest node in the wedge
struct wedge {
  sfs_ID start;
  sfs_ID end;
  sfs_ID first;
  bool alive;
};

class p2p : public virtual refcount  {
  static const int stabilize_timer = 30;      // seconds
  static const int max_retry = 5;

  net_address wellknownhost;
  sfs_ID wellknownID;
  net_address myaddress;
  sfs_ID myID;
  ptr<aclnt> wellknownclnt;

  wedge finger_table[NBIT+1];
  wedge predecessor;

  ihash<sfs_ID,location,&location::n,&location::fhlink,hashID> locations;
 
  int nbootstrap;
  bool bootstrap_failure;
  bool stable;
  timecb_t *stabilize_tmo;
  
  int lookup_ops;
  int lookups_outstanding;
  int lookup_RPCs;

  vec<cbaction_t> actionCallbacks;
  vec<cbsearch_t> searchCallbacks;

 public:
  p2p (str host, int hostport, const sfs_ID &hostID, int myport, 
       const sfs_ID &ID);

  ~p2p(); // added to help do RPCs/lookup & simulate
  
  // added to help simulate
  int* edges; // holds array of edges
  int numnodes; //XXX - why is this a public field?

  void initialize_graph();
  void doRealRPC (sfs_ID ID, rpc_program progno, int procno, 
		  const void *in, void *out,
		  aclnt_cb cb);  
  // end added to help simulate

  void deleteloc (sfs_ID &n);
  void updateloc (sfs_ID &x, net_address &r, sfs_ID &source);
  bool lookup_anyloc (sfs_ID &n, sfs_ID *r);
  bool lookup_closeloc (sfs_ID &n, sfs_ID *r);
  void set_closeloc (wedge &w);
  bool updatesucc (wedge &w, sfs_ID &x);
  bool updatepred (wedge &w, sfs_ID &x);
  bool noticesucc (int k, sfs_ID &x, net_address &r);
  bool notice (int k, sfs_ID &x, net_address &r);
  int successor_wedge (sfs_ID &n);
  void print ();

  void timeout(location *l);
  void connect_cb (callback<void, ptr<axprt_stream> >::ref cb, int fd);
  void doRPC (sfs_ID &n, rpc_program progno, int procno, const void *in, void *out,
	      aclnt_cb cb);
  void dorpc_connect_cb(location *l, ptr<axprt_stream> x);
  void chord_connect(sfs_ID ID, callback<void, ptr<axprt_stream> >::ref cb);

  void stabilize (int i);
  void stabilize_getsucc_cb (sfs_ID s, net_address r, sfsp2pstat status);
  void stabilize_getpred_cb (sfs_ID s, net_address r, sfsp2pstat status);
  void stabilize_findsucc_cb (int i, sfs_ID s, route path, sfsp2pstat status);
  void stabilize_findpred_cb (sfs_ID p, route r, sfsp2pstat status);

  void join ();
  void join_findpred_cb (sfs_ID pred, route r, sfsp2pstat status);
  void join_getsucc_cb (sfs_ID p, sfs_ID s, net_address r, sfsp2pstat status);

  void find_predecessor (sfs_ID &n, sfs_ID &x, cbroute_t cb);
  void find_closestpred_cb (sfs_ID n, cbroute_t cb, sfsp2p_findres *res, 
			    route search_path, clnt_stat err);

  void find_successor (sfs_ID &n, sfs_ID &x, cbroute_t cb);
  void find_predecessor_cb (cbroute_t cb, sfs_ID p, route search_path, 
			  sfsp2pstat status);
  void find_successor_cb (cbroute_t cb, route sp, sfs_ID s, net_address r,
			    sfsp2pstat status);

  void get_successor (sfs_ID n, cbsfsID_t cb);
  void get_successor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
			 clnt_stat err);
  void get_predecessor (sfs_ID n, cbsfsID_t cb);
  void get_predecessor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
			 clnt_stat err);

  void notify (sfs_ID &n, sfs_ID &x);
  void notify_cb (sfsp2pstat *res, clnt_stat err);
  void alert (sfs_ID &n, sfs_ID &x);
  void alert_cb (sfsp2pstat *res, clnt_stat err);

  void bootstrap ();
  void bootstrap_done ();
  void bootstrap_succ_cb (int i, sfs_ID n, sfs_ID s, route path, 
			  sfsp2pstat status);
  void bootstrap_pred_cb (sfs_ID n, sfs_ID s, route r, sfsp2pstat status);

  void doget_successor (svccb *sbp);
  void doget_predecessor (svccb *sbp);
  void dofindclosestsucc (svccb *sbp, sfsp2p_findarg *fa);  
  void dofindclosestpred (svccb *sbp, sfsp2p_findarg *fa);
  void donotify (svccb *sbp, sfsp2p_notifyarg *na);
  void doalert (svccb *sbp, sfsp2p_notifyarg *na);
  void dofindsucc (sfs_ID &n, cbroute_t cb);
  void dofindsucc_cb (cbroute_t cb, sfs_ID n, sfs_ID x,
		      route search_path, sfsp2pstat status);

  void timing_cb(aclnt_cb cb, location *l, ptr<struct timeval> start, clnt_stat err);

  void registerSearchCallback(cbsearch_t cb);
  bool testSearchCallbacks(sfs_ID id);
  void registerActionCallback(cbaction_t cb);
  void doActionCallbacks(sfs_ID id, char action);
};

extern ptr<p2p> defp2p;

class client {
  ptr<asrv> p2psrv;
  void dispatch (svccb *sbp);
 public:
  client (ptr<axprt_stream> x);
};

#endif _CHORD_H_
