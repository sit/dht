#ifndef _CHORD_H_
#define _CHORD_H_
/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
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

#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include "sys/time.h"
#include "vec.h"
#include "qhash.h"

#ifdef DMALLOC
#include "dmalloc.h"
#endif

#include "chord_prot.h"
#include "chord_util.h"
#include "location.h"

#define NBIT     160     // size of Chord identifiers in bits
#define NSUCC    2*10     // 2 * log of # vnodes

typedef int cb_ID;

class vnode;

typedef vec<chordID> route;
typedef callback<void,vnode*,chordstat>::ref cbjoin_t;
typedef callback<void,chordID,net_address,chordstat>::ref cbchordID_t;
typedef callback<void,chordID,route,chordstat>::ref cbroute_t;
typedef callback<void, svccb *>::ref cbdispatch_t;

struct findpredecessor_cbstate {
  chordID x;
  route search_path;
  cbroute_t cb;
  findpredecessor_cbstate (chordID xi, route spi, cbroute_t cbi) :
    x (xi), search_path (spi), cb (cbi) {};
};

// ============================= Stabilizer ====================================
typedef callback<bool>::ref cbb;

// Parent class for things that need stabilization.
class stabilizable {
 public:
  virtual ~stabilizable () { }
  // called to initiate next round of continuous stabilization
  virtual void do_continuous () { return; }
  // called to initiate next round of backoff stabilization
  virtual void do_backoff () { return; }
  // indicator of whether or not still in progress. however,
  // do_continuous and do_backoff should not break if called
  // when stabilizing returns true.
  virtual bool continuous_stabilizing () { return false; }
  virtual bool backoff_stabilizing () { return false; }
  // indicator that we believe that we are stable.
  // You must implement this function!
  virtual bool isstable () = 0;
};

// Class to manage stabilization timers, etc.
class stabilize_manager {
  static const u_int32_t stabilize_timer = 1000;  // milliseconds
  static const u_int32_t stabilize_decrease_timer = 100; // milliseconds
  static const float stabilize_slowdown_factor = 1.2;
  static const u_int32_t stabilize_timer_max = 2; // seconds

  chordID myID;
  
  bool stable;

  vec<ref<stabilizable> > clients;

  timecb_t *stabilize_continuous_tmo;
  timecb_t *stabilize_backoff_tmo;
  u_int32_t continuous_timer;
  u_int32_t backoff_timer;

 public:
  stabilize_manager (chordID _myID);
  ~stabilize_manager ();

  bool isstable (void);

  u_int32_t cts_timer (void) { return continuous_timer; }
  u_int32_t bo_timer (void) { return backoff_timer; }
  void start (void);
  void stop (void);

  void register_client (ref<stabilizable> c) { clients.push_back (c); }
  void stabilize_backoff (u_int32_t t);
  void stabilize_continuous (u_int32_t t);

  // operations in progress? ask our clients please.
  bool continuous_stabilizing ();
  bool backoff_stabilizing ();
};

class succ_list;

#define MAX_LEVELS 5
class toe_table : public stabilizable {
  static const int max_delay = 800; // ms

  vec<chordID> toes;
  ptr<locationtable> locations;
  ptr<succ_list> successors;
  short target_size[MAX_LEVELS];
  int in_progress;
  short last_level;

  void add_toe_ping_cb (chordID id, int level);
  void get_toes_rmt_cb (chord_gettoes_res *res, int level, clnt_stat err);

 public:
  toe_table (ptr<locationtable> locs,
	     ptr<succ_list> succ);

  vec<chordID> get_toes (int level);
  void add_toe (chordID id, net_address r, int level);
  int filled_level ();
  int level_to_delay ();
  void get_toes_rmt (int level);
  void stabilize_toes ();
  int level_to_delay (int level);
  void dump ();
  short get_last_level () { return last_level; };
  void set_last_level (int l) { last_level = l; };
  void bump_target (int l) { target_size[l] *= 2; };

  // Stabilizable methods
  bool backoff_stabilizing () { return in_progress > 0; }
  void do_backoff () { stabilize_toes (); }
  bool isstable () { return true; } // XXX
};

struct finger {
  chordID start;
  node first; // first ID after start
};

class finger_table : public stabilizable {
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  finger fingers[NBIT+1];
  chordID myID;

  int f; // next finger to stabilize
  bool stable_fingers;
  bool stable_fingers2;
  
  u_int nout_backoff;
  u_int nout_continuous;
  
  u_long nslowfinger;
  u_long nfastfinger;

  void check ();
  int runlength (int i); // how many forward is fingers[i] repeated?
  
  void stabilize_finger_getpred_cb (chordID dn, int i, chordID p, 
				    net_address r, chordstat status);
  void stabilize_findsucc_cb (chordID dn,
			      int i, chordID s, route path, chordstat status);

  
 public:
  finger_table (ptr<vnode> v, ptr<locationtable> locs, chordID myID);

  void updatefinger (chordID &x); //stick x(r) wherever it fits
  void updatefinger (chordID &x, net_address& r); //stick x(r) wherever it fits
  void replacefinger (int i); //find a better finger for the ith finger (no RPCs)
  void deletefinger (chordID &x);

  chordID closestpredfinger (chordID &x);
  chordID closestsuccfinger (chordID &x);

  bool succ_alive ();
  chordID succ ();

  bool alive (int i) { return fingers[i].first.alive; }
  chordID operator[] (int i);
  chordID start (int i) { return fingers[i].start; }

  int countrefs (chordID &x);
  void print ();
  bool better_ith_finger (int i, chordID s);

  void fill_getfingersres (chord_getfingersres *res);
  void fill_getfingersresext (chord_getfingers_ext_res *res);

  void stabilize_finger ();
  void stabilize_succ ();
  void stabilize_getpred_cb (chordID s, chordID p,
			     net_address r, chordstat status);
  void stabilize_getpred_cb_ok (chordID sd, 
				chordID p, bool ok, chordstat status);

  void stats ();

  // Stabilize methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_backoff () { stabilize_finger (); }
  void do_continuous () { stabilize_succ (); }
  bool isstable () { return stable_fingers && stable_fingers2; }
};

class succ_list : public stabilizable {
  chordID myID;
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  node succlist[NSUCC+1];  
  int nsucc;
  
  u_long nnodes;	  // estimate of the number of chord nodes
  
  int s; // next succ to stabilize
  bool stable_succlist;
  bool stable_succlist2;
  u_int nout_backoff;

 public:  
  succ_list (ptr<vnode> v, ptr<locationtable> locs, chordID myID);

  void stabilize_succlist ();
  void stabilize_getsucclist_cb (chordID jid, int i, chordID s, net_address r, 
				 chordstat status);
  void stabilize_getsucclist_ok (int i, chordID s, bool ok, chordstat status);
  
  chordID first_succ ();
  int countrefs (chordID &x);
  void print ();
  void replace_succ (int j);
  void new_succ (int i, chordID s);
  chordID operator[] (int n);
  bool nth_alive (int n) { return succlist[n].alive; };
  void remove_succ (int j);
  chordID closest_succ (chordID &x);
  chordID closest_pred (chordID &x);
  u_long estimate_nnodes ();
  void fill_getsuccres (chord_getsucc_ext_res *res);
  int num_succ () { return nsucc; };
  void delete_succ (chordID &x);

  // Stabilizable methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  void do_backoff () { stabilize_succlist (); }
  bool isstable () { return stable_succlist && stable_succlist2; } // XXX
};

class vnode : public virtual refcount, public stabilizable {
  ptr<locationtable> locations;
  ptr<finger_table> fingers;
  ptr<succ_list> successors;
  ptr<toe_table> toes;
  ptr<stabilize_manager> stabilizer;

  node predecessor;
  int myindex;

  qhash<unsigned long, cbdispatch_t> dispatch_table;

  u_long ngetsuccessor;
  u_long ngetpredecessor;
  u_long nfindsuccessor;
  u_long nhops;
  u_long nmaxhops;
  u_long nfindpredecessor;
  u_long nfindsuccessorrestart;
  u_long nfindpredecessorrestart;
  u_long ntestrange;
  u_long nnotify;
  u_long nalert;
  u_long ngetfingers;

  u_long ndogetsuccessor;
  u_long ndogetpredecessor;
  u_long ndofindclosestpred;
  u_long ndonotify;
  u_long ndoalert;
  u_long ndotestrange;
  u_long ndogetfingers;
  u_long ndogetfingers_ext;
  u_long ndogetsucc_ext;
  u_long ndochallenge;
  u_long ndogettoes;

  u_long nout_continuous;
  void stabilize_pred (void);
  void stabilize_getsucc_cb (chordID pred,
			     chordID s, net_address r, chordstat status);
  void updatepred_cb (chordID p, bool ok, chordstat status);
  
  void join_getsucc_cb (cbjoin_t cb, chordID s, route r, chordstat status);
  void get_successor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			 clnt_stat err);
  void get_predecessor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			   clnt_stat err);
  void find_route (chordID &x, cbroute_t cb);
  void find_successor_cb (chordID x, 
			  cbroute_t cb, chordID s, route sp, chordstat status);
  void testrange_findclosestpred (chordID node, chordID x, 
				  findpredecessor_cbstate *st);
  void testrange_findclosestpred_cb (chord_testandfindres *res, 
			 findpredecessor_cbstate *st, clnt_stat err);
  void testrange_fcp_done_cb (findpredecessor_cbstate *st,
			      chordID s, bool ok, chordstat status);
  void testrange_fcp_step_cb (findpredecessor_cbstate *st,
			      chordID s, bool ok, chordstat status);
  
  void notify_cb (chordID n, chordstat *res, clnt_stat err);
  void alert_cb (chordstat *res, clnt_stat err);
  void get_fingers (chordID &x);
  void get_fingers_cb (chordID x, chord_getfingersres *res, clnt_stat err);
  void get_fingers_chal_cb (chordID o, chordID x, bool ok, chordstat s);

  void doalert_cb (svccb *sbp, chordID x, chordID s, net_address r, 
		   chordstat stat);
  void doRPC (chordID &ID, rpc_program prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb);
 public:
  chordID myID;
  ptr<chord> chordnode;
  int server_selection_mode;

  vnode (ptr<locationtable> _locations, ptr<chord> _chordnode, chordID _myID,
	 int _vnode, int server_sel_mode);
  ~vnode (void);
  chordID my_ID () { return myID; };
  chordID my_pred () { return predecessor.n; };
  chordID my_succ ();

  // The API
  void stabilize (void);
  void join (cbjoin_t cb);
  void get_successor (chordID n, cbchordID_t cb);
  void get_predecessor (chordID n, cbchordID_t cb);
  void find_successor (chordID &x, cbroute_t cb);
  void notify (chordID &n, chordID &x);
  void alert (chordID &n, chordID &x);
  chordID nth_successorID (int n);

  // For other modules
  int countrefs (chordID &x);
  chordID closestsuccfinger (chordID &x);
  void deletefingers (chordID &x);
  void stats (void);
  void print (void);
  void stop (void);
  chordID lookup_closestpred (chordID &x);
  chordID lookup_closestsucc (chordID &x);

  // For stabilization of the predecessor
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_continuous () { stabilize_pred (); }
  bool isstable ();
  
  // The RPCs
  void doget_successor (svccb *sbp);
  void doget_predecessor (svccb *sbp);
  void dofindclosestsucc (svccb *sbp, chord_findarg *fa);  
  void dofindclosestpred (svccb *sbp, chord_findarg *fa);
  void dotestrange_findclosestpred (svccb *sbp, chord_testandfindarg *fa);
  void donotify (svccb *sbp, chord_nodearg *na);
  void doalert (svccb *sbp, chord_nodearg *na);
  void dogetfingers (svccb *sbp);
  void dogetfingers_ext (svccb *sbp);
  void dogetsucc_ext (svccb *sbp);
  void dochallenge (svccb *sbp, chord_challengearg *ca);
  void dogettoes (svccb *sbp);

  //RPC demux
  void addHandler (const rpc_program &prog, cbdispatch_t cb);

  cbdispatch_t getHandler (unsigned long prog) {
    return dispatch_table [prog];
  };
};


class chord : public virtual refcount {
  int nvnode;
  net_address wellknownhost;
  int myport;
  str myname;
  chordID wellknownID;
  int ss_mode;
  ptr<axprt> x_dgram;
  vec<int> handledProgs;

  qhash<chordID, ref<vnode>, hashID> vnodes;
  ptr<vnode> active;

  void dispatch (ptr<asrv> s, svccb *sbp);
  void tcpclient_cb (int srvfd);
  int startchord (int myp);
  int startchord (int myp, int type);
  void deletefingers_cb (chordID x, const chordID &k, ptr<vnode> v);
  void stats_cb (const chordID &k, ptr<vnode> v);
  void print_cb (const chordID &k, ptr<vnode> v);
  void stop_cb (const chordID &k, ptr<vnode> v);
  void checkwellknown_cb (chordID s, bool ok, chordstat status);
 
 public:
  // system wide default on the maximum number of vnodes/node.
  static const int max_vnodes = 1024;

  // locations contains all nodes that appear as fingers in vnodes plus
  // a number of cached nodes.  the cached nodes have refcnt = 0
  ptr<locationtable> locations; 
    
  chord (str _wellknownhost, int _wellknownport,
	 str _myname, int port, int max_cache, int server_selection_mode);
  ptr<vnode> newvnode (cbjoin_t cb);
  void deletefingers (chordID x);
  int countrefs (chordID &x);
  void stats (void);
  void print (void);
  void stop (void);

  int get_port () { return myport; }

  //RPC demux
  void handleProgram (const rpc_program &prog);
  bool isHandled (int progno);

  //'wrappers' for vnode functions (to be called from higher layers)
  void set_active (int n) { 
    int i=0;
    n %= nvnode;
    qhash_slot<chordID, ref<vnode> > *s = vnodes.first ();
    while ( (s) && (i++ < n)) s = vnodes.next (s);
    if (!s) active = vnodes.first ()->value;
    else  active = s->value;

    warn << "Active node now " << active->my_ID () << "\n";
  };

  chordID lookup_closestpred (chordID k) { 
    return active->lookup_closestpred (k); 
  };
  void find_successor (chordID n, cbroute_t cb) {
    active->find_successor (n, cb);
  };
  void get_predecessor (chordID n, cbchordID_t cb) {
    active->get_predecessor (n, cb);
  };
  void doRPC (chordID &n, rpc_program progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb) {
    locations->doRPC (n, progno, procno, in, out, cb);
  };
  void alert (chordID &n, chordID &x) {
    active->alert (n, x);
  };
  chordID nth_successorID (int n) {
    return active->nth_successorID (n);
  };
  chordID clnt_ID () {
    return active->my_ID ();
  };

  //public stats
  u_int64_t nrcv;
    
};

extern ptr<chord> chordnode;

#endif /* _CHORD_H_ */
