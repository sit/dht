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
typedef callback<void,chordID,bool,chordstat>::ref cbchallengeID_t;
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

struct finger {
  chordID start;
  node first; // first ID after start
};

class toe_table {
  static const int max_delay = 800; // ms

  vec<chordID> toes;
  ptr<locationtable> locations;
  int in_progress;

  void add_toe_ping_cb (chordID id, int level);
  void get_toes_rmt_cb (chord_gettoes_res *res, int level, clnt_stat err);

 public:
  toe_table (ptr<locationtable> locs) : locations (locs) {};

  vec<chordID> get_toes (int level);
  void add_toe (chordID id, net_address r, int level);
  int filled_level ();
  int level_to_delay ();
  void get_toes_rmt (int level);
  bool stabilizing () { return (in_progress > 0); };
  int level_to_delay (int level);
  void dump ();
};

class vnode : public virtual refcount {
  static const int stabilize_timer = 1000;  // millseconds
  static const int stabilize_timer_max = 500;      // seconds
  static const int max_retry = 5;
  
  ptr<locationtable> locations;
  finger finger_table[NBIT+1];
  node succlist[NSUCC+1];
  ptr<toe_table> toes;
  int nsucc;
  node predecessor;
  int myindex;
  bool stable;
  bool stable_fingers;
  bool stable_fingers2;
  bool stable_succlist;
  bool stable_succlist2;

  qhash<unsigned long, cbdispatch_t> dispatch_table;

  u_long nnodes;	  // estimate of the number of chord nodes
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
  u_long nchallenge;

  u_long ndogetsuccessor;
  u_long ndogetpredecessor;
  u_long ndofindclosestpred;
  u_long ndonotify;
  u_long ndoalert;
  u_long ndotestrange;
  u_long ndogetfingers;
  u_long ndogetfingers_ext;
  u_long ndochallenge;
  u_long ndogettoes;

  timecb_t *stabilize_continuous_tmo;
  timecb_t *stabilize_backoff_tmo;
  u_int32_t continuous_timer;
  u_int32_t backoff_timer;

  void checkfingers (void);
  void updatefingers (chordID &x, net_address &r);
  void replacefinger (chordID &s, node *n);
  u_long estimate_nnodes ();
  chordID closestpredfinger (chordID &x);
  chordID closestpredfinger_ss (chordID &x);

  u_int nout_backoff;
  u_int nout_continuous;
  void stabilize_backoff (int f, int s, u_int32_t t);
  void stabilize_continuous (u_int32_t t);
  int stabilize_succlist (int s);
  int stabilize_finger (int f);
  void stabilize_toes ();
  void stabilize_succ (void);
  void stabilize_pred (void);
  void stabilize_getpred_cb (chordID s, net_address r, chordstat status);
  void stabilize_getpred_cb_ok (chordID p, bool ok, chordstat status);
  void stabilize_findsucc_ok (int i, chordID s, bool ok, chordstat status);
  void stabilize_findsucc_cb (int i, chordID s, route path, chordstat status);
  void stabilize_getsucc_cb (chordID s, net_address r, chordstat status);
  void stabilize_getsucclist_cb (int i, chordID s, net_address r, 
				 chordstat status);
  void stabilize_getsucclist_ok (int i, chordID s, bool ok, chordstat status);
  void join_getsucc_ok (cbjoin_t cb, chordID s, bool ok, chordstat status);
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
  void donotify_cb (chordID p, bool ok, chordstat status);
  void notify_cb (chordID n, chordstat *res, clnt_stat err);
  void alert_cb (chordstat *res, clnt_stat err);
  void get_fingers (chordID &x);
  void get_fingers_cb (chordID x, chord_getfingersres *res, clnt_stat err);
  void challenge (chordID &x, cbchallengeID_t cb);
  void challenge_cb (int challenge, chordID x, cbchallengeID_t cb, 
		     chord_challengeres *res, clnt_stat err);
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
  bool hasbecomeunstable (void);
  bool isstable (void);
  chordID lookup_closestpred (chordID &x);
  chordID lookup_closestsucc (chordID &x);

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
  void dochallenge (svccb *sbp, chord_challengearg *ca);
  void dogettoes (svccb *sbp);

  //RPC demux
  void addHandler (rpc_program prog, cbdispatch_t cb);

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
 
 public:
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
  void handleProgram (rpc_program prog);
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
extern bool nochallenges;

#endif /* _CHORD_H_ */



