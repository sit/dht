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
#include "stabilize.h"

typedef int cb_ID;
typedef vec<chordID> route;

class chord;
class vnode;

typedef callback<void,ptr<vnode>,chordstat>::ref cbjoin_t;
typedef callback<void,chordID,net_address,chordstat>::ref cbchordID_t;
typedef callback<void,vec<chord_node>,chordstat>::ref cbchordIDlist_t;
typedef callback<void,chordID,route,chordstat>::ref cbroute_t;
typedef callback<void, svccb *>::ref cbdispatch_t;

typedef callback<void, bool>::ref cbupcalldone_t;
typedef callback<void, int, void *, cbupcalldone_t>::ref cbupcall_t; 

#include "toe_table.h"
#include "finger_table.h"
#include "succ_list.h"
#include "debruijn.h"
#include "fingerlike.h"
#include "route.h"

extern int logbase;
extern long outbytes;

// ================ VIRTUAL NODE ================

struct dispatch_record {
  unsigned long progno;
  cbdispatch_t cb;
  ihash_entry<dispatch_record> hlink;
  dispatch_record (int p, cbdispatch_t c) : progno (p), cb (c) {};
};

struct upcall_record {
  int progno;
  cbupcall_t cb;
  ihash_entry<upcall_record> hlink;
  upcall_record (int p, cbupcall_t c) : progno (p), cb (c) {};
};

class vnode : public virtual refcount, public stabilizable {
  
  ptr<fingerlike> fingers;
  
  ptr<succ_list> successors;
  ptr<toe_table> toes;
  ptr<stabilize_manager> stabilizer;
  
  int myindex;
  chordID last_pred;		// for stabilize
  
  ihash<unsigned long, 
    dispatch_record, 
    &dispatch_record::progno, 
    &dispatch_record::hlink > dispatch_table;
  ihash<int, 
    upcall_record, 
    &upcall_record::progno, 
    &upcall_record::hlink> upcall_table;

  u_long ngetsuccessor;
  u_long ngetpredecessor;
  u_long ngetsucclist;
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
  u_long ndogetsucclist;
  u_long ndotestrange;
  u_long ndogetfingers;
  u_long ndogetfingers_ext;
  u_long ndogetsucc_ext;
  u_long ndogetpred_ext;
  u_long ndochallenge;
  u_long ndogettoes;
  u_long ndodebruijn;

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
  void find_successor_cb (chordID x, 
			  cbroute_t cb, chordID s, route sp, chordstat status);
  void get_succlist_cb (cbchordIDlist_t cb, chord_nodelistres *res,
			clnt_stat err);

  void find_route_hop_cb (cbroute_t cb, route_iterator *ri, bool done);
  void find_route (chordID &x, cbroute_t cb);
  void dofindroute_cb (svccb *sbp, chordID s, route r, chordstat err);
  
  void notify_cb (chordID n, chordstat *res, clnt_stat err);
  void alert_cb (chordstat *res, clnt_stat err);
  void get_fingers (chordID &x);
  void get_fingers_cb (chordID x, chord_nodelistres *res, clnt_stat err);
  void get_fingers_chal_cb (chordID o, chordID x, bool ok, chordstat s);

  void doalert_cb (svccb *sbp, chordID x, chordID s, net_address r, 
		   chordstat stat);

  void chord_upcall_done (chord_testandfindarg *fa,
			  chord_testandfindres *res,
			  svccb *sbp,
			  bool stop);
  void debruijn_upcall_done (chord_debruijnarg *da,
			     chord_debruijnres *res,
			     svccb *sbp,
			     bool stop);
  void do_upcall (int upcall_prog, int upcall_proc,
		  void *uc_args, int uc_args_len,
		  cbupcalldone_t app_cb);
  void do_upcall_cb (char*, cbupcalldone_t, bool v);

 public:
  chordID myID;
  ptr<chord> chordnode;
  ptr<locationtable> locations;
  ptr<route_factory> factory;
  int server_selection_mode;
  int lookup_mode;

  vnode (ptr<locationtable> _locations, ptr<fingerlike> stab, 
	 ptr<route_factory> f, ptr<chord> _chordnode, 
	 chordID _myID, int _vnode, int server_sel_mode,
	 int lookup_mode);
  ~vnode (void);
  chordID my_ID () { return myID; };
  chordID my_pred ();
  chordID my_succ ();

  // The API
  void stabilize (void);
  void join (cbjoin_t cb);
  void get_successor (chordID n, cbchordID_t cb);
  void get_predecessor (chordID n, cbchordID_t cb);
  void get_succlist (const chordID &n, cbchordIDlist_t cb);
  void find_successor (chordID &x, cbroute_t cb);
  void notify (chordID &n, chordID &x);
  void alert (chordID &n, chordID &x);

  //upcall
  void register_upcall (int progno, cbupcall_t cb);

  // For other modules
  long doRPC (const chordID &ID, rpc_program prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb);
  void resendRPC (long seqno);
  void stats (void);
  void print (void);
  void stop (void);
  vec<chordID> succs () { return successors->succs (); };

  chordID lookup_closestpred (const chordID &x, vec<chordID> f);
  chordID lookup_closestpred (const chordID &x);
  chordID lookup_closestsucc (const chordID &x);

  // For stabilization of the predecessor
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_continuous () { stabilize_pred (); }
  bool isstable () { return last_pred == my_pred(); };
  
  // The RPCs
  void doget_successor (svccb *sbp);
  void doget_predecessor (svccb *sbp);
  void dofindclosestpred (svccb *sbp, chord_findarg *fa);
  void dotestrange_findclosestpred (svccb *sbp, chord_testandfindarg *fa);
  void donotify (svccb *sbp, chord_nodearg *na);
  void doalert (svccb *sbp, chord_nodearg *na);
  void dogetsucclist (svccb *sbp);
  void dogetfingers (svccb *sbp);
  void dogetfingers_ext (svccb *sbp);
  void dogetsucc_ext (svccb *sbp);
  void dogetpred_ext (svccb *sbp);
  void dochallenge (svccb *sbp, chord_challengearg *ca);
  void dogettoes (svccb *sbp);
  void dodebruijn (svccb *sbp, chord_debruijnarg *da);
  void dofindroute (svccb *sbp, chord_findarg *fa);

  //bogus
  void fill_nodelistresext (chord_nodelistextres *res);
  void fill_nodelistres (chord_nodelistres *res);

  //RPC demux
  void addHandler (const rpc_program &prog, cbdispatch_t cb);
  bool progHandled (int progno);
  cbdispatch_t getHandler (unsigned long prog); 
};


class chord : public virtual refcount {
  int nvnode;
  net_address wellknownhost;
  int myport;
  str myname;
  chordID wellknownID;
  int ss_mode;
  int lookup_mode;
  ptr<axprt> x_dgram;
  vec<rpc_program> handledProgs;

  qhash<chordID, ref<vnode>, hashID> vnodes;

  void dispatch (ptr<asrv> s, svccb *sbp);
  void tcpclient_cb (int srvfd);
  int startchord (int myp);
  int startchord (int myp, int type);
  void stats_cb (const chordID &k, ptr<vnode> v);
  void print_cb (const chordID &k, ptr<vnode> v);
  void stop_cb (const chordID &k, ptr<vnode> v);
  void checkwellknown_cb (chordID s, bool ok, chordstat status);
  
  // Number of received RPCs, for locationtable comm stuff
  ptr<u_int32_t> nrcv;

 public:
  // system wide default on the maximum number of vnodes/node.
  static const int max_vnodes;
  ptr<vnode> active;
  ptr<locationtable> locations; 
    
  chord (str _wellknownhost, int _wellknownport,
	 str _myname, int port, int max_cache, int server_selection_mode,
	 int lookup_mode, int _logbase);
  ptr<vnode> newvnode (cbjoin_t cb, ptr<fingerlike> fingers,
		       ptr<route_factory> f);
  void stats (void);
  void print (void);
  void stop (void);

  int get_port () { return myport; }

  //RPC demux
  void handleProgram (const rpc_program &prog);
  bool isHandled (int progno);
  void get_program (int progno, rpc_program **prog);

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

  chordID lookup_closestpred (chordID k, vec<chordID> f) { 
    return active->lookup_closestpred (k, f); 
  };

  chordID lookup_closestpred (chordID k) { 
    return active->lookup_closestpred (k); 
  };

  chordID lookup_closestsucc (chordID k) { 
    return active->lookup_closestsucc (k); 
  };
  void find_successor (chordID n, cbroute_t cb) {
    active->find_successor (n, cb);
  };
  void get_predecessor (chordID n, cbchordID_t cb) {
    active->get_predecessor (n, cb);
  };
  void cacheloc (chordID &x, net_address &r, cbchallengeID_t cb) {
    active->locations->cacheloc (x, r, cb);
  }
  long doRPC (chordID &n, rpc_program progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb) {
    return active->locations->doRPC (n, progno, procno, in, out, cb);
  };
  void alert (chordID &n, chordID &x) {
    active->alert (n, x);
  };
  chordID clnt_ID () {
    return active->my_ID ();
  };    
};

extern const int CHORD_LOOKUP_FINGERLIKE;
extern const int CHORD_LOOKUP_LOCTABLE;
extern const int CHORD_LOOKUP_PROXIMITY;
extern const int CHORD_LOOKUP_FINGERSANDSUCCS;

#endif /* _CHORD_H_ */
