
#ifndef _CHORD_IMPL_H_
#define _CHORD_IMPL_H_
/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 * Copyright (C) 2003 Emil Sit (sit@mit.edu).
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

#include "chord.h"

#include "location.h"
#include "stabilize.h"

#include "toe_table.h"
#include "finger_table.h"
#include "succ_list.h"
#include "pred_list.h"
#include "debruijn.h"
#include "fingerlike.h"
#include "route.h"
#include "transport_prot.h"

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

class vnode_impl : public vnode {
  ptr<fingerlike> fingers;
  
  ptr<succ_list> successors;
  ptr<pred_list> predecessors;
  ptr<toe_table> toes;
  ptr<stabilize_manager> stabilizer;
  
  int myindex;
  
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

  void dispatch (svccb *sbp, void *args, int procno);
  void stabilize_pred (void);
  void stabilize_getsucc_cb (chordID pred,
			     chordID s, net_address r, chordstat status);
  
  void join_getsucc_cb (const chord_node n, cbjoin_t cb, chord_nodelistres *r, clnt_stat err);
  void get_successor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			 clnt_stat err);
  void get_predecessor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			   clnt_stat err);
  void find_successor_cb (chordID x, 
			  cbroute_t cb, chordID s, route sp, chordstat status);
  void get_succlist_cb (cbchordIDlist_t cb, chord_nodelistres *res,
			clnt_stat err);

  void find_route_hop_cb (cbroute_t cb, route_iterator *ri, bool done);
  void find_route (const chordID &x, cbroute_t cb);
  void dofindroute_cb (svccb *sbp, chord_findarg *fa, 
		       chordID s, route r, chordstat err);
  
  void notify_cb (chordID n, chordstat *res, clnt_stat err);
  void alert_cb (chordstat *res, clnt_stat err);
  
  void get_fingers_cb (cbchordIDlist_t cb,
		       chordID x, chord_nodelistres *res, clnt_stat err);

  void doalert_cb (chord_noderes *res, chordID x, clnt_stat err);

  void secchord_upcall_done (chord_nodelistres *res,
			     svccb *sbp,
			     bool stop);
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

  void doRPC_cb (const rpc_program prog, int procno,
		 void *out, aclnt_cb cb, 
		 dorpc_res *res, clnt_stat err);
 public:
  chordID myID;
  ptr<chord> chordnode;
  ptr<route_factory> factory;
  int server_selection_mode;
  int lookup_mode;

  vnode_impl (ptr<locationtable> _locations, ptr<fingerlike> stab, 
	      ptr<route_factory> f, ptr<chord> _chordnode, 
	      chordID _myID, int _vnode, int server_sel_mode,
	      int lookup_mode);
  ~vnode_impl (void);
  chordID my_ID () const;
  chordID my_pred () const;
  chordID my_succ () const;

  // The API
  void stabilize (void);
  void join (const chord_node &n, cbjoin_t cb);
  void get_successor (const chordID &n, cbchordID_t cb);
  void get_predecessor (const chordID &n, cbchordID_t cb);
  void get_succlist (const chordID &n, cbchordIDlist_t cb);
  void get_fingers (const chordID &n, cbchordIDlist_t cb);
  void find_successor (const chordID &x, cbroute_t cb);
  void notify (const chordID &n, chordID &x);
  void alert (const chordID &n, chordID &x);
  
  //upcall
  void register_upcall (int progno, cbupcall_t cb);

  // For other modules
  long doRPC (const chordID &ID, const rpc_program &prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb);
  long doRPC (const chord_node &ID, const rpc_program &prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb);
  void resendRPC (long seqno);
  void stats (void) const;
  void print (void) const;
  void stop (void);
  vec<chordID> succs () { return successors->succs (); };
  void doRPC_reply (svccb *sbp, void *res, const rpc_program &prog, int procno);

  chordID lookup_closestpred (const chordID &x, vec<chordID> f);
  chordID lookup_closestpred (const chordID &x);
  chordID lookup_closestsucc (const chordID &x);
  
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
  void dosecfindsucc (svccb *sbp, chord_testandfindarg *fa);
  void dogettoes (svccb *sbp, chord_gettoes_arg *ta);
  void dodebruijn (svccb *sbp, chord_debruijnarg *da);
  void dofindroute (svccb *sbp, chord_findarg *fa);

  //RPC demux
  void addHandler (const rpc_program &prog, cbdispatch_t cb);
  bool progHandled (int progno);
  cbdispatch_t getHandler (unsigned long prog); 
};

#endif /* _CHORD_IMPL_H_ */
