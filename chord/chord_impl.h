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

#include "route.h"
#include "transport_prot.h"
#include "coord.h"

extern long outbytes;

class stabilize_manager;
class succ_list;
class pred_list;
class rpc_manager;

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
 protected:
  ptr<location> me_;
  ptr<rpc_manager> rpcm;
  ptr<succ_list> successors;
  ptr<pred_list> predecessors;
  ptr<stabilize_manager> stabilizer;

  virtual void dispatch (user_args *a);
  
  void doroute (user_args *sbp, chord_testandfindarg *fa);
  void do_upcall (int upcall_prog, int upcall_proc,
		  void *uc_args, int uc_args_len,
		  cbupcalldone_t app_cb);
  void upcall_done (chord_testandfindarg *fa,
		    chord_testandfindres *res,
		    user_args *sbp,
		    bool stop);
 private:
  int myindex;
  int checkdead_int;

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
  u_long nnotify;
  u_long nalert;

  u_long ndogetsuccessor;
  u_long ndogetpredecessor;
  u_long ndonotify;
  u_long ndoalert;
  u_long ndogetsucclist;
  u_long ndogetpredlist;
  u_long ndogetsucc_ext;
  u_long ndogetpred_ext;

  vec<ptr<location> > dead_nodes;

  void join_getsucc_cb (ptr<location> n, cbjoin_t cb, chord_nodelistres *r, 
			clnt_stat err);
  void get_successor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			 clnt_stat err);
  void get_predecessor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			   clnt_stat err);
  void find_successor_cb (chordID x, cbroute_t cb,
			  vec<chord_node> s, route sp, chordstat status);
  void get_succlist_cb (cbchordIDlist_t cb, chord_nodelistres *res,
			clnt_stat err);
  void find_succlist_hop_cb (cbroute_t cb, route_iterator *ri, u_long m, bool done);

  void find_route_hop_cb (cbroute_t cb, route_iterator *ri, bool done);
  void find_route (const chordID &x, cbroute_t cb);
  void dofindroute_cb (user_args *sbp, chord_findarg *fa, 
		       vec<chord_node> s, route r, chordstat err);
  
  void notify_cb (chordID n, chordstat *res, clnt_stat err);
  void alert_cb (chordstat *res, clnt_stat err);
  void ping_cb (ptr<location> n, cbping_t cb, clnt_stat status);
  
  void doalert_cb (chord_noderes *res, chordID x, clnt_stat err);

  void do_upcall_cb (char *a, int upcall_prog, int upcall_proc,
		     cbupcalldone_t, bool v);

  void doRPC_cb (ptr<location> l, xdrproc_t proc,
		 void *out, aclnt_cb cb,
		 ref<dorpc_res> res, clnt_stat err);


  void update_error (float actual, float expect, float e);
  void update_coords (Coord uc, float ud);
  void check_dead_node_cb (ptr<location> l, time_t nbackoff, chordstat s);
  void check_dead_node (ptr<location> l, time_t backoff);

  bool tmo (cbtmo_t cb_tmo, int progno,
	    int procno, int args_len, chord_node n, int r);
  
  // The RPCs
  void doget_successor (user_args *sbp);
  void doget_predecessor (user_args *sbp);
  void donotify (user_args *sbp, chord_nodearg *na);
  void doalert (user_args *sbp, chord_nodearg *na);
  void dogetsucclist (user_args *sbp);
  void dogetpredlist (user_args *sbp);
  void dogetsucc_ext (user_args *sbp);
  void dogetpred_ext (user_args *sbp);
  void dofindroute (user_args *sbp, chord_findarg *fa);

 public:
  chordID myID;
  ptr<chord> chordnode;

  vnode_impl (ref<chord> _chordnode, 
	      ref<rpc_manager> _rpcm,
	      ref<location> _l);
  virtual ~vnode_impl (void);
  
  ref<location> my_location ();
  chordID my_ID () const;
  ptr<location> my_pred () const;
  ptr<location> my_succ () const;

  virtual ptr<route_iterator> produce_iterator (chordID xi);
  virtual ptr<route_iterator> produce_iterator (chordID xi,
						const rpc_program &uc_prog,
						int uc_procno,
						ptr<void> uc_args);
  virtual route_iterator *produce_iterator_ptr (chordID xi);
  virtual route_iterator *produce_iterator_ptr (chordID xi,
						const rpc_program &uc_prog,
						int uc_procno,
						ptr<void> uc_args);

  // The API
  virtual void stabilize (void);
  virtual void join (ptr<location> n, cbjoin_t cb);
  virtual void get_successor (ptr<location> n, cbchordID_t cb);
  virtual void get_predecessor (ptr<location> n, cbchordID_t cb);
  virtual void get_succlist (ptr<location> n, cbchordIDlist_t cb);
  virtual void get_predlist (ptr<location> n, cbchordIDlist_t cb);
  virtual void notify (ptr<location> n, chordID &x);
  virtual void alert (ptr<location> n, ptr<location> x);
  virtual void ping (ptr<location> n, cbping_t cb);
  virtual void find_successor (const chordID &x, cbroute_t cb);
  virtual void find_succlist (const chordID &x, u_long m, cbroute_t cb,
			      ptr<chordID> guess = NULL);

  //upcall
  void register_upcall (int progno, cbupcall_t cb);

  // For other modules
  long doRPC (ref<location> l, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb,
	      cbtmo_t cb_tmo = NULL, bool stream = false);
  long doRPC (const chord_node &ID, const rpc_program &prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb,
	      cbtmo_t cb_tmo = NULL, bool stream = false);

  void fill_user_args (user_args *a);

  virtual void stats (void) const;
  virtual void print (strbuf &outbuf) const;
  virtual void stop (void);
  
  vec<ptr<location> > succs ();
  vec<ptr<location> > preds ();

  virtual ptr<location> closestpred (const chordID &x, const vec<chordID> &f);
  
  //RPC demux
  void addHandler (const rpc_program &prog, cbdispatch_t cb);
  bool progHandled (int progno);
  cbdispatch_t getHandler (unsigned long prog); 
};

class route_chord : public route_iterator {
  void make_hop (ptr<location> n);
  void make_hop_cb (ptr<bool> del, chord_testandfindres *res, clnt_stat err);
  void send_hop_cb (bool done);

 public:
  route_chord (ptr<vnode> vi, chordID xi);
  route_chord (ptr<vnode> vi, chordID xi,
	       rpc_program uc_prog,
	       int uc_procno,
	       ptr<void> uc_args);

  ~route_chord () {};
  virtual void first_hop (cbhop_t cb, ptr<chordID> guess);
  void send (ptr<chordID> guess);

  void next_hop ();

  void on_failure (ptr<location> f);
  ptr<location> pop_back ();
};

#endif /* _CHORD_IMPL_H_ */
