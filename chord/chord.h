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

typedef int cb_ID;
typedef vec<chordID> route;

class chord;
class vnode;

class fingerlike;
class route_factory;
class chord;
class locationtable;
struct user_args;

typedef callback<void,ptr<vnode>,chordstat>::ref cbjoin_t;
typedef callback<void,chord_node,chordstat>::ref cbchordID_t;
typedef callback<void,vec<chord_node>,chordstat>::ref cbchordIDlist_t;
typedef callback<void,vec<chord_node>,route,chordstat>::ref cbroute_t;
typedef callback<void, user_args *>::ref cbdispatch_t;
typedef callback<void,chordstat>::ptr cbping_t;
typedef callback<void, bool>::ref cbupcalldone_t;
typedef callback<void, int, void *, cbupcalldone_t>::ref cbupcall_t; 

extern int logbase;

struct user_args {
  //info about the RPC
  void *args;
  int procno;
  svccb *sbp;
  const rpc_program *prog;
  u_int64_t send_time;

  //info about the vnode that will reply
  chordID myID;
  int myindex;
  vec<float> coords;

  user_args (svccb *s, void *a, const rpc_program *pr, int p, u_int64_t st) : 
    args (a), procno (p), sbp (s), prog (pr), send_time (st) {};

  void *getvoidarg () { return args; };
  const void *getvoidarg () const { return args; };
  template<class T> T *getarg () { return static_cast<T *> (args); };
  template<class T> const T *getarg () const {return static_cast<T *> (args);};
  
  void reject (auth_stat s) {sbp->reject (s); };
  void reject (accept_stat s) {sbp->reject (s); };
  void reply (void *res);
  void replyref (const int &res)
    { sbp->replyref (res); }
  
  dorpc_arg * transport_header () 
    { return sbp->template getarg<dorpc_arg> (); };

  void fill_from (chord_node *from);
  
  ~user_args ();
};

// ================ VIRTUAL NODE ================
class vnode : public virtual refcount {  
 public:
  ptr<locationtable> locations;

  static ref<vnode> produce_vnode
    (ptr<locationtable> _locations, ptr<fingerlike> stab, 
     ptr<route_factory> f, ptr<chord> _chordnode, 
     chordID _myID, int _vnode, int server_sel_mode,
     int lookup_mode);

  virtual ~vnode (void) = 0;
  virtual chordID my_ID () const = 0;
  virtual chordID my_pred () const = 0;
  virtual chordID my_succ () const = 0;

  // The API
  virtual void stabilize (void) = 0;
  virtual void join (const chord_node &n, cbjoin_t cb) = 0;
  virtual void get_successor (const chordID &n, cbchordID_t cb) = 0;
  virtual void get_predecessor (const chordID &n, cbchordID_t cb) = 0;
  virtual void get_succlist (const chordID &n, cbchordIDlist_t cb) = 0;
  virtual void get_fingers (const chordID &n, cbchordIDlist_t cb) = 0;
  virtual void find_successor (const chordID &x, cbroute_t cb) = 0;
  virtual void notify (const chordID &n, chordID &x) = 0;
  virtual void alert (const chordID &n, chordID &x) = 0;
  virtual void ping (const chordID &x, cbping_t cb) = 0;

  //upcall
  virtual void register_upcall (int progno, cbupcall_t cb) = 0;

  // For other modules
  virtual long doRPC (const chordID &ID, const rpc_program &prog, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb) = 0;
  virtual long doRPC (const chord_node &ID, const rpc_program &prog, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb) = 0;

  virtual void resendRPC (long seqno) = 0;
  virtual void fill_user_args (user_args *a) = 0;

  virtual void stats (void) const = 0;
  virtual void print (void) const = 0;
  virtual void stop (void) = 0;
  virtual vec<chord_node> succs () = 0;
  virtual vec<chord_node> preds () = 0;


  virtual chordID lookup_closestpred (const chordID &x, const vec<chordID> &f) = 0;
  virtual chordID lookup_closestpred (const chordID &x) = 0;
  virtual chordID lookup_closestsucc (const chordID &x) = 0;
  
  // The RPCs
  virtual void doget_successor (user_args *sbp) = 0;
  virtual void doget_predecessor (user_args *sbp) = 0;
  virtual void dofindclosestpred (user_args *sbp, chord_findarg *fa) = 0;
  virtual void dotestrange_findclosestpred (user_args *sbp, chord_testandfindarg *fa) = 0;
  virtual void donotify (user_args *sbp, chord_nodearg *na) = 0;
  virtual void doalert (user_args *sbp, chord_nodearg *na) = 0;
  virtual void dogetsucclist (user_args *sbp) = 0;
  virtual void dogetfingers (user_args *sbp) = 0;
  virtual void dogetfingers_ext (user_args *sbp) = 0;
  virtual void dogetsucc_ext (user_args *sbp) = 0;
  virtual void dogetpred_ext (user_args *sbp) = 0;
  virtual void dosecfindsucc (user_args *sbp, chord_testandfindarg *fa) = 0;
  virtual void dogettoes (user_args *sbp, chord_gettoes_arg *ta) = 0;
  virtual void dodebruijn (user_args *sbp, chord_debruijnarg *da) = 0;
  virtual void dofindroute (user_args *sbp, chord_findarg *fa) = 0;

  //RPC demux
  virtual void addHandler (const rpc_program &prog, cbdispatch_t cb) = 0;
  virtual bool progHandled (int progno) = 0;
  virtual cbdispatch_t getHandler (unsigned long prog) = 0; 
};


class chord : public virtual refcount {

  int nvnode;
  chord_node wellknown_node;
  int myport;
  str myname;
  int ss_mode;
  int lookup_mode;
  ptr<axprt> x_dgram;
  vec<const rpc_program *> handledProgs;

  qhash<chordID, ref<vnode>, hashID> vnodes;

  void dispatch (ptr<asrv> s, svccb *sbp);
  void tcpclient_cb (int srvfd);
  int startchord (int myp);
  int startchord (int myp, int type);
  void stats_cb (const chordID &k, ptr<vnode> v);
  void print_cb (const chordID &k, ptr<vnode> v);
  void stop_cb (const chordID &k, ptr<vnode> v);
  
  // Number of received RPCs, for locationtable comm stuff
  ptr<u_int32_t> nrcv;

 public:
  enum { NCOORDS = 3 };

  // system wide default on the maximum number of vnodes/node.
  static const int max_vnodes;
  ptr<vnode> active;
  ptr<locationtable> locations; 
    
  chord (str _wellknownhost, int _wellknownport,
	 str _myname, int port, int max_cache,
	 int server_selection_mode,
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
  const rpc_program *get_program (int progno);

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
  long doRPC (chordID &n, const rpc_program &progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb) {
    return active->doRPC (n, progno, procno, in, out, cb);
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
