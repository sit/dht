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

#include <sfsmisc.h>
#include <arpc.h>
#include <crypt.h>
#include <vec.h>
#include <qhash.h>

#ifdef DMALLOC
#include <dmalloc.h>
#endif

#include <id_utils.h>
#include <chord_prot.h>
#include <transport_prot.h>

typedef int cb_ID;

class chord;
class vnode;

class route_factory;
class chord;
class location;
class locationtable;
class rpc_manager;
class finger_table;
struct user_args;

typedef vec<ptr<location> > route;
class route_iterator;

typedef callback<void,ptr<vnode>,chordstat>::ref cbjoin_t;
typedef callback<void,chord_node,chordstat>::ref cbchordID_t;
typedef callback<void,vec<chord_node>,chordstat>::ref cbchordIDlist_t;
typedef callback<void,vec<chord_node>,route,chordstat>::ref cbroute_t;
typedef callback<void, user_args *>::ref cbdispatch_t;
typedef callback<void,chordstat>::ptr cbping_t;
typedef callback<void, bool>::ref cbupcalldone_t;
typedef callback<void, int, void *, cbupcalldone_t>::ref cbupcall_t; 

typedef callback<ref<vnode>, ref<chord>, ref<rpc_manager>, ref<location> >::ref vnode_producer_t;
typedef callback<ptr<finger_table>, ptr<vnode>, ptr<locationtable> >::ref cb_fingertableproducer_t;

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
  
  void reject (auth_stat s) {sbp->reject (s); delete this; }
  void reject (accept_stat s) {sbp->reject (s); delete this; }
  void reply (void *res);
  void replyref (const int &res);
  
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
    (ref<chord> _chordnode, 
     ref<rpc_manager> _rpcm,
     ref<location> _l);

  virtual ~vnode (void) = 0;

  virtual ref<location> my_location () = 0;
  virtual chordID my_ID () const = 0;
  virtual ptr<location> my_pred () const = 0;
  virtual ptr<location> my_succ () const = 0;

  virtual ptr<route_iterator> produce_iterator (chordID xi) = 0;
  virtual ptr<route_iterator> produce_iterator (chordID xi,
						const rpc_program &uc_prog,
						int uc_procno,
						ptr<void> uc_args) = 0;
  virtual route_iterator *produce_iterator_ptr (chordID xi) = 0;
  virtual route_iterator *produce_iterator_ptr (chordID xi,
						const rpc_program &uc_prog,
						int uc_procno,
						ptr<void> uc_args) = 0;

  // The API
  virtual void stabilize (void) = 0;
  virtual void join (ptr<location> n, cbjoin_t cb) = 0;
  virtual void get_successor (ptr<location> n, cbchordID_t cb) = 0;
  virtual void get_predecessor (ptr<location> n, cbchordID_t cb) = 0;
  virtual void get_succlist (ptr<location> n, cbchordIDlist_t cb) = 0;
  virtual void notify (ptr<location> n, chordID &x) = 0;
  virtual void alert (ptr<location> n, chordID &x) = 0;
  virtual void ping (ptr<location> n, cbping_t cb) = 0;
  virtual void find_successor (const chordID &x, cbroute_t cb) = 0;
  virtual void find_succlist (const chordID &x, u_long m, cbroute_t cb,
			      ptr<chordID> guess = NULL) = 0;

  //upcall
  virtual void register_upcall (int progno, cbupcall_t cb) = 0;

  // For other modules
  virtual long doRPC (const chord_node &ID, const rpc_program &prog, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb) = 0;
  virtual long doRPC (ref<location> l, const rpc_program &prog, int procno,
		      ptr<void> in, void *out, aclnt_cb cb) = 0;

  virtual void resendRPC (long seqno) = 0;
  virtual void fill_user_args (user_args *a) = 0;
  
  virtual void stats (void) const = 0;
  virtual void print (strbuf &outbuf) const = 0;
  virtual void stop (void) = 0;
  virtual vec<ptr<location> > succs () = 0;
  virtual vec<ptr<location> > preds () = 0;

  virtual ptr<location> closestpred (const chordID &x, const vec<chordID> &f) = 0;

  //RPC demux
  virtual void addHandler (const rpc_program &prog, cbdispatch_t cb) = 0;
  virtual bool progHandled (int progno) = 0;
  virtual cbdispatch_t getHandler (unsigned long prog) = 0; 
};


class chord : public virtual refcount {
  // system wide default on the maximum number of vnodes/node.
  int max_vnodes;

  int nvnode;
  ptr<location> wellknown_node;
  int myport;
  str myname;
  ptr<axprt> x_dgram;
  vec<const rpc_program *> handledProgs;

  qhash<chordID, ref<vnode>, hashID> vnodes;

  void dispatch (ptr<asrv> s, svccb *sbp);
  void tcpclient_cb (int srvfd);
  int startchord (int myp, int type);
  void stats_cb (const chordID &k, ptr<vnode> v);
  void print_cb (strbuf outbuf, const chordID &k, ptr<vnode> v);
  void stop_cb (const chordID &k, ptr<vnode> v);
  
  // Number of received RPCs, for locationtable comm stuff
  ptr<u_int32_t> nrcv;
  ptr<rpc_manager> rpcm;

 public:
  enum { NCOORDS = 3 };

  ptr<vnode> active;
  ptr<locationtable> locations; 
    
  chord (str _wellknownhost, int _wellknownport,
	 str _myname, int port, int max_cache);

  int startchord (int myp);
  ptr<vnode> newvnode (vnode_producer_t p, cbjoin_t cb);
  void stats (void);
  void print (strbuf &outbuf);
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

  ptr<location> closestpred (chordID k, vec<chordID> f) { 
    return active->closestpred (k, f); 
  };
  void find_succlist (chordID n, u_long m, cbroute_t cb, ptr<chordID> guess) {
    active->find_succlist (n, m, cb, guess);
  }
  void find_successor (chordID n, cbroute_t cb) {
    active->find_successor (n, cb);
  };
  void get_predecessor (ptr<location> n, cbchordID_t cb) {
    active->get_predecessor (n, cb);
  };
  long doRPC (ptr<location> n, const rpc_program &progno, int procno,
	      ptr<void> in, void *out, aclnt_cb cb) {
    return active->doRPC (n, progno, procno, in, out, cb);
  };
  void alert (ptr<location> n, chordID &x) {
    active->alert (n, x);
  };
  chordID clnt_ID () {
    return active->my_ID ();
  };    
};

#endif /* _CHORD_H_ */
