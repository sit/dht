#ifndef _DEBRUIN_H_
#define _DEBRUIN_H_

#include "chord_impl.h"
#include "stabilize.h"

#include <debruijn_prot.h>

// N.G. de Bruijn (http://www.win.tue.nl/~wsdwnb/)

class debruijn : public vnode_impl, public stabilizable {
  chordID mydoubleID;

  int logbase_;

  void debruijn::dodebruijn (user_args *sbp, debruijn_arg *da);
  void finddoublesucc_cb (vec<chord_node> sl, route path, chordstat status);
  void debruijn_upcall_done (debruijn_arg *da,
			     debruijn_res *res,
			     user_args *sbp,
			     bool stop);
  
 public:
  static ref<vnode> produce_vnode (ref<chord> _chordnode, 
				   ref<rpc_manager> _rpcm,
				   ref<location> _l);
  debruijn (ref<chord> _chordnode,
	    ref<rpc_manager> _rpcm,
	    ref<location> _l,
	    int logbase);
  ~debruijn ();

  virtual void print (strbuf &outbuf) const;
  
  virtual void dispatch (user_args *sbp);
  
  void fill_nodelistres (chord_nodelistres *res)
  {
    res->resok->nlist.setsize (0);
  }
  void fill_nodelistresext (chord_nodelistextres *res)
  {
    res->resok->nlist.setsize (0);
  }

  void stabilize();

  bool isstable () { return true; };
  bool continous_stabilizing () { return true; };
  bool backoff_stabilizing () { return false; };
  void do_backoff () { return; };
  void do_continuous () { stabilize (); };

  ptr<location> closestsucc (const chordID &x);
  virtual ptr<location> closestpred (const chordID &x, const vec<chordID> &f);
  ptr<location> debruijnptr (void);
  
  ptr<route_iterator> produce_iterator (chordID xi);
  ptr<route_iterator> produce_iterator (chordID xi,
					const rpc_program &uc_prog,
					int uc_procno,
					ptr<void> uc_args);
  route_iterator *produce_iterator_ptr (chordID xi);
  route_iterator *produce_iterator_ptr (chordID xi,
					const rpc_program &uc_prog,
					int uc_procno,
					ptr<void> uc_args);

};


class route_debruijn : public route_iterator {
  int hops;
  int logbase_;
  vec<chordID> virtual_path;
  vec<chordID> k_path;
  void make_hop (ptr<location> n, chordID &x, chordID &k, chordID &i);
  void make_hop_cb (ptr<bool> del, debruijn_res *res, clnt_stat err);
  void send_hop_cb (bool done);

 public:
  route_debruijn (ptr<debruijn> vi, chordID xi, int logbase);
  route_debruijn (ptr<debruijn> vi, chordID xi, int logbase,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args);
  ~route_debruijn () {};
  void send (ptr<chordID> guess);

  virtual void first_hop (cbhop_t cb, ptr<chordID> guess);
  void print ();
  void next_hop ();
  ptr<location> pop_back ();
};

#endif /* _DEBRUIN_H_ */
