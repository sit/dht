#ifndef _ROUTE_ITERATOR_H_
#define _ROUTE_ITERATOR_H_

#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include "chord.h"

typedef callback<void,bool>::ptr cbhop_t;

class route_iterator : public virtual refcount {
 protected:
  ptr<vnode> v;
  chordID x;
  route search_path;
  chordstat r;
  bool done;
  cbhop_t cb;

  bool do_upcall;
  int uc_procno;
  ptr<void> uc_args;
  rpc_program prog;

  bool stop;
  bool last_hop;

 public:
  route_iterator (ptr<vnode> vi, chordID xi) :
    v (vi), x (xi), r (CHORD_OK), done (false), do_upcall (false), stop (false),
    last_hop (false) {};
  route_iterator (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args) :
    v (vi), x (xi), r (CHORD_OK), done (false), do_upcall (true),
    uc_procno (uc_procno), uc_args (uc_args), prog (prog),
    stop (false), last_hop (false) {};
    

  chordID last_node () { return search_path.back (); };
  chordID key () { return x; };
  route path () { return search_path; };
  chordstat status () { return r; };

  virtual void print ();
  virtual void first_hop (cbhop_t cb) {};
  virtual void next_hop () {};
  virtual void send (chordID guess) = 0;
  virtual void send (bool ucs) = 0;

  static char * marshall_upcall_args (rpc_program *prog, 
				      int uc_procno,
				      ptr<void> uc_args,
				      int *upcall_args_len);
  
  static bool unmarshall_upcall_res (rpc_program *prog, 
				     int uc_procno, 
				     void *upcall_res,
				     int upcall_res_len,
				     void *dest);

};

class route_chord : public route_iterator {
  void make_hop (chordID &n);
  void make_hop_cb (chord_testandfindres *res, clnt_stat err);
  void make_route_done_cb (chordID s, bool ok, chordstat status);
  void make_hop_done_cb (chordID s, bool ok, chordstat status);
  void send_hop_cb (bool done);

 public:
  route_chord (ptr<vnode> vi, chordID xi);
  route_chord (ptr<vnode> vi, chordID xi,
	       rpc_program uc_prog,
	       int uc_procno,
	       ptr<void> uc_args);

  void first_hop (cbhop_t cb, bool ucs = false);
  void first_hop (cbhop_t cb, chordID guess);
  void send (chordID guess);
  void send (bool ucs = false);

  void next_hop ();
};

class route_debruijn : public route_iterator {
  int hops;
  route virtual_path;
  void make_hop (chordID &n, chordID &x, chordID &d);
  void make_hop_cb (chord_debruijnres *res, clnt_stat err);
  void make_route_done_cb (chordID s, bool ok, chordstat status);
  void make_hop_done_cb (chordID d, chordID s, bool ok, chordstat status);
  void send_hop_cb (bool done);

 public:
  route_debruijn (ptr<vnode> vi, chordID xi);
  route_debruijn (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args);

  void send (chordID guess);
  void send (bool ucs);
  void first_hop (cbhop_t cb, bool ucs); // ucs = false, by default
  void first_hop (cbhop_t cb, chordID guess);
  void print ();
  void next_hop ();
};

class route_factory {
protected:
  ptr<vnode> vi;
public:
  route_factory (ptr<vnode> vi) : vi (vi) {};
  virtual ~route_factory () {};
  virtual ptr<route_iterator> produce_iterator (chordID xi) = 0;
  virtual ptr<route_iterator> produce_iterator (chordID xi,
						rpc_program uc_prog,
						int uc_procno,
						ptr<void> uc_args) = 0;
  ptr<vnode> get_vnode () {return vi;};
  void get_node (chord_node *n) {vi->locations->get_node (vi->my_ID (),
							  n); };
};

class debruijn_route_factory : public route_factory {
public:
  debruijn_route_factory (ptr<vnode> vi) : route_factory (vi) {};
  
  ptr<route_iterator> produce_iterator (chordID xi);
  ptr<route_iterator> produce_iterator (chordID xi,
					rpc_program uc_prog,
					int uc_procno,
					ptr<void> uc_args);
};

class chord_route_factory : public route_factory {
public:
  chord_route_factory (ptr<vnode> vi) : route_factory (vi) {};
  
  ptr<route_iterator> produce_iterator (chordID xi);
  ptr<route_iterator> produce_iterator (chordID xi,
					rpc_program uc_prog,
					int uc_procno,
					ptr<void> uc_args);
};
#endif /* _ROUTE_ITERATOR_H_ */
