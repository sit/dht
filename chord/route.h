#ifndef _ROUTE_ITERATOR_H_
#define _ROUTE_ITERATOR_H_

#include <refcnt.h>
#include "chord.h"

class vnode;
typedef callback<void, bool>::ptr cbhop_t;

/**
 * A base class for interfacing with different hop-based routing mechanisms.
 *
 * After receiving a route_iterator from a factory, clients call one of
 * two methods:
 * 1. The send method performs a lookup but does not return any result
 *    (e.g.  giving back the route in a callback.)  It is usually used to
 *    send an upcall to each of the nodes that appears on the lookup
 *    path.  Typically, nodes on the path that receive the upcall will
 *    then choose to respond to the upcall by making an explicit RPC back
 *    to the origin.  Some external mechanism must be provided for
 *    associating this new RPC as being a "reply" for the original
 *    requesting RPC.
 * 2. The first_hop method performs a lookup but calls back to the
 *    calling layer at each hop, passing back the route iterator and
 *    whether or not the route is complete.  The calling layer must
 *    explicitly decide whether or not to continue the route by then
 *    calling next_hop.
 *
 * Each of these two methods also has two variants.  One allows the caller
 * to provide a guess for the first node to use.  The other gives the caller
 * the option of proceeding as "normal" or by using a potential cached
 * successor.
 *
 * XXX This could be made slightly cleaner in interface by just
 *     providing send(chordID *guess) or some such.  The "ucs" code could
 *     then be the same as the "guess" code where the caller looks up the
 *     current successor as the guess. guess could default to NULL in
 *     which case, we look up a guess.
 *
 * To cancel an in-progress lookup, users should delete the iterator.
 */
class route_iterator {
 protected:
  ptr<vnode> v;
  chordID x;
  vec<chord_node> successors_;
  route search_path;
  chordstat r;
  bool done;
  cbhop_t cb;
  ptr<bool> deleted;

  bool do_upcall;
  int uc_procno;
  ptr<void> uc_args;
  rpc_program prog;

  bool stop;
  bool last_hop;

  vec<chordID> failed_nodes;

 public:
  route_iterator (ptr<vnode> vi, chordID xi) :
    v (vi), x (xi), r (CHORD_OK), done (false), 
    deleted (New refcounted<bool> (false)),
    do_upcall (false), stop (false), last_hop (false) {};
  route_iterator (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args) :
    v (vi), x (xi), r (CHORD_OK), done (false), 
    deleted (New refcounted<bool> (false)),
    do_upcall (true),
    uc_procno (uc_procno), uc_args (uc_args), prog (prog),
    stop (false), last_hop (false) {};

  virtual ~route_iterator () { *deleted = true; };

  vec<chord_node> successors () { return successors_; }
  ptr<location> last_node () { return search_path.back (); };
  chordID key () { return x; };
  route path () { return search_path; };
  vec<chordID> failed_path () { return failed_nodes; };

  chordstat status () { return r; };

  virtual void print ();
  virtual void first_hop (cbhop_t cb, ptr<chordID> guess) = 0;
  virtual void next_hop () {};
  virtual void send (ptr<chordID> guess) = 0;

  virtual ptr<location> pop_back () = 0;

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

  void on_failure (chordID f);
  ptr<location> pop_back ();
};

class route_debruijn : public route_iterator {
  int hops;
  vec<chordID> virtual_path;
  vec<chordID> k_path;
  void make_hop (ptr<location> n, chordID &x, chordID &k, chordID &i);
  void make_hop_cb (ptr<bool> del, chord_debruijnres *res, clnt_stat err);
  void send_hop_cb (bool done);

 public:
  route_debruijn (ptr<vnode> vi, chordID xi);
  route_debruijn (ptr<vnode> vi, chordID xi,
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

class route_factory {
protected:
  ptr<vnode> vi;
public:
  route_factory (ptr<vnode> vi) : vi (vi) {};
  route_factory () {};
  
  void setvnode (ptr<vnode> v) { vi = v; };
  virtual ~route_factory () {};
  virtual ptr<route_iterator> produce_iterator (chordID xi) = 0;
  virtual ptr<route_iterator> produce_iterator (chordID xi,
						rpc_program uc_prog,
						int uc_procno,
						ptr<void> uc_args) = 0;
  virtual route_iterator* produce_iterator_ptr (chordID xi) = 0;
  virtual route_iterator* produce_iterator_ptr (chordID xi,
						rpc_program uc_prog,
						int uc_procno,
						ptr<void> uc_args) = 0;
  ptr<vnode> get_vnode () {return vi;};
  void get_node (chord_node_wire *n);
};

class debruijn_route_factory : public route_factory {
public:
  debruijn_route_factory (ptr<vnode> vi) : route_factory (vi) {};
  debruijn_route_factory () {};
  ptr<route_iterator> produce_iterator (chordID xi);
  ptr<route_iterator> produce_iterator (chordID xi,
					rpc_program uc_prog,
					int uc_procno,
					ptr<void> uc_args);
  virtual route_iterator* produce_iterator_ptr (chordID xi);
  virtual route_iterator* produce_iterator_ptr (chordID xi,
					rpc_program uc_prog,
					int uc_procno,
					ptr<void> uc_args);
};

class chord_route_factory : public route_factory {
public:
  chord_route_factory (ptr<vnode> vi) : route_factory (vi) {};
  chord_route_factory () {};
  ptr<route_iterator> produce_iterator (chordID xi);
  ptr<route_iterator> produce_iterator (chordID xi,
					rpc_program uc_prog,
					int uc_procno,
					ptr<void> uc_args);
  virtual route_iterator* produce_iterator_ptr (chordID xi);
  virtual route_iterator* produce_iterator_ptr (chordID xi,
					rpc_program uc_prog,
					int uc_procno,
					ptr<void> uc_args);
};
#endif /* _ROUTE_ITERATOR_H_ */
