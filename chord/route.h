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
  cbhop_t cb;
  ptr<bool> deleted;

  bool do_upcall;
  int uc_procno;
  ptr<void> uc_args;
  rpc_program prog;

  vec<chordID> failed_nodes;

 public:
  route_iterator (ptr<vnode> vi, chordID xi) :
    v (vi), x (xi), r (CHORD_OK),
    deleted (New refcounted<bool> (false)),
    do_upcall (false) {};
  route_iterator (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args) :
    v (vi), x (xi), r (CHORD_OK),
    deleted (New refcounted<bool> (false)),
    do_upcall (true),
    uc_procno (uc_procno), uc_args (uc_args), prog (prog)
    {};

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

#endif /* _ROUTE_ITERATOR_H_ */
