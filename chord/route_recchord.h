#ifndef _ROUTE_RECCHORD_H_
#define _ROUTE_RECCHORD_H_

#include "route.h"
#include <ihash.h>

class location;
struct timecb_t;
struct user_args;
struct recroute_complete_arg;

/**
 * A recursive variant of Chord routing.
 */
class route_recchord : public route_iterator {
  static long get_nonce ();
  void first_hop_cb (ptr<bool> del,
		     ptr<recroute_route_arg> ra,
		     recroute_route_stat *res,
		     ptr<location> p,
		     clnt_stat status);

 public:
  // must be public for ihash??
  long routeid_;
  ihash_entry<route_recchord> hlink_;

  route_recchord (ptr<vnode> vi, chordID xi);
  route_recchord (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args);
  ~route_recchord ();
  void print ();
  void send (ptr<chordID> guess);
  void first_hop (cbhop_t cb, ptr<chordID> guess);
  void next_hop ();
  ptr<location> pop_back ();

  void handle_complete (user_args *sbp, recroute_complete_arg *ca);
};

#endif /* !_ROUTE_RECCHORD_H_ */
