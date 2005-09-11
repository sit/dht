#ifndef _ROUTE_RECCHORD_H_
#define _ROUTE_RECCHORD_H_

#include "route.h"
#include <ihash.h>

class location;
struct timespec;
struct user_args;
struct recroute_complete_arg;

/**
 * A recursive variant of Chord routing.
 */
class route_recchord : public route_iterator {
 protected:
  timespec start_time_;
  u_long desired_;

  static u_long get_nonce ();
  void first_hop_cb (ptr<bool> del,
		     ptr<recroute_route_arg> ra,
		     recroute_route_stat *res,
		     ptr<location> p,
		     clnt_stat status);

  bool timeout_cb (ptr<bool> del,
		   ptr<recroute_route_arg> ra,
		   ptr<location> p,
		   chord_node n,
		   int retries);

 public:
  // must be public for ihash??
  u_long routeid_;
  ihash_entry<route_recchord> hlink_;

  route_recchord (ptr<vnode> vi, chordID xi);
  route_recchord (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args);
  ~route_recchord ();
  void print ();
  void send (ptr<chordID> guess);
  virtual void first_hop (cbhop_t cb, ptr<chordID> guess);
  void next_hop ();
  ptr<location> pop_back ();

  void set_desired (u_long m);
  void handle_complete (user_args *sbp, recroute_complete_arg *ca);
  void handle_timeout ();
  
  bool started () const;
  const timespec &start_time () const;
};

#endif /* !_ROUTE_RECCHORD_H_ */
