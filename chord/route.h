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

 public:
  route_iterator (ptr<vnode> vi, chordID xi) :
    v (vi), x (xi), r (CHORD_OK), done (false) {};
  chordID last_node () { return search_path.back (); };
  chordID key () { return x; };
  route path () { return search_path; };
  chordstat status () { return r; };
  virtual void print ();
  virtual void first_hop (cbhop_t cb) {};
  virtual void next_hop () {};
};

class route_chord : public route_iterator {
  void make_hop (chordID &n);
  void make_hop_cb (chord_testandfindres *res, clnt_stat err);
  void make_route_done_cb (chordID s, bool ok, chordstat status);
  void make_hop_done_cb (chordID s, bool ok, chordstat status);

  bool do_upcall;
  int uc_procno;
  ptr<void> uc_args;
  void *upcall_res;
  int upcall_res_len;
  rpc_program prog;

 public:
  route_chord (ptr<vnode> vi, chordID xi);
  route_chord (ptr<vnode> vi, chordID xi,
	       rpc_program uc_prog,
	       int uc_procno,
	       ptr<void> uc_args);

  void first_hop (cbhop_t cb);
  void next_hop ();
  void get_upcall_res (void *res);

};

class route_debruijn: public route_iterator {
  int hops;
  route virtual_path;
  void make_hop (chordID &n, chordID &x, chordID &d);
  void make_hop_cb (chord_debruijnres *res, clnt_stat err);
  void make_route_done_cb (chordID s, bool ok, chordstat status);
  void make_hop_done_cb (chordID d, chordID s, bool ok, chordstat status);
 public:
  route_debruijn (ptr<vnode> vi, chordID xi) : 
    route_iterator (vi, xi), hops (0) {};
  void print ();
  void first_hop (cbhop_t cb);
  void next_hop ();
};

#endif /* _ROUTE_ITERATOR_H_ */
