#ifndef __PMAINT_H__
#define __PMAINT_H__

#include <chord.h>
#include <chord_prot.h>

#include <dhash_common.h>
#include <dhash.h>
#include <dhashcli.h>
#include <location.h>

typedef callback<void, ref<dbrec> >::ref delete_t;

class pmaint {

public:
  pmaint (dhashcli *cli, ptr<vnode> host_node, ptr<dbfe> db, 
	  delete_t delete_helper);

  enum { PRTTM = 5 };

private:

  //helpers from parent class
  dhashcli *cli;
  ptr<vnode> host_node;
  ptr<dbfe> db;
  delete_t delete_helper;

  bigint pmaint_offer_left;
  bigint pmaint_offer_right;
  bool pmaint_searching;
  bigint pmaint_next_key;

  vec<chord_node> pmaint_succs;
  qhash<chordID, bool, hashID> pmaint_handoff_tbl;
  u_int pmaint_offers_pending;
  u_int pmaint_offers_erred;

  void pmaint_next ();
  void pmaint_lookup (chordID key, dhash_stat err, vec<chord_node> sl, route r);
  void pmaint_offer ();
  void pmaint_offer_cb (chord_node dst, vec<bigint> keys, ref<dhash_offer_res> res, 
			clnt_stat err);
  void pmaint_handoff (chord_node dst, bigint key);
  void pmaint_handoff_cb (bigint key, dhash_stat err, bool present);

  static bigint db_next (ptr<dbfe> db, bigint a);
  static vec<bigint> get_keys (ptr<dbfe> db, bigint a, bigint b, u_int maxcount);
};

#endif
