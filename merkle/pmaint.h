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

  void stop ();

  enum { PRTTMSHORT = 1, PRTTMLONG = 60, MAX_PENDING = 20};
  enum { PMAINT_HANDOFF_ERROR = 0, PMAINT_HANDOFF_NOTPRESENT = 1, 
	 PMAINT_HANDOFF_PRESENT = -1};

private:

  //helpers from parent class
  dhashcli *cli;
  ptr<vnode> host_node;
  ptr<dbfe> db;
  delete_t delete_helper;

  bigint pmaint_offer_left;
  int pmaint_offer_next_succ;
  vec<u_int> pmaint_present_count;

  bigint pmaint_offer_right;
  bool pmaint_searching;
  bigint pmaint_next_key;

  vec<chord_node> pmaint_succs;

  timecb_t *active_cb;

  void pmaint_next ();
  void pmaint_lookup (chordID key, dhash_stat err, vec<chord_node> sl, route r);
  void pmaint_offer ();
  void pmaint_offer_cb (chord_node dst, vec<bigint> keys, ref<dhash_offer_res> res, 
			clnt_stat err);
  void pmaint_handoff (chord_node dst, bigint key, cbi cb);
  void pmaint_handoff_cb (bigint key, cbi cb, dhash_stat err, bool present);
  void handed_off_cb (chord_node dst,
		      vec<bigint> keys,
		     ref<dhash_offer_res> res,
		     int key_number,
		     int status);
  static bigint db_next (ptr<dbfe> db, bigint a);
  static vec<bigint> get_keys (ptr<dbfe> db, bigint a, bigint b, u_int maxcount);
};

#endif
