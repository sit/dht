#ifndef __PMAINT_H__
#define __PMAINT_H__

#include <chord.h>
#include <chord_prot.h>

#include <dhash_common.h>
#include <dhash.h>
#include <dhashcli.h>
#include <ihash.h>
#include <location.h>

typedef callback<void, ref<dbrec> >::ref delete_t;


class offer_state_item {
public:
  bool handed_off; //already gone?
  char offered_to; // we've tried to give it to this many successors
  char refused_by; // this many already had it
  char errored_by; // this many returned an error
  bigint key; // the key

  ihash_entry<offer_state_item> hlink_;

  offer_state_item (bigint k) : handed_off (0), offered_to (0),
				refused_by (0), errored_by (0),
				key (k) {};
};

class offer_state {

  ihash<bigint, offer_state_item, &offer_state_item::key, &offer_state_item::hlink_, hashID> keys;

  int outstanding;

public:
  offer_state () : outstanding (0) {};

  void clear ();
  void add_work (bigint left, bigint right, ptr<dbfe> db);
  int keys_outstanding ();
  vec<bigint> outstanding_keys (int n);
  vec<bigint> rejected_keys ();
  void mark_deleted (bigint k);
  void inc_offered (bigint k);
  void inc_rejected (bigint k);
  bigint left_key (); 
  bool handed_off (bigint k) { return keys[k]->handed_off; };
};

class pmaint {

public:
  pmaint (dhashcli *cli, ptr<vnode> host_node, ptr<dbfe> db, 
	  delete_t delete_helper);

  void start ();
  void stop ();

  enum { PRTTMTINY = 1, PRTTMSHORT = 10, PRTTMLONG = 60, MAX_PENDING = 20};
  enum { PMAINT_HANDOFF_ERROR = 0, PMAINT_HANDOFF_NOTPRESENT = 1, 
	 PMAINT_HANDOFF_PRESENT = -1};

  static bigint db_next (ptr<dbfe> db, bigint a);
  static vec<bigint> get_keys (ptr<dbfe> db, bigint a, bigint b, 
			       u_int maxcount);

private:

  //helpers from parent class
  dhashcli *cli;
  ptr<vnode> host_node;
  ptr<dbfe> db;
  delete_t delete_helper;

  //keep track of state during offer
  bigint pmaint_offer_left;
  u_int pmaint_offer_next_succ;
  offer_state work;

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
};

#endif
