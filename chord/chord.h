#ifndef _CHORD_H_
#define _CHORD_H_

/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include "sys/time.h"
#include "vec.h"
#include "qhash.h"

#include <chord_prot.h>
#include <chord_util.h>
#include <location.h>

#define NBIT     160     // size of Chord identifiers in bits
#define NIMM     NBIT    // number of immediate successors

typedef int cb_ID;

typedef vec<chordID> route;
typedef callback<void,chordID,net_address,chordstat>::ref cbsfsID_t;
typedef callback<void,chordID,route,chordstat>::ref cbroute_t;
typedef callback<void,chordID,char>::ref cbaction_t;
typedef callback<void,chordID,chordID,callback<void,int>::ref >::ref cbsearch_t;
typedef callback<void,int>::ref cbtest_t;

#define ACT_NODE_JOIN 1
#define ACT_NODE_UPDATE 2
#define ACT_NODE_LEAVE 3

struct findpredecessor_cbstate {
  chordID x;
  chordID nprime;
  route search_path;
  cbroute_t cb;
  findpredecessor_cbstate (chordID xi, chordID npi, route spi, cbroute_t cbi) :
    x (xi), nprime (npi), search_path (spi), cb (cbi) {};
};

struct searchcb_entry {
  cbsearch_t cb;
  list_entry<searchcb_entry> link;
  searchcb_entry (cbsearch_t scb) : cb (scb) {  };
};


struct finger {
  chordID start;
  node first; // first ID after start
};

class vnode : public virtual refcount  {
  static const int stabilize_timer = 1000;      // milliseconds
  static const int max_retry = 5;
  
  ptr<locationtable> locations;
  ptr<chord> chordnode;
  finger finger_table[NBIT+1];
  node succlist[NBIT+1];
  int nsucc;
  node predecessor;

  int ngetsuccessor;
  int ngetpredecessor;
  int nfindsuccessor;
  int nhops;
  int nfindpredecessor;
  int nfindsuccessorrestart;
  int nfindpredecessorrestart;
  int ntestrange;
  int nnotify;
  int nalert;
  int ngetfingers;

  int ndogetsuccessor;
  int ndogetpredecessor;
  int ndofindclosestpred;
  int ndonotify;
  int ndoalert;
  int ndotestrange;
  int ndogetfingers;

  timecb_t *stabilize_tmo;
  vec<cbaction_t> actionCallbacks;
  list<searchcb_entry, &searchcb_entry::link> searchCallbacks;

  void updatefingers (chordID &x, net_address &r);
  chordID findpredfinger (chordID &x);
  void print ();

  void stabilize_getpred_cb (chordID s, net_address r, chordstat status);
  void stabilize_findsucc_cb (int i, chordID s, route path, chordstat status);
  void stabilize_getsucc_cb (chordID s, net_address r, chordstat status);
  void stabilize_getsucclist_cb (int i, chordID s, net_address r, 
				 chordstat status);
  void join_getsucc_cb (chordID s, route r, chordstat status);
  void find_closestpred_cb (chordID n, findpredecessor_cbstate *st,
			    chord_noderes *res, clnt_stat err);
  void test_and_find_cb (chord_testandfindres *res, 
			 findpredecessor_cbstate *st, clnt_stat err);
  void find_successor_cb (cbroute_t cb, route sp, chordID s, net_address r,
			    chordstat status);
  void find_predecessor_cb (cbroute_t cb, chordID x, chordID p, 
			    route search_path, chordstat status);
  void find_pred_test_cache_cb (chordID n, chordID x, cbroute_t cb, int found);
  void find_closestpred_succ_cb (findpredecessor_cbstate *st, chordID s,
				 net_address r, chordstat status);
  void find_closestpred_test_cache_cb (chordID node, 
				       findpredecessor_cbstate *st, int found);
  void get_successor_cb (chordID n, cbsfsID_t cb, chord_noderes *res, 
			 clnt_stat err);
  void get_succ_cb (callback<void, chordID, chordstat>::ref cb, 
		    chordID succ, net_address r, chordstat err);
  void get_predecessor_cb (chordID n, cbsfsID_t cb, chord_noderes *res, 
			 clnt_stat err);
  void notify_cb (chordID n, chordstat *res, clnt_stat err);
  void alert_cb (chordstat *res, clnt_stat err);
  void get_fingers (chordID &x);
  void get_fingers_cb (chordID x, chord_getfingersres *res, clnt_stat err);

  void dofindsucc_cb (cbroute_t cb, chordID n, chordID x,
		      route search_path, chordstat status);
 public:
  chordID myID;
  ihash_entry<vnode> fhlink;

  vnode (ptr<locationtable> _locations, ptr<chord> _chordnode, chordID _myID);
  chordID my_ID () { return myID; };
  chordID my_pred () { return predecessor.n; };
  chordID my_succ () { return finger_table[1].first.n; };

  // The API
  void stabilize (int i);
  void join ();
  void find_predecessor (chordID &n, chordID &x, cbroute_t cb);
  void find_predecessor_restart (chordID &n, chordID &x, route search_path,
				 cbroute_t cb);
  void find_successor (chordID &n, chordID &x, cbroute_t cb);
  void find_successor_restart (chordID &n, chordID &x, route search_path, 
			       cbroute_t cb);
  void get_successor (chordID n, cbsfsID_t cb);
  void get_succ (chordID n, callback<void, chordID, chordstat>::ref cb);
  void get_predecessor (chordID n, cbsfsID_t cb);
  void notify (chordID &n, chordID &x);
  void alert (chordID &n, chordID &x);
  void dofindsucc (chordID &n, cbroute_t cb);

  // The RPCs
  void doget_successor (svccb *sbp);
  void doget_predecessor (svccb *sbp);
  void dofindclosestsucc (svccb *sbp, chord_findarg *fa);  
  void dofindclosestpred (svccb *sbp, chord_findarg *fa);
  void dotestandfind (svccb *sbp, chord_testandfindarg *fa);
  void donotify (svccb *sbp, chord_nodearg *na);
  void doalert (svccb *sbp, chord_nodearg *na);
  void dogetfingers (svccb *sbp);

  // For other modules
  int countrefs (chordID &x);
  void deletefingers (chordID &x);
  void stats (void);

  // For dhash
  void timing_cb(aclnt_cb cb, location *l, ptr<struct timeval> start, 
		 int procno, rpc_program progno, clnt_stat err);

  searchcb_entry * registerSearchCallback(cbsearch_t cb);
  void removeSearchCallback(searchcb_entry *scb);
  void testSearchCallbacks(chordID id, chordID target, cbtest_t cb);
  void tscb (chordID id, chordID x, searchcb_entry *scb, cbtest_t cb);
  void tscb_cb (chordID id, chordID x, searchcb_entry *scb, cbtest_t cb, 
		int result);
  void registerActionCallback(cbaction_t cb);
  void doActionCallbacks(chordID id, char action);
};

class chord : public virtual refcount {
  int nvnode;
  vec< ptr<asrv> > srv;
  net_address wellknownhost;
  net_address myaddress;
  chordID wellknownID;
  ihash<chordID,vnode,&vnode::myID,&vnode::fhlink,hashID> vnodes;

  int ngetsuccessor;
  int ngetpredecessor;
  int nfindclosestpred;
  int nnotify;
  int nalert;
  int ntestrange;
  int ngetfingers;

  void dispatch (svccb *sbp);
  void doaccept (int fd);
  void accept_standalone (int lfd);
  int startchord (int myp);
  chordID initID (int index);

 public:
  // locations contains all nodes that appear as fingers in vnodes plus
  // a number of cached nodes.  the cached nodes have refcnt = 0
  ptr<locationtable> locations; 
  chord (str _wellknownhost, int _wellknownport, const chordID &_wellknownID,
	 int port, str myhost, int set_rpcdelay);
  void newvnode (void);
  void newvnode (chordID &x);
  void deletefingers (chordID &x);
  int countrefs (chordID &x);
  void stats (void);
  void doRPC (chordID &n, rpc_program progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb) {
    locations->doRPC (n, progno, procno, in, out, cb);
  }

};

extern ptr<chord> chordnode;

#endif _CHORD_H_


