#ifndef _LOCATION_H_
#define _LOCATION_H_
/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */


#include "arpc.h"
#include "aclnt_chord.h"
#include "skiplist.h"
#include "chord_prot.h"
#include "chord_util.h"

typedef callback<void,chordstat>::ptr cbping_t;
typedef callback<void,chordID,bool,chordstat>::ref cbchallengeID_t;
extern cbchallengeID_t cbchall_null;

class chord;
class vnode;

// the identifier for the ihash class
struct hashID {
  hashID () {}
  hash_t operator() (const chordID &ID) const {
    return ID.getui ();
  }
};

struct location {
  chordID n;
  net_address addr;
  sockaddr_in saddr;
  unsigned int nrpc;
  bool alive; // whether this node responded to its last RPC
  timecb_t *checkdeadcb; // timer to check if this node has come back to life
  bool challenged; // whether this node has been succesfully challenged
  vec<cbchallengeID_t> outstanding_cbs;
  location (const chordID &_n, const net_address &_r);
  ~location ();
};

#include "comm.h"

class locationtable : public virtual refcount {
  typedef unsigned short loctype;
  static const loctype LOC_REGULAR = 1 << 0;
  static const loctype LOC_PINSUCC = 1 << 1;
  static const loctype LOC_PINPRED = 1 << 2;
  
  struct locwrap {
    ihash_entry<locwrap> hlink_;
    tailq_entry<locwrap> uselink_;
    sklist_entry<locwrap> sortlink_;

    ptr<location> loc_; 
    loctype type_;
    chordID n_;
    locwrap (ptr<location> l, loctype lt = LOC_REGULAR) :
      loc_ (l), type_ (lt) { n_ = l->n; }
    locwrap (const chordID &x, loctype lt) :
      loc_ (NULL), type_ (lt), n_ (x) { }
    bool good ();
  };
  
  ptr<u_int32_t> nrcv;
  ptr<rpc_manager> hosts;

  // Indices into our locations... for O(1) access, for expiring,
  //   for rapid successor/pred lookups.
  ihash<chordID, locwrap, &locwrap::n_, &locwrap::hlink_, hashID> locs;
  tailq<locwrap, &locwrap::uselink_> cachedlocs;
  skiplist<locwrap, chordID, &locwrap::n_, &locwrap::sortlink_> loclist;
  size_t good;

  u_int32_t size_cachedlocs;
  u_int32_t max_cachedlocs;

  u_long nnodessum;
  u_long nnodes;
  unsigned nvnodes;
  unsigned nchallenge;
  
  locationtable ();
  void initialize_rpcs ();
  
  void delete_cachedlocs ();
  void realinsert (ref<location> l);
  ptr<location> lookup (const chordID &n);
  
  bool betterpred1 (chordID current, chordID target, chordID newpred);

  void ping_cb (cbping_t cb, clnt_stat err);
  void challenge_cb (int challenge, ptr<location> l,
		     chord_challengeres *res, clnt_stat err);

  void printloc (locwrap *l);
  void doRPCcb (ptr<location> l, aclnt_cb realcb, clnt_stat err);

  void check_dead (ptr<location> l, unsigned int newwait);
  void check_dead_cb (ptr<location> l, unsigned int newwait,
		      chordID x, bool b, chordstat s);

  bool remove (locwrap *l);
  
 public:
  locationtable (ptr<u_int32_t> _nrcv, int _max_connections);
  locationtable (const locationtable &src);

  size_t size ();
  size_t usablenodes ();
  u_long estimate_nodes ();
  void replace_estimate (u_long o, u_long n);

  void incvnodes () { nvnodes++; };
  
  void insertgood (const chordID &n, sfs_hostname s, int p);
  void insert (const chordID &n, sfs_hostname s, int _p,
	       cbchallengeID_t cb);
  void cacheloc (const chordID &x, net_address &r, cbchallengeID_t cb);
  void pinsucc (const chordID &x);
  void pinpred (const chordID &x);
  
  bool lookup_anyloc (const chordID &n, chordID *r);
  chordID closestsuccloc (const chordID &x);
  chordID closestpredloc (const chordID &x, vec<chordID> failed);
  chordID closestpredloc (const chordID &x);

  long doRPC (const chordID &n, rpc_program progno, 
	      int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);
  void resendRPC (long seqno);

  void ping (const chordID &x, cbping_t cb);
  void challenge (const chordID &x, cbchallengeID_t cb);
  void get_node (const chordID &x, chord_node *n);
    
  // info about a particular location...
  bool alive (const chordID &x);
  bool challenged (const chordID &x);
  bool cached (const chordID &x);
  net_address & getaddress (const chordID &x);
  float get_a_lat (const chordID &x);
  void fill_getnodeext (chord_node_ext &data, const chordID &x);
  unsigned int get_nrpc(const chordID &x);
  
  //average stats
  float get_avg_lat ();
  float get_avg_var ();

  void stats ();
};

extern bool nochallenges;
extern int chord_rpc_style;
extern const int CHORD_RPC_STP;  // our own rpc style
extern const int CHORD_RPC_SFSU; // libarpc over UDP
extern const int CHORD_RPC_SFST; // libarpc over TCP

#endif /* _LOCATION_H_ */
