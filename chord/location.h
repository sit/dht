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

struct location {
  chordID n;
  net_address addr;
  sockaddr_in saddr;
  unsigned int nrpc;

  int vnode;  // the vnode # that will make this node legit.
  bool alive; // whether this node responded to its last RPC

  timecb_t *checkdeadcb; // timer to check if this node has come back to life
  location (const chordID &_n, const net_address &_r);
  ~location ();
};

class rpc_manager;

class locationtable : public virtual refcount {
  typedef unsigned short loctype;
  static const loctype LOC_REGULAR = 1 << 0;
  static const loctype LOC_PINSUCC = 1 << 1;
  static const loctype LOC_PINPRED = 1 << 2;
  static const loctype LOC_PINSUCCLIST = 1 << 3;
  static const loctype LOC_PINPREDLIST = 1 << 4;
  
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
  
  void initialize_rpcs ();
  
  void delete_cachedlocs ();
  void realinsert (ref<location> l);
  ptr<location> lookup (const chordID &n);
  
  bool betterpred1 (chordID current, chordID target, chordID newpred);

  void ping_cb (cbping_t cb, clnt_stat err);

  // Circular, in-order traversal of all known nodes.
  locwrap *next (locwrap *lw);
  locwrap *prev (locwrap *lw);
  
  void printloc (locwrap *l);
  
  void doRPCcb (ptr<location> l, aclnt_cb realcb, clnt_stat err);

  void check_dead (ptr<location> l, unsigned int newwait);
  void check_dead_cb (ptr<location> l, unsigned int newwait,
		      chordID x, bool b, chordstat s);

  bool remove (locwrap *l);
  void pin (const chordID &x, loctype pt);

  // NOT IMPLEMENTED (copy constructor)
  locationtable (const locationtable &src);

 public:
  locationtable (ptr<u_int32_t> _nrcv, int _max_connections);

  size_t size ();
  size_t usablenodes ();
  u_long estimate_nodes ();
  void replace_estimate (u_long o, u_long n);

  void incvnodes () { nvnodes++; };

  // Inserts node into LT.  Returns true if node is now available.
  // Returns false of n is not a plausible chordID for s:p.
  bool insert (const chordID &n, sfs_hostname s, int p);
  bool insert (const chordID &n, const net_address &r);
  bool insert (const chord_node &n) {
    return insert (n.x, n.r);
  }
  // Insert node into LT.  Backwards compatibility for old code;
  // Calls cb immediately with good or bad result.
  void insert (const chordID &n, sfs_hostname s, int _p,
	       cbchallengeID_t cb);
  // Alternate interface for insert with stupid name change.
  void cacheloc (const chordID &x, const net_address &r, cbchallengeID_t cb);
  
  void pinpredlist (const chordID &x);
  void pinsucclist (const chordID &x);
  void pinsucc (const chordID &x);
  void pinpred (const chordID &x);
  
  bool lookup_anyloc (const chordID &n, chordID *r);
  chordID closestsuccloc (const chordID &x);
  chordID closestpredloc (const chordID &x, vec<chordID> failed);
  chordID closestpredloc (const chordID &x);
  
  long doRPC (const chord_node &n, const rpc_program &prog, 
	      int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);
  long doRPC (const chordID &n, const rpc_program &prog, 
	      int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);
  void resendRPC (long seqno);

  void ping (const chordID &x, cbping_t cb);

  void get_node (const chordID &x, chord_node *n);
    
  // info about a particular location...
  bool alive (const chordID &x);
  bool challenged (const chordID &x);
  bool cached (const chordID &x);
  const net_address & getaddress (const chordID &x);
  float get_a_lat (const chordID &x);
  void fill_getnodeext (chord_node_ext &data, const chordID &x);
  unsigned int get_nrpc(const chordID &x);
  
  //average stats
  float get_avg_lat ();
  float get_avg_var ();

  void stats ();
};

extern int chord_rpc_style;
extern const int CHORD_RPC_STP;  // our own rpc style
extern const int CHORD_RPC_SFSU; // libarpc over UDP
extern const int CHORD_RPC_SFST; // libarpc over TCP

#endif /* _LOCATION_H_ */
