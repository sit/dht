#ifndef _LOCATIONTABLE_H_
#define _LOCATIONTABLE_H_

#include <skiplist.h>
#include <chord_types.h>

#include <ihash.h>
#include <list.h>

class location;
struct Coord;
struct hashID;

class locationtable {
  struct pininfo {
    sklist_entry<pininfo> sortlink_;
    
    chordID n_;
    bool pinself_;
    u_short pinsucc_;
    u_short pinpred_;
    pininfo (chordID n, bool pinself, u_short pinsucc, u_short pinpred) :
      n_ (n), pinself_ (pinself), pinsucc_ (pinsucc), pinpred_ (pinpred)
    {};
  };
  
  struct locwrap {
    ihash_entry<locwrap> hlink_;
    tailq_entry<locwrap> uselink_;
    sklist_entry<locwrap> sortlink_;

    ptr<location> loc_; 
    chordID n_;
    bool pinned_;
    locwrap (ptr<location> l);
    bool good ();
  };
  
  // Indices into our locations... for O(1) access, for expiring,
  //   for rapid successor/pred lookups.
  ihash<chordID, locwrap, &locwrap::n_, &locwrap::hlink_, hashID> locs;
  tailq<locwrap, &locwrap::uselink_> cachedlocs;
  skiplist<locwrap, chordID, &locwrap::n_, &locwrap::sortlink_> loclist;

  // Pin list
  skiplist<pininfo, chordID, &pininfo::n_, &pininfo::sortlink_> pinlist;
  
  u_int32_t max_cachedlocs;

  u_long nnodessum;
  u_long nnodes;
  unsigned nvnodes;

  bool pins_updated_;

  void process_pin (pininfo *cpin, int num);
  void figure_pins ();
  void evict (size_t n);
  ptr<location> realinsert (ref<location> l);

  // Circular, in-order traversal of all known nodes.
  locwrap *next (locwrap *lw);
  locwrap *prev (locwrap *lw);

  bool remove (locwrap *l);

  // NOT IMPLEMENTED (copy constructor)
  locationtable (const locationtable &src);

 public:
  locationtable (int _max_cache);
  ~locationtable ();

  size_t size ();
  size_t usablenodes ();
  u_long estimate_nodes ();
  void replace_estimate (u_long o, u_long n);

  void incvnodes () { nvnodes++; };

  // Inserts node into LT.  Returns true if node is now available.
  // Returns false of n is not a plausible chordID for s:p.
  ptr<location> insert (const chord_node &n);
  ptr<location> insert (ptr<location> l);
  ptr<location> insert (const chordID &n, 
			const chord_hostname &s, 
			int p, int v,
			const Coord &coords,
			time_t k, time_t a, int32_t b, bool m);

  void pin (const chordID &x, short num = 0);
  void unpin (const chordID &x);

  void flush (void);
  
  bool lookup_anyloc (const chordID &n, chordID *r);
  ptr<location> closestsuccloc (const chordID &x);
  ptr<location> closestpredloc (const chordID &x, vec<chordID> failed);
  ptr<location> closestpredloc (const chordID &x);

  // Like insert, but doesn't cache it.  If it is already
  // cached, return the existing object.
  ptr<location> lookup_or_create (const chord_node &n);
  ptr<location> lookup (const chordID &x);

  bool cached (const chordID &x);
  bool pinned (const chordID &x); // is an actual ptr<location> that is pinned.

  //iterating over locations
  ptr<location> first_loc ();
  ptr<location> next_loc (const chordID &n);
  
#if 0    
  //average stats
  float get_avg_lat ();
  float get_avg_var ();

  void stats ();
#endif /* 0 */
};

#endif /* !_LOCATIONTABLE_H_ */
