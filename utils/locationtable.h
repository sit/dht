
#include <skiplist.h>
#include <chord_types.h>

#include <ihash.h>
#include <list.h>

class location;
struct hashID;

class locationtable {
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
    locwrap (ptr<location> l, loctype lt = LOC_REGULAR);
    locwrap (const chordID &x, loctype lt) :
      loc_ (NULL), type_ (lt), n_ (x) { }
    bool good ();
  };
  
  // Indices into our locations... for O(1) access, for expiring,
  //   for rapid successor/pred lookups.
  ihash<chordID, locwrap, &locwrap::n_, &locwrap::hlink_, hashID> locs;
  tailq<locwrap, &locwrap::uselink_> cachedlocs;
  skiplist<locwrap, chordID, &locwrap::n_, &locwrap::sortlink_> loclist;

  u_int32_t size_cachedlocs;
  u_int32_t max_cachedlocs;

  u_long nnodessum;
  u_long nnodes;
  unsigned nvnodes;

  void delete_cachedlocs ();
  void realinsert (ref<location> l);

  // Circular, in-order traversal of all known nodes.
  locwrap *next (locwrap *lw);
  locwrap *prev (locwrap *lw);

  bool remove (locwrap *l);
  void pin (const chordID &x, loctype pt);

  // NOT IMPLEMENTED (copy constructor)
  locationtable (const locationtable &src);

 public:
  locationtable (int _max_cache);

  size_t size ();
  size_t usablenodes ();
  u_long estimate_nodes ();
  void replace_estimate (u_long o, u_long n);

  void incvnodes () { nvnodes++; };

  // Inserts node into LT.  Returns true if node is now available.
  // Returns false of n is not a plausible chordID for s:p.
  ptr<location> insert (const chord_node &n);
  ptr<location> insert (const chordID &n, 
			const chord_hostname &s, 
			int p, int v,
			const vec<float> &coords);
  
  void pinpredlist (const chordID &x);
  void pinsucclist (const chordID &x);
  void pinsucc (const chordID &x);
  void pinpred (const chordID &x);
  
  bool lookup_anyloc (const chordID &n, chordID *r);
  ptr<location> closestsuccloc (const chordID &x);
  ptr<location> closestpredloc (const chordID &x, vec<chordID> failed);
  ptr<location> closestpredloc (const chordID &x);

  ptr<location> lookup (const chordID &x);
  bool cached (const chordID &x);

  //iterating over locations
  ptr<location> first_loc ();
  ptr<location> next_loc (chordID n);
  
#if 0    
  //average stats
  float get_avg_lat ();
  float get_avg_var ();

  void stats ();
#endif /* 0 */
};
