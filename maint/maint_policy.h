#include <tame.h>
#include <qhash.h>
#include <dhash_types.h>
#include <adb_prot.h>

class adb;
class merkle_tree; 
struct maint_dhashinfo_t;
struct maint_repair_t;

// {{{ locationcc
class locationcc : public virtual refcount
{
  static vec<locationcc *> allocated;

  const chord_node n;

  tcpconnect_t *tcpc;
  ptr<axprt_stream> x;

  typedef callback<void, ptr<aclnt> >::ref aclntcb;
  vec<cbv> aclntcbs;
  void aclntmaker (const rpc_program *p, aclntcb cb);

  timecb_t *reapcaller;
  void reaper ();

protected:
  ~locationcc ();
  locationcc (const chord_node &n);
public:
  int vnode () const { return n.vnode_num; }
  chordID id () const { return n.x; }
  const chord_node &chordnode () const { return n; }
  static ptr<locationcc> alloc (const chord_node &n);
  void fill_ipportvn (u_int32_t &a, u_int32_t &b);
  void get_stream_aclnt (const rpc_program &p,
      aclntcb cb,
      CLOSURE);
};
// }}}
// {{{ syncers
struct syncer {
protected:
  syncer (dhash_ctype c) : ctype (c) {}
public:
  const dhash_ctype ctype;
  virtual ~syncer () {}

  static ref<syncer> produce_syncer (dhash_ctype c);
  virtual const rpc_program &sync_program () = 0;
  virtual void dispatch (ptr<merkle_tree> localtree, svccb *sbp) {
    sbp->reject (PROC_UNAVAIL);
  }
  virtual void sync_with (ptr<locationcc> who,
      chordID rngmin, chordID rngmax,
      ptr<merkle_tree> t,
      missingfnc_t m,
      cbv cb, CLOSURE) { delaycb (0, cb); }
};
struct merkle_sync : public syncer {
protected:
  merkle_sync (dhash_ctype c) : syncer (c) {}
public:
  static ref<syncer> produce_syncer (dhash_ctype c);
  const rpc_program &sync_program ();
  void dispatch (ptr<merkle_tree> localtree, svccb *sbp);
  void sync_with (ptr<locationcc> who, 
      chordID rngmin, chordID rngmax,
      ptr<merkle_tree> localtree, 
      missingfnc_t m,
      cbv cb, CLOSURE);
};
struct time_sync : public syncer {
protected:
  time_sync (dhash_ctype c) : syncer (c) {}
public:
  static ref<syncer> produce_syncer (dhash_ctype c);
  void dispatch (ptr<merkle_tree> localtree, svccb *sbp);
  void sync_with (ptr<locationcc> who, ptr<merkle_tree> localtree,
      chordID rngmin, chordID rngmax,
      missingfnc_t m,
      cbv cb, CLOSURE);
};
// }}}
// {{{ maintainers
class maintainer : public virtual refcount {
  maintainer (const maintainer &m);

public:
  const chord_node host;
  const dhash_ctype ctype;
  const ptr<syncer> sync;

  static const u_int32_t default_delay;

protected:
  u_int32_t efrags;
  u_int32_t dfrags;

  ptr<adb> db;
  const str private_path;

  bool running;     // Is maintenance in general active?
  bool in_progress; // Is an active synchronization cycle in progress?
  // RepInv: if (in_progress) then running.

  // Does not include host in either.
  vec<ptr<locationcc> > succs;
  vec<ptr<locationcc> > preds;

  u_int32_t delay;
  timecb_t *mainttimer;
  // RepInv: (!running && mainttimer == NULL)

  ptr<merkle_tree> ltree;

  virtual void run_cycle (cbv cb, CLOSURE);
  void start_helper ();
  virtual void update_neighbors (cbv cb, CLOSURE);
  virtual void process_neighbors (const vec<ptr<locationcc> > &preds,
      const vec<ptr<locationcc> > &succs, cbv cb, CLOSURE);
  virtual void restart (u_int32_t delay = default_delay);

  maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s);

public:
  // static ref<maintainer> produce_maintainer (const chord_node &h, dhash_ctype c, ptr<syncer> s);
  virtual ~maintainer ();

  virtual void start (u_int32_t delay = default_delay);
  virtual void stop ();

  virtual ptr<merkle_tree> localtree () { return ltree; }
  virtual void getrepairs (chordID start, int thresh, int count,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs) {}
};

class carbonite: public maintainer {
  void init_ltree (cbv cb, adb_status err, str path, bool hasaux);
  void handle_missing (ptr<locationcc> from, ptr<merkle_tree> t, chordID key, bool missing_local);
  void process_neighbors (const vec<ptr<locationcc> > &preds,
      const vec<ptr<locationcc> > &succs, cbv cb, CLOSURE);

  carbonite (const carbonite &m);
protected:
  carbonite (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb);
public:
  static ref<maintainer> produce_maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb);
  void getrepairs (chordID start, int thresh, int count,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs);
  ~carbonite ();
};

class passingtone: public maintainer {
  vec<chordID> repairqueue;
  vec<ptr<locationcc> > repairfrom;
  void handle_missing (ptr<locationcc> from, ptr<merkle_tree> t, chordID key, bool missing_local);
  void process_neighbors (const vec<ptr<locationcc> > &preds,
      const vec<ptr<locationcc> > &succs, cbv cb, CLOSURE);

  void init_ltree (cbv cb, adb_status err, str path, bool hasaux);
  passingtone (const passingtone &m);
protected:
  passingtone (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb);
public:
  static ref<maintainer> produce_maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb);
  ~passingtone ();

  void getrepairs (chordID start, int thresh, int count,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs);
};
// }}}

typedef ref<maintainer> (*maintainer_producer_t) (str, maint_dhashinfo_t *, ptr<syncer>, cbv);
typedef ref<syncer> (*syncer_producer_t) (dhash_ctype);

// vim: foldmethod=marker
