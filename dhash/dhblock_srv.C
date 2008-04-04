#include <chord.h>
#include "dhash.h"
#include "dhash_common.h"
#include "dhashcli.h"

#include <locationtable.h>
#include <location.h>
#include <libadb.h>

#include <dhblock_srv.h>

#include <configurator.h>
#include <modlogger.h>
#define info    modlogger ("dbhlock_srv", modlogger::INFO)
#define trace   modlogger ("dhblock_srv", modlogger::TRACE)

dhblock_srv::dhblock_srv (ptr<vnode> node,
			  ptr<dhashcli> cli,
		          dhash_ctype c,
			  str msock,
			  str dbsock,
			  str dbname,
			  bool hasaux,
			  ptr<chord_trigger_t> t) :
  repair_tcb (NULL),
  ctype (c),
  db (New refcounted<adb> (dbsock, dbname, hasaux, t)),
  maint (get_maint_aclnt (msock)),
  node (node),
  cli (cli),
  repair_read_bytes (0),
  repair_sent_bytes (0),
  repairs_completed (0),
  expired_repairs (0)
{
  warn << "opened " << dbsock << " with space " << dbname 
       << (hasaux ? " (hasaux)\n" : "\n");
}

dhblock_srv::~dhblock_srv ()
{
  stop ();
}

str
dhblock_srv::prefix () const
{
  const char *p = "unknown";
  switch (ctype) {
    case DHASH_CONTENTHASH:
      p = "chash";
      break;
    case DHASH_NOAUTH:
      p = "noauth";
      break;
    case DHASH_KEYHASH:
      p = "keyhash";
      break;
    case DHASH_APPEND:
      p = "append";
      break;
  }
  return p;
}      

ptr<aclnt>
dhblock_srv::get_maint_aclnt (str msock)
{
  int fd = unixsocket_connect (msock);
  if (fd < 0)
    fatal ("get_maint_aclnt: Error connecting to %s: %m\n", msock.cstr ());
  make_async (fd);
  ptr<aclnt> c = aclnt::alloc (axprt_unix::alloc (fd, 1024*1025),
      maint_program_1);
  return c;
}

void
dhblock_srv::maint_initspace (int efrags, int dfrags, ptr<chord_trigger_t> t)
{
  maint_dhashinfo_t dhi;
  node->my_location ()->fill_node (dhi.host);
  dhi.ctype = ctype;
  dhi.dbsock = db->dbsock ();
  dhi.dbname = db->name ();
  dhi.hasaux = db->hasaux ();
  dhi.efrags = efrags;
  dhi.dfrags = dfrags;
  maint_status *res = New maint_status (MAINTPROC_OK);
  maint->call (MAINTPROC_INITSPACE, &dhi, res,
      wrap (this, &dhblock_srv::maintinitcb, t, res));
}

void
dhblock_srv::maintinitcb (ptr<chord_trigger_t> t, maint_status *res, clnt_stat err)
{
  if (err || *res)
    warn << "Maintenance initialization failed for " 
      << db->name () << ": " << err << "/" << *res << "\n";
  delete res;
}

void
dhblock_srv::maint_getrepairs (int thresh, int count, chordID start,
    cb_maintrepairs_t cbr)
{
  maint_getrepairsarg arg;
  bzero (&arg, sizeof (arg));
  node->my_location ()->fill_node (arg.host);
  arg.ctype = ctype;
  arg.thresh = thresh;
  arg.count = count;
  arg.start = start;
  maint_getrepairsres *res = New maint_getrepairsres ();
  maint->call (MAINTPROC_GETREPAIRS, &arg, res,
      wrap (this, &dhblock_srv::maintgetrepairscb, res, cbr));
}

void
dhblock_srv::maintgetrepairscb (maint_getrepairsres *res,
    cb_maintrepairs_t cbr, clnt_stat err)
{
  vec<maint_repair_t> repairs;
  if (err || res->status) {
    warn << "Maintenance getrepairs failed for " 
      << db->name () << ": " << err << "/" << res->status << "\n";
  } else {
    for (size_t i = 0; i < res->repairs.size (); i++)
      repairs.push_back (res->repairs[i]);
  }
  cbr (repairs);

  delete res;
}

ptr<location>
dhblock_srv::maintloc2location (u_int32_t a, u_int32_t b)
{
  chord_node_wire x;
  bzero (&x, sizeof (x));
  x.machine_order_ipv4_addr  = a;
  x.machine_order_port_vnnum = b;
  return node->locations->lookup_or_create (make_chord_node (x));
}

// These 2 functions exist just so that we can have a generic adbcb.
// Unfortunately, we can't wrap superclass functions in subclasses.
void 
dhblock_srv::db_store (chordID k, str d, cb_dhstat cb)
{
  db->store (k, d, wrap (this, &dhblock_srv::adbcb, cb));
}
void 
dhblock_srv::db_store (chordID k, str d, u_int32_t aux, u_int32_t expire, cb_dhstat cb)
{
  db->store (k, d, aux, expire, wrap (this, &dhblock_srv::adbcb, cb));
}

void
dhblock_srv::adbcb (cb_dhstat cb, adb_status astat)
{
  switch (astat) {
  case ADB_OK:
    cb (DHASH_OK);
    break;
  case ADB_ERR:
    cb (DHASH_DBERR);
    break;
  case ADB_DISKFULL:
    cb (DHASH_DISKFULL);
    break;
  default:
    // Don't expect adb::store and adb::remove to have other return codes.
    fatal << "Unexpected adb_status in dhblock_srv::adbcb: " << astat << "\n";
  }
}

void
dhblock_srv::start (bool randomize)
{
  int delay = 0;
  if (!repair_tcb) {
    if (randomize)
      delay = random_getword () % dhash::reptm ();
    repair_tcb = delaycb (dhash::reptm () + delay,
	wrap (this, &dhblock_srv::repair_timer));
  }
}

void
dhblock_srv::stop ()
{
  if (repair_tcb) {
    // Outstanding repairs will continue
    timecb_remove (repair_tcb);
    repair_tcb = NULL;
  }
}

void
dhblock_srv::fetch (chordID k, cb_fetch cb)
{
  db->fetch (k, cb);
}

void
dhblock_srv::base_stats (vec<dstat> &s)
{
  str p = prefix ();
  s.push_back (dstat (p << ".repair_read_bytes", repair_read_bytes));
  s.push_back (dstat (p << ".repair_sent_bytes", repair_sent_bytes));
  s.push_back (dstat (p << ".repairs_completed", repairs_completed));
  s.push_back (dstat (p << ".expired_repairs", expired_repairs));
  s.push_back (dstat (p << ".repairs_queued", repairs_queued.size ()));
  s.push_back (dstat (p << ".repairs_inprogress", repairs_inprogress.size ()));
}

void
dhblock_srv::stats (vec<dstat> &s)
{
  base_stats (s);
}

//
// Repair management
// 
//
repair_job::repair_job (blockID key, ptr<location> w, u_int32_t to) :
  key (key),
  where (w),
  desc (strbuf () << key << " ->" << where->id ()),
  donecb (NULL), timeout (to)
{
}

void
repair_job::setdonecb (cbv cb, u_int32_t to)
{
  donecb = cb;
  timeout = to;
}

void
repair_job::start ()
{
  // if (to)
  //   delaycb (to, 0, XXXgiveup);
  execute ();
}

bool
dhblock_srv::repair_add (ptr<repair_job> job)
{
  if (!job)
    return false;
  if (repairs_queued[job->desc] || repairs_inprogress[job->desc]) 
    return false;
  info << node->my_ID () << ": repair_add for " << job->desc << "\n"; 
  repair_q.push_back (job);
  repairs_queued.insert (job->desc);

  repair_flush_q ();

  return true;
}

u_int32_t
dhblock_srv::repair_qlength ()
{
  return repairs_queued.size ();
}

void
dhblock_srv::repair_timer ()
{
  if (repairs_queued.size () < REPAIR_QUEUE_MAX)
    generate_repair_jobs ();
  // Generation will finish asynchronously
  // REPAIR_QUEUE_MAX is a soft cap to try and
  // keep us from generating too many repairs; subclasses
  // are free to call repair_add to enqueue jobs whenever
  // they like.

  repair_flush_q ();

  repair_tcb = delaycb (dhash::reptm (),
      wrap (this, &dhblock_srv::repair_timer));
}

void
dhblock_srv::repair_done (str desc)
{
  repairs_inprogress.remove (desc);
  repairs_completed++;
  info << "completed repair of " << desc << "; "
	<< repairs_inprogress.size () << " in progress, "
	<< repairs_queued.size () << " in queue.\n";
  if (repairs_queued.size () < REPAIR_QUEUE_MAX)
    generate_repair_jobs ();
  repair_flush_q ();
}

void
dhblock_srv::repair_flush_q ()
{
  while ((repairs_inprogress.size () < REPAIR_OUTSTANDING_MAX)
         && (repair_q.size () > 0)) {
    ptr<repair_job> job = repair_q.pop_front ();
    repairs_queued.remove (job->desc);
    assert (!repairs_inprogress[job->desc]);
    repairs_inprogress.insert (job->desc);
    job->setdonecb (wrap (this, &dhblock_srv::repair_done, job->desc));
    job->start ();
    trace << "dhblock_srv::repair_flush_q: started repair of " << job->desc 
	  << "\n";
  }
  assert (repair_q.size () == repairs_queued.size ());
}
