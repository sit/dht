#include <misc_utils.h>
#include <configurator.h>
#include <qhash.h>

#include "chord.h"
#include <sys/types.h>

#include "cd_prot.h"

#include <location.h>
#include <locationtable.h>

#include <debruijn.h>
#include <fingerroute.h>
#include <fingerroutepns.h>
#include <recroute.h>

#include <modlogger.h>
#define info modlogger ("cd")

static char *logfname;

ptr<chord> chordnode;
static str ctlsocket;

static qhash<chordID, ref<vnode>, hashID> vnodes;

struct routing_mode_desc {
  cd_routing_mode m;
  vnode_producer_t producer;
};					       

/* List of routing modes. */
routing_mode_desc modes[] = {
  { MODE_SUCC, wrap (vnode::produce_vnode) },
  { MODE_CHORD, wrap (fingerroute::produce_vnode) },
  { MODE_DEBRUIJN, wrap (debruijn::produce_vnode) },
  { MODE_PNS, wrap (fingerroutepns::produce_vnode) },
  { MODE_PNSREC, wrap (recroute<fingerroutepns>::produce_vnode) },
  { MODE_CHORDREC, wrap (recroute<fingerroute>::produce_vnode) },
  { MODE_TCPPNSREC, wrap (recroute<fingerroutepns>::produce_vnode) },
};

int nmodes = sizeof (modes)/sizeof(modes[0]);

void stats ();
void stop ();
void halt ();

// =====================================

static void
lookup_cb (svccb *sbp, vec<chord_node> nodes, route r, chordstat stat)
{
  cd_lookup_res *res = New cd_lookup_res ();
  res->set_stat(stat);

  if (stat == CHORD_OK) {
    chord_node_wire cnw;
    while (!r.empty()) {
      r.pop_front()->fill_node(cnw);
      res->resok->route.push_back(cnw);
    }
  }

  sbp->reply(res);
}

void
cd_dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    // Close the server
    s->setcb (NULL);
	// Since cd is meant for one-time use, now that my one connection
	// has shutdown, shutdown the server too
	halt();
    return;
  }
  info << "received cd " << sbp->proc () << "\n";

  switch (sbp->proc ()) {
  case CD_NULL:
    sbp->reply (NULL);
    break;

  case CD_EXIT:
    sbp->reply (NULL);
    halt ();
    break;

  case CD_NEWCHORD:
    {
      cd_newchord_arg *a = sbp->Xtmpl getarg<cd_newchord_arg> ();
      chord_hostname myname = a->myname;
	  chord_hostname wellknownhost = a->wellknownhost;
      if (myname.len() == 0) {
        myname = my_addr();
      }
      if (wellknownhost.len() == 0) {
        wellknownhost = my_addr();
      }

      cd_newchord_res *res = New cd_newchord_res ();
      if (chordnode) {
        res->set_stat(CHORD_NOTINRANGE);
      } else {
        // Find routing mode
        int i;
        bool success = false;
        for (i = 0; i < nmodes; i++) {
          if (a->routing_mode == modes[i].m) {
            success = true;
            break;
          }
        }
        
        if (!success) {
          res->set_stat(CHORD_NOTINRANGE);
        } else {
          chordnode = New refcounted<chord> (myname,
                                             a->myport,
                                             modes[i].producer,
                                             a->nvnodes,
                                             a->maxcache);
          chordnode->startchord();
          chordnode->join(wellknownhost, a->wellknownport, false);
          res->set_stat(CHORD_OK);
          res->resok->nvnodes = chordnode->num_vnodes();
          for (int j = 0; j < res->resok->nvnodes; j++) {
            ptr<vnode> vn = chordnode->get_vnode(j);
            chordID id = vn->my_ID();
            vnodes.insert(id, vn);
            res->resok->vnodes.push_back(id);
          }
        }
      }
      sbp->reply(res);
    }
    break;

  case CD_UNNEWCHORD:
    {
      cd_newchord_res *res = New cd_newchord_res ();
      if (chordnode) {
        chordnode = NULL;
        res->set_stat(CHORD_OK);
      } else {
        res->set_stat(CHORD_NOTINRANGE);
      }
      sbp->reply(res);
    }
    break;
    
  case CD_LOOKUP:
    {
      cd_lookup_arg *a = sbp->Xtmpl getarg<cd_lookup_arg> ();
      ptr<vnode> vn = vnodes[a->vnode];

      if (vn == NULL) {
        cd_lookup_res *res = New cd_lookup_res ();
        res->set_stat(CHORD_NOTINRANGE);
        sbp->reply(res);
      } else {
        vn->find_successor(a->key, wrap(lookup_cb, sbp));
      }
    }
    break;

  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

// =====================================

void
control_accept (ref<axprt_stream> x)
{
  ptr<asrv> srv;
  srv = asrv::alloc (x, cd_program_1, NULL);
  srv->setcb (wrap (&cd_dispatch, srv));
}

static void
startcontroller ()
{
  // XXX Are there any security issues here?
  int fd = unixsocket_connect(ctlsocket);
  if (fd < 0)
    fatal << "Error opening control socket: " << strerror(errno) << "\n";
  //unlink(ctlsocket);
  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);
  control_accept(x);
}

void 
clear_stats (const rpc_program &prog)
{
#ifdef RPC_PROGRAM_STATS
  bzero (prog.outcall_num, sizeof (prog.outcall_num));
  bzero (prog.outcall_bytes, sizeof (prog.outcall_bytes));
  bzero (prog.outcall_numrex, sizeof (prog.outcall_numrex));
  bzero (prog.outcall_bytesrex, sizeof (prog.outcall_bytesrex));
  bzero (prog.outreply_num, sizeof (prog.outreply_num));
  bzero (prog.outreply_bytes, sizeof (prog.outreply_bytes));
#endif
}

void
dump_rpcstats (const rpc_program &prog, bool first, bool last)
{
  warn << "dump_rpcstats: " << (u_int)&prog << "\n";

  // In arpc/rpctypes.h -- if defined
#ifdef RPC_PROGRAM_STATS
  static rpc_program total;

  str fmt1 ("%-40s %15s %15s %15s %15s %15s %15s\n");
  str fmt2 ("%-40s %15d %15d %15d %15d %15d %15d\n");

  if (first) {
    bzero (&total, sizeof (total));
    warn.fmt (fmt1,
	      "",
	      "outcall_num","outcall_bytes",
	      "outcall_numrex","outcall_bytesrex",
	      "outreply_num","outreply_bytes");
  }

  rpc_program subtotal;
  bzero (&subtotal, sizeof (subtotal));
  for (size_t procno = 0; procno < prog.nproc; procno++) {
    if (strlen (prog.tbl[procno].name) == 1)
      continue;

    warn.fmt (fmt2,
	      prog.tbl[procno].name,
	      prog.outcall_num[procno],
	      prog.outcall_bytes[procno],
	      prog.outcall_numrex[procno],
	      prog.outcall_bytesrex[procno],
	      prog.outreply_num[procno],
	      prog.outreply_bytes[procno]);
    
    subtotal.outcall_num[0] += prog.outcall_num[procno];
    subtotal.outcall_bytes[0] += prog.outcall_bytes[procno];
    subtotal.outcall_numrex[0] += prog.outcall_numrex[procno];
    subtotal.outcall_bytesrex[0] += prog.outcall_bytesrex[procno];
    subtotal.outreply_num[0] += prog.outreply_num[procno];
    subtotal.outreply_bytes[0] += prog.outreply_bytes[procno];
  }
  
  str tmp = strbuf () << "SUMMARY " << prog.name;
  warn.fmt (fmt2,
	    tmp.cstr (),
	    subtotal.outcall_num[0],
	    subtotal.outcall_bytes[0],
	    subtotal.outcall_numrex[0],
	    subtotal.outcall_bytesrex[0],
	    subtotal.outreply_num[0],
	    subtotal.outreply_bytes[0]);

  warn << "TOTAL " << prog.name << "  out*_num "
       << subtotal.outcall_num[0]
          + subtotal.outcall_numrex[0] + subtotal.outreply_num[0]
       << " out*_bytes " 
       << subtotal.outcall_bytes[0]
          + subtotal.outcall_bytesrex[0] + subtotal.outreply_bytes[0]
       << "\n";

  warn << "\n";

  total.outcall_num[0] += subtotal.outcall_num[0];
  total.outcall_bytes[0] += subtotal.outcall_bytes[0];
  total.outcall_numrex[0] += subtotal.outcall_numrex[0];
  total.outcall_bytesrex[0] += subtotal.outcall_bytesrex[0];
  total.outreply_num[0] += subtotal.outreply_num[0];
  total.outreply_bytes[0] += subtotal.outreply_bytes[0];
  
  if (last) {
    warn.fmt (fmt2,
	      "SUMMARY all protocols",
	      total.outcall_num[0],
	      total.outcall_bytes[0],
	      total.outcall_numrex[0],
	      total.outcall_bytesrex[0],
	      total.outreply_num[0],
	      total.outreply_bytes[0]);

    warn << "TOTAL all protocols      out*_num " 
	 << total.outcall_num[0]
	    + total.outcall_numrex[0] + total.outreply_num[0]
	 << " out*_bytes " 
	 << total.outcall_bytes[0]
	    + total.outcall_bytesrex[0] + total.outreply_bytes[0]
	 << "\n";
  }

#endif
}



void
bandwidth ()
{
  warn << gettime () << " bandwidth\n";

  static bool first_call = true;
  extern const rpc_program chord_program_1;
  extern const rpc_program cd_program_1;

  if (!first_call) {
    // don't dump on the first call, because stats
    // have not been cleared yet.
    dump_rpcstats (chord_program_1, true, false);
    dump_rpcstats (cd_program_1, false, true);
  }

  clear_stats (chord_program_1);
  clear_stats (cd_program_1);

  warn << gettime () << " bandwidth delaycb\n";
  delaycb (1, 0, wrap (bandwidth));
  first_call = false;
}


void
stats () 
{
  warn << "STATS:\n";
  //      bandwidth ();
  
  chordnode->stats ();
  strbuf x;
  chordnode->print (x);
  warnx << x;
}

void
stop ()
{
  chordnode->stop ();
}

void
halt ()
{
  warnx << "Exiting on command.\n";
  info << "stopping.\n";
  chordnode = NULL;
  exit (0);
}

static void
usage ()
{
  warnx << "Usage: " << progname 
	<< " -C <control socket> "
    "[-O <conf file>] "
    "[-b logbase] "
    "[-s <server select mode>] "
    "[-L <warn/fatal/panic output file name>] "
    "[-T <trace file name (aka new log)>] "
    "[-t]"
    "\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);
  mp_clearscrub ();
  // sfsconst_init ();
  random_init ();
  sigcb(SIGUSR1, wrap (&stats));
  sigcb(SIGUSR2, wrap (&stop));
  sigcb(SIGHUP, wrap (&halt));
  sigcb(SIGINT, wrap (&halt));

  int ch;
  int ss_mode = -1;
  int lbase = 1;

  ctlsocket = "";
  logfname = "cd-trace.log";

  char *cffile = NULL;

  while ((ch = getopt (argc, argv, "b:C:L:O:p:s:T:t"))!=-1)
    switch (ch) {
    case 'b':
      lbase = atoi (optarg);
      break;
    case 'C':
      ctlsocket = optarg;
      break;
    case 'L':
      {
	int logfd = open (optarg, O_RDWR | O_CREAT, 0666);
	if (logfd <= 0)
	  fatal << "Could not open logfile " << optarg << " for appending\n";
	lseek (logfd, 0, SEEK_END);
	errfd = logfd;
	break;
      }
    case 'O':
      cffile = optarg;
      break;
    case 's':
      ss_mode = atoi(optarg);
      break;
    case 't':
      modlogger::setmaxprio (modlogger::TRACE);
      break;
    case 'T':
      logfname = optarg;
      break;
    default:
      usage ();
      break;
    }

  if (ctlsocket.len() == 0)
  {
    fatal << "Must specify -C\n";
  }
  
  {
    int logfd = open (logfname, O_WRONLY|O_APPEND|O_CREAT, 0666);
    if (logfd < 0)
      fatal << "Couldn't open " << optarg << " for append.\n";
    modlogger::setlogfd (logfd);
  }

  if (cffile) {
    bool ok = Configurator::only ().parse (cffile);
    assert (ok);
  }
  
  if (ss_mode >= 0) {
    if (ss_mode & 1) {
      fatal << "DHash ordered successors not supported by cd.\n";
    }
    Configurator::only ().set_int ("chord.greedy_lookup",
				   ((ss_mode & 2) ? 1 : 0));
    Configurator::only ().set_int ("chord.find_succlist_shaving",
				   ((ss_mode & 4) ? 1 : 0));
  }

  // XXX The following should be made an option to NEWVNODE
  if (lbase != 1) {
    Configurator::only ().set_int ("debruijn.logbase", lbase);
  }

  Configurator::only ().dump ();
  
  {
    strbuf x = strbuf ("starting: ");
    for (int i = 0; i < argc; i++) { x << argv[i] << " "; }
    x << "\n";
    info << x;
  }

  time_t now = time (NULL);
  warn << "cd starting up at " << ctime ((const time_t *)&now);
  warn << " running with options: \n";
  warn << "  control socket: " << ctlsocket << "\n";
  warn << "  ss mode: " << ss_mode << "\n";

  startcontroller ();
  
  info << "starting amain.\n";

  amain ();
}

// This is needed to instantiate recursive routing classes.
#include <recroute.C>
template class recroute<fingerroutepns>;
template class recroute<fingerroute>;
