/* $Id: playtrace.C,v 1.1 2001/06/21 19:03:13 fdabek Exp $ */

/*
 *
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
 * Copyright (C) 2000 Kevin Fu (fubob@mit.edu)
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

#include "arpc.h"
#include "sfsmisc.h"
#include "sfsro_prot.h"
#include "parseopt.h"

#if 0

char trace_one_block[][20] = {
#include "one_block.h"
};
size_t trace_size_one_block = sizeof (trace_one_block) 
  / sizeof (trace_one_block[0]);

char trace_small_files[][20] = {
#include "small_files1.h"
};
size_t trace_size_small_files = sizeof (trace_small_files) 
  / sizeof (trace_small_files[0]);

#endif

char trace_single_file[][20] = {
#include "single_file_trace.h"
};
size_t trace_size_single_file = sizeof (trace_single_file) 
  / sizeof (trace_single_file[0]);

void finish ();

struct tracevec {
  const char *name;
  char (*trace)[20];
  size_t trace_size; 
};

const tracevec traces[]  = {

};
size_t tracevec_size = 0;


struct player {
  static u_int64_t bytecount;
  static unsigned long playcount;
  static unsigned long playcount_attempted;

  const in_addr srvaddr;
  const aclnt_cb mycb;

  unsigned reqno;
  ptr<aclnt> cs;
  ptr<aclnt> cr;
  sfs_connectres cres;
  sfs_fsinfo fsires;
  sfsro_datares res;

  player (in_addr a)
    : srvaddr (a), mycb (wrap (this, &player::nextreq)), reqno (0)
    { start (); }
  void start ();
  void getfd (int fd);
  void conres (clnt_stat stat);
  void fsinfores (clnt_stat stat);
  void nextreq (clnt_stat stat);
};

u_int64_t player::bytecount;
unsigned long player::playcount;
unsigned long player::playcount_attempted;

bool opt_verbose = false;
int opt_trace = -1;
char *opt_name = NULL;
int opt_single = 0;

void
player::start ()
{
  cr = cs = NULL;
  reqno = 0;
  playcount_attempted++;

  res.set_status (SFSRO_OK);
  res.resok->data.setsize (0);
  tcpconnect (srvaddr, sfs_port, wrap (this, &player::getfd));
}

void
player::getfd (int fd)
{
  if (fd < 0) {
    warn ("tcpconnect: %m\n");	// XXX
    timecb (timenow + 1, wrap (this, &player::start));
    return;
  }
  ref<axprt> x (axprt_stream::alloc (fd));
  cs = aclnt::alloc (x, sfs_program_1);
  cr = aclnt::alloc (x, sfsro_program_1);

  sfs_connectarg carg;
  carg.release = sfs_release;
  carg.service = SFS_SFS;
  carg.name = "";
  cs->call (SFSPROC_CONNECT, &carg, &cres, wrap (this, &player::conres));
}

void
player::conres (clnt_stat stat)
{
  if (stat) {
    warn << stat;
    start ();
    return;
  }

  bytecount += 
    76 +
    (cres.reply->servinfo.host.hostname.len () + 3 & ~3) +
    (mpz_rawsize (&cres.reply->servinfo.host.pubkey) + 3 & ~3);
  /* Account for overhead */
  
  cs->call (SFSPROC_GETFSINFO, NULL, &fsires, 
	    wrap (this, &player::fsinfores));
}

void
player::fsinfores (clnt_stat stat)
{
  if (stat) {
    warn << stat;
    start ();
    return;
  }

  bytecount += 
    108 +
    (mpz_rawsize (&fsires.sfsro->v1->sig) + 3 & ~3);
  /* Account for overhead */
  
  cr->call (SFSROPROC_GETDATA, traces[opt_trace].trace[reqno++], &res, mycb);
}

void
player::nextreq (clnt_stat stat)
{
  if (stat) {
    warn << stat << "\n";
    start ();
    return;
  }
  if (!res.status)
    bytecount += res.resok->data.size () + 32;  /* Account for overhead */
  else
    warn ("ENOENT\n");
  if (reqno >= traces[opt_trace].trace_size) {
    playcount++;
    if (!opt_single) start ();
    else finish();
  } else
    cr->call (SFSROPROC_GETDATA, traces[opt_trace].trace[reqno++], &res, mycb);
}

inline u_int64_t
gettime ()
{
  timeval tv;
  gettimeofday (&tv, NULL);
  return tv.tv_sec * INT64(1000000) + tv.tv_usec;
}

u_int64_t starttime;

void
finish ()
{
  u_int64_t elapsed_usec = gettime () - starttime;
  if (opt_verbose) {
    printf ("Bytes: %qd\n", player::bytecount);
    printf (" usec: %qd\n", elapsed_usec);
    printf (" MB/s: %f\n", 0.953674316406 * player::bytecount / elapsed_usec);
    printf ("Successful plays: %lu\n", player::playcount);
    printf ("Attempted plays: %lu\n", player::playcount_attempted);
    printf ("Successful plays/s: %f\n", 
	    player::playcount / (elapsed_usec / 1e6F));
    printf ("Attempted plays/s: %f\n\n", 
	    player::playcount_attempted / (elapsed_usec/ 1e6F));
    printf ("Test: %s\n", traces[opt_trace].name);
    printf ("RPCs per play: %d\n", traces[opt_trace].trace_size);

  } else {

    printf ("%5s %10qd %10qd %10f %6lu %6lu %8.2f %8.2f\n",
	    opt_name?opt_name:"Local",
	    player::bytecount, 
	    elapsed_usec,
	    0.953674316406 * player::bytecount / elapsed_usec,
	    player::playcount,
	    player::playcount_attempted,
	    player::playcount / (elapsed_usec / 1e6F),
	    player::playcount_attempted / (elapsed_usec/ 1e6F));
  }

  exit (0);
}

void
usage ()
{

  warnx << "Trace numbers are:\n\n";

  for (unsigned int i = 0; i < tracevec_size; i++) 
    warnx << i << ": " << traces[i].name << "\n";
 

  fatal << "\nusage: " << progname << " [-p trigger-port] [-n num-clients]\n"
	<< "            [-t seconds] [-l label] [-v] -r trace-num server\n\n"
	<< "Remember to set SFS_PORT\n";
}


int
main (int argc, char **argv)
{
  setprogname (argv[0]);
  sfsconst_init ();



  int opt_triggerport = 0;
  int opt_nclients = 20;
  double opt_nseconds = 5.0;

  int ch;
  while ((ch = getopt (argc, argv, "sp:n:t:r:l:v")) != -1)
    switch (ch) {
    case 's':
      opt_single = 1;
      break;
    case 'p':
      if (!convertint (optarg, &opt_triggerport))
	usage ();
      break;
    case 'n':
      if (!convertint (optarg, &opt_nclients))
	usage ();
      break;
    case 't':
      opt_nseconds = atof (optarg);
      if (opt_nseconds <= 0)
	usage ();
      break;
    case 'r':
      if (!convertint (optarg, &opt_trace))
	usage ();
      break;
    case 'v':
      opt_verbose = true;
      break;
    case 'l':
      opt_name = optarg;
      break;
    default:
      usage ();
      break;
    }

  if (  (opt_trace < 0) 
      || (opt_trace >= (int)tracevec_size))
    usage ();


  itimerval itv;
  bzero (&itv, sizeof (itv));
  itv.it_value.tv_sec = long (opt_nseconds);
  itv.it_value.tv_usec = long (1000000 * (opt_nseconds - itv.it_value.tv_sec));
  sigcb (SIGALRM, wrap (finish));


  if (opt_verbose) {
    printf ("Clients     Est. Duration (usec)    Bytes   Act. Duration (usec)   MB/s   Succ. Plays   Attem. Plays  Succ. Plays/s   Attem. Plays/s\n");
 
    printf ("[ %d clients / %f seconds ]\n", opt_nclients, opt_nseconds);
  }

  if (opt_triggerport) {
    int fd = inetsocket (SOCK_DGRAM, opt_triggerport);
    if (fd < 0)
      fatal ("socket: %m\n");
    char c;
    socklen_t l = 0;
    recvfrom (fd, &c, 1, 0, NULL, &l);
  }
  
  if (setitimer (ITIMER_REAL, &itv, NULL) < 0)
    fatal ("setitimer: %m\n");

  starttime = gettime ();

  for (int i = 0; i < opt_nclients; i++) {
    int cli = optind + (i % (argc - optind) );
    warn << "starting client for " << argv[cli] << "\n";
    hostent *hp = gethostbyname (argv[cli]);
    if (!hp)
      fatal << argv[cli] << ": no such host\n";
    in_addr addr = *(in_addr *) hp->h_addr;
    vNew player (addr);
  }

  amain ();

}


