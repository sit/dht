/* $Id: sfsrocd.C,v 1.2 2001/03/23 06:21:34 fdabek Exp $ */

/*
 *
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
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

#include "sfsrocd.h"

#ifdef MAINTAINER
const bool sfsrocd_noverify = (getenv ("SFSROCD_NOVERIFY"));
const bool sfsrocd_nocache = (getenv ("SFSROCD_NOCACHE"));
const bool sfsrocd_cache_stat = (getenv ("SFSROCD_CACHE_STAT"));
const bool sfsrocd_prefetch = (getenv ("SFSROCD_PREFETCH"));
#endif /* !MAINTAINER */

void
show_stats (int sig)
{
  warn << "Caught signal " << sig << "\n";

  if (sfsrocd_cache_stat) {

    warn << "Cache analysis follows.\n";
  
    warn << "Inode cache:\n";
    if (cstat.ic_tot > 0) {
      warn << "Hit  "
	   << static_cast<u_int32_t>((100*cstat.ic_hit)/cstat.ic_tot) 
	   << "% (" << cstat.ic_hit << " hits)\n";
      warn << "Miss "
	   << static_cast<u_int32_t>((100*cstat.ic_miss)/cstat.ic_tot) 
	   << "% (" << cstat.ic_miss << " misses)\n";
      warn << "Total " << cstat.ic_tot << " requests\n\n";
    } else {
      warn << "No inodes requested\n\n";
    }

    warn << "Directory cache:\n";
    if (cstat.dc_tot > 0) {
      warn << "Hit  "
	   << static_cast<u_int32_t>((100*cstat.dc_hit)/cstat.dc_tot) 
	   << "% (" << cstat.dc_hit << " hits)\n";
      warn << "Miss "
	   << static_cast<u_int32_t>((100*cstat.dc_miss)/cstat.dc_tot) 
	   << "% (" << cstat.dc_miss << " misses)\n";
      warn << "Total " << cstat.dc_tot << " requests\n\n";
    } else {
      warn << "No directory blocks requested\n\n";
    }
      

    warn << "Indirect block cache:\n";
    if (cstat.ibc_tot > 0) {
      warn << "Hit  "
	   << static_cast<u_int32_t>((100*cstat.ibc_hit)/cstat.ibc_tot) 
	   << "% (" << cstat.ibc_hit << " hits)\n";
      warn << "Miss "
	   << static_cast<u_int32_t>((100*cstat.ibc_miss)/cstat.ibc_tot) 
	   << "% (" << cstat.ibc_miss << " misses)\n";
      warn << "Total " << cstat.ibc_tot << " requests\n\n";
    } else {
      warn << "No indirect blocks requested\n\n";
    }

    warn << "File data block cache:\n";
    if (cstat.bc_tot > 0) {
      warn << "Hit  "
	   << static_cast<u_int32_t>((100*cstat.bc_hit)/cstat.bc_tot) 
	   << "% (" << cstat.bc_hit << " hits)\n";
      warn << "Miss "
	   << static_cast<u_int32_t>((100*cstat.bc_miss)/cstat.bc_tot)
	   << "% (" << cstat.bc_miss << " misses)\n";
      warn << "Total " << cstat.bc_tot << " requests\n\n";
    } else {
      warn << "No file data blocks requested\n\n";
    }
  }

  exit(0);
}

void
cpu_time ()

{
  struct rusage ru;
  double res = 0;
  
  if (getrusage (RUSAGE_SELF, &ru) != 0) {
    warnx << "Getrusage: self failed\n";
    exit (1);
  }

  res += (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1e6;
  res += ru.ru_utime.tv_usec + ru.ru_stime.tv_usec;
    

  if (getrusage (RUSAGE_CHILDREN, &ru) != 0) {
    warnx << "Getrusage: child failed\n";
    exit (1);
  }

  res += (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1e6;
  res += ru.ru_utime.tv_usec + ru.ru_stime.tv_usec;

  printf ("CPU time: %f\n", res);

  sigcb (SIGUSR1, wrap (cpu_time));

}

int
main (int argc, char **argv)
{
  #ifdef MAINTAINER
  if (sfsrocd_noverify) 
    warn << "SFSROCD_NOVERIFY\n";
  if (sfsrocd_nocache)
    warn << "SFSROCD_NOCACHE\n";
  if (sfsrocd_cache_stat) 
    warn << "SFSROCD_CACHE_STAT\n";
  #endif /* !MAINTAINER */

  setprogname (argv[0]);
  warn ("version %s, pid %d\n", VERSION, getpid ());

  if (argc != 1)
    fatal ("usage: %s\n", progname.cstr ());

  sfsconst_init ();
  random_init_file (sfsdir << "/random_seed");
  // server::keygen ();

  if (ptr<axprt_unix> x = axprt_unix_stdin ())
    vNew sfsprog (x, &sfsserver_alloc<server>);
  else
    fatal ("could not get connection to sfscd.\n");


#ifdef MAINTAINER
  sigcb (SIGUSR1, wrap (cpu_time));
#endif

  amain ();
}


