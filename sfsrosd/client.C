//Last modified by $Author: fdabek $ on $Date: 2001/03/22 16:41:19 $
#include <sfsrosd.h>
#include "sfsdb.h"
#include "rxx.h"
#include "vec.h"
#include "qhash.h"
#include "sfsrodb_core.h"

#ifndef MAINTAINER
enum { dumptrace = 1 };
#else /* MAINTAINER */
const bool dumptrace (getenv ("SFSRO_TRACE"));
#endif /* MAINTAINER */

sfsrodb db;
sfs_connectres cres;
sfs_fsinfo fsinfores;

#ifdef STATS
int proxy_requests;
int proxy_misses;
#endif /* #ifdef STATS */

sfsroclient::sfsroclient (ptr<axprt_stream> _x, const authunix_parms *aup)
  : x (_x), destroyed (new refcounted<bool> (false))
{
  xdrsuio xxx (XDR_ENCODE);

  if ((unixauth = aup))
    uid = aup->aup_uid;
  
  rosrv = asrv::alloc (x, sfsro_program_1,
		       wrap (this, &sfsroclient::dispatch, destroyed));
  sfssrv = asrv::alloc (x, sfs_program_1,
			wrap (this, &sfsroclient::dispatch, destroyed));
  authid_valid = false;

#ifdef STATS
  proxy_requests = 0;
  proxy_misses = 0;
#endif
}

sfsroclient::~sfsroclient()
{
  *destroyed = true;

#ifdef STATS
  warn << "total proxy requests: " << proxy_requests << "\n";
  warn << "proxy misses: " << proxy_misses << "\n";
#endif
}

void
sfsroclient::dispatch (ref<bool> d, svccb *sbp)
{
  if (!sbp) {
    if (*d)
      return;
    delete this;
    return;
  }

  if (sbp->prog () == SFS_PROGRAM) {
    switch (sbp->proc ()) {
    case SFSPROC_NULL:
      sbp->reply (NULL);
      return;
    case SFSPROC_CONNECT:
      {
	//	warn << "connect\n";
	sbp->reply (&cres);
	return;
      }
    case SFSPROC_GETFSINFO:
      {
	//	warn << "fsinfo request\n";
	//	warn << "root fh is " << hexdump(&fsinfores.sfsro->v1->info.rootfh, 20) << "\n";
	sbp->reply(&fsinfores);
	return;
      }
    default:
      sbp->reject (PROC_UNAVAIL);
      return;
    }
  }
  else {
    switch (sbp->proc ()) {
    case SFSROPROC_NULL:
      sbp->reply (NULL);
      return;
    case SFSROPROC_GETDATA:
      {

	sfs_hash *fh = sbp->template getarg<sfs_hash> ();


	if (1) {
	  u_char *cp = reinterpret_cast<u_char *> (fh->base ());
	  u_char *lim = cp + fh->size ();
	  printf ("  { 0x%02x", *cp);
	  while (++cp < lim)
	    printf (", 0x%02x", *cp);
	  printf (" },\n");
	}

	sfsro_datares *res = New sfsro_datares();
        db.getdata(fh, res, wrap(this, &sfsroclient::getdata_cb, sbp, res, destroyed));
        return;
      }

    default:
      sbp->reject (PROC_UNAVAIL);
      break;
    }
  }
}

void
sfsroclient::getdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d) 
{
  if (*d)
    return;
  
  if (res->resok->data.size () == 0) 
    warn << "key not found\n";

  sbp->reply(res);
  delete (res);
  return;
}


