#include <sfsrosd.h>
#include "sfsdb.h"
#include "rxx.h"
#include "vec.h"
#include "qhash.h"

#ifndef MAINTAINER
enum { dumptrace = 0 };
#else /* MAINTAINER */
const bool dumptrace (getenv ("SFSRO_TRACE"));
#endif /* MAINTAINER */

sfsrodb db;
sfs_connectres cres;
sfs_fsinfo fsinfores;
extern vec<sfsro_mirrorarg> mirrors;
//qhash<sfs_hash, ref<sfsro_datares> > cache;


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

}

void
sfsroclient::updatemirrorinfo() {
  
  //FED - update fsinfo
  fsinfores.sfsro->v1->mirrors.set (mirrors.base(), mirrors.size (), freemode::NOFREE); 
}

void
sfsroclient::addmirror(sfsro_mirrorarg *mirror) {

  //FED - update global mirror list
  mirrors.push_back(*mirror);
  warn << "added " << mirror->host << "as a mirror. It is the " << mirrors.size () << "\n";
}

sfsroclient::~sfsroclient()
{
  *destroyed = true;
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
	sbp->reply (&cres);
	return;
      }
    case SFSPROC_GETFSINFO:
      {
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
#if 0
	if (dumptrace) {
	  u_char *cp = reinterpret_cast<u_char *> (fh->base ());
	  u_char *lim = cp + fh->size ();
	  printf ("  { 0x%02x", *cp);
	  while (++cp < lim)
	    printf (", 0x%02x", *cp);
	  printf (" },\n");
	}
#endif
	sfsro_datares *res = New sfsro_datares();
        db.getdata(fh, res, wrap(this, &sfsroclient::getdata_cb, sbp, res, destroyed));
        return;
      }

    case SFSROPROC_PROXYGETDATA:
      {
	warn << "got a proxy data request\n";
	sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();
	
	struct in_addr pubaddr;
	pubaddr.s_addr = arg->pub_addr;
	char buf[100];
	warn << "I think that the publisher is.... ";
	warn << inet_ntop(AF_INET, &pubaddr, buf, 100) << "\n";
	//this is where I would check the cache
	//right here

	sfsro_datares *res = New sfsro_datares();
	ref<sfs_hash> fh = new refcounted<sfs_hash>(arg->fh);
	db.getdata(fh, res, wrap(this, &sfsroclient::proxygetdata_cb, sbp, res, destroyed));
	return;
	  
      }
    case SFSROPROC_GETDATA_PARTIAL:
      {
	
	sfsro_partialgetarg *arg = sbp->template getarg<sfsro_partialgetarg> ();

	
	if (dumptrace) {
	  u_char *offset = reinterpret_cast<u_char *> (&arg->offset);
	  u_char *len = reinterpret_cast<u_char *> (&arg->len);

	  u_char *cp = reinterpret_cast<u_char *> (arg->key.base ());
	  u_char *lim = cp + arg->key.size();
	  printf ("  { 0x%02x", *offset);
	  for (int ix=1; ix < 4; ix++)
	    printf (", 0x%02x", offset[ix]);
	  for (int ix=0; ix < 4; ix++)     
            printf (", 0x%02x", len[ix]);
	  printf (", 0x%02x", *cp);
	  while (++cp < lim)
	    printf (", 0x%02x", *cp);
	  printf (" },\n");
	}

	//warn << "got a partial get request for " << arg->offset << " to " << arg->len+arg->offset << "\n";
	
	sfsro_datares *res = New sfsro_datares();
        db.getpartialdata(&(arg->key), res, 
			  wrap(this, &sfsroclient::getdata_cb, 
			       sbp, res, destroyed),
			  arg->offset, arg->len);
        return;
      }
    case SFSROPROC_ADDMIRROR:
      {
	sfsro_mirrorarg *arg = sbp->template getarg<sfsro_mirrorarg> ();
	warn << "got add_mirror message for " << arg->host << "\n";
	addmirror(arg);
	updatemirrorinfo();
	return;
      }
    default:
      sbp->reject (PROC_UNAVAIL);
      break;
    }
  }
}

void
sfsroclient::getdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d) {
  if (*d)
    return;
  sbp->reply(res);
  delete (res);
  return;
}

void
sfsroclient::proxygetdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d) {
  if (*d)
    return;

  if (res->status == SFSRO_OK) {
    sbp->reply(res);
    delete (res);
    return;
  } else {
    warn << "cache miss, contacting publisher\n";
    //contact the publisher
    sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();
    struct in_addr pubaddr;
    pubaddr.s_addr = arg->pub_addr;
    tcpconnect(pubaddr, arg->pub_port, 
	       wrap(this, &sfsroclient::proxygetdata_cb_connect, sbp, res));
    return;
  }
  
}

void
sfsroclient::proxygetdata_cb_connect(svccb *sbp, sfsro_datares *res , int fd) {

  ptr<axprt_stream> x = axprt_stream::alloc (fd);
  ptr<aclnt> sfsroc = aclnt::alloc (x, sfsro_program_1);

  sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();
  ref<sfs_hash> fh = New refcounted<sfs_hash>(arg->fh);
  sfsroc->call (SFSROPROC_GETDATA, fh, res, wrap(this,
						 &sfsroclient::proxygetdata_cb_call, sbp, res));
}

void
sfsroclient::proxygetdata_cb_call(svccb *sbp, sfsro_datares *res, clnt_stat err) {

  if (err) {
    fatal << "Error contacting publisher\n";
  }
  
  sbp->reply(res);
}


