//Last modified by $Author: fdabek $ on $Date: 2001/02/05 17:31:29 $
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

void
sfsroclient::updatemirrorinfo() 
{
  
  //FED - update fsinfo
  fsinfores.sfsro->v1->mirrors.set (mirrors.base(), mirrors.size (), freemode::NOFREE); 
}

void
sfsroclient::addmirror(sfsro_mirrorarg *mirror) 
{

  //FED - update global mirror list
  mirrors.push_back(*mirror);
  warn << "added " << mirror->host << "as a mirror. It is the " << mirrors.size () << "\n";
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
	sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();

	struct in_addr pubaddr;
	pubaddr.s_addr = arg->pub_addr;

	sfsro_datares *res = New sfsro_datares();
	ref<sfs_hash> fh = new refcounted<sfs_hash>(arg->fh);
	db.getdata(fh, res, wrap(this, &sfsroclient::proxygetdata_cb, sbp, res, destroyed));
	return;
	  
      }

    case SFSROPROC_PROXYGETDATA_PARTIAL:
      {
	sfsro_partialproxygetarg *arg = sbp->template getarg<sfsro_partialproxygetarg> ();
	
	struct in_addr pubaddr;
	pubaddr.s_addr = arg->pub_addr;

	sfsro_datares *res = New sfsro_datares();
	ref<sfs_hash> fh = new refcounted<sfs_hash>(arg->fh);
	db.getpartialdata(fh, res, 
			  wrap(this, &sfsroclient::partialproxygetdata_cb, sbp, res, destroyed),
			  arg->offset, arg->len);
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
sfsroclient::remote_get_data(sfs_hash fh,
			     struct in_addr *pub_addr, 
			     short pub_port,
			     sfsro_datares *res,
			     callback<void, sfsro_datares *>::ref cb) 
{
  tcpconnect(*pub_addr, pub_port, 
	     wrap(this, &sfsroclient::remoteget_cb, fh, cb, res));
};

void
sfsroclient::remoteget_cb(sfs_hash fh,
			  callback<void, sfsro_datares *>::ref cb,
			  sfsro_datares *res,
			  int fd) 
{
  
  ptr<axprt_stream> x = axprt_stream::alloc (fd);
  ptr<aclnt> sfsroc = aclnt::alloc (x, sfsro_program_1);
  
  ref<sfs_hash> rfh = New refcounted<sfs_hash>(fh);
  sfsroc->call (SFSROPROC_GETDATA, rfh, res, 
		wrap(this, &sfsroclient::remotegetdata_cb_call, fh, cb, res));
}

void
sfsroclient::remotegetdata_cb_call(sfs_hash fh, 
		      callback<void, sfsro_datares *>::ref cb,
		      sfsro_datares *res,
		      clnt_stat err) 
{

  if (err) {
    warn << "error fetching data remotely\n";
    (*cb)(NULL);
    return;
  }

  (*cb)(res);
}

void
sfsroclient::getdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d) 
{
  if (*d)
    return;
  sbp->reply(res);
  delete (res);
  return;
}

void
sfsroclient::proxygetdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d) 
{
  if (*d)
    return;

#ifdef STATS
  proxy_requests++;
#endif

  if (res->status == SFSRO_OK) {
    sbp->reply(res);
    delete (res);
    return;
  } else {
#ifdef STATS
    proxy_misses++;
#endif
    sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();
    struct in_addr pubaddr;
    pubaddr.s_addr = arg->pub_addr;
    remote_get_data(arg->fh, &pubaddr, arg->pub_port, res,
		    wrap(this, &sfsroclient::proxygetdata_cb_call, sbp));
    return;
  }
  
}

void
sfsroclient::proxygetdata_cb_call(svccb *sbp, sfsro_datares *res) 
{

  if (res == NULL) {
    sbp->reply(NULL);
    return;
  }
  
  sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();
  ref<sfs_hash> fh = New refcounted<sfs_hash>(arg->fh);
  db.putdata(fh, res);

  sbp->reply(res);
}

void
sfsroclient::partialproxygetdata_cb(svccb *sbp, sfsro_datares *res, ref<bool> d) 
{
  if (*d)
    return;

#ifdef STATS
  proxy_requests++;
#endif

  if (res->status == SFSRO_OK) {
    sbp->reply(res);
    delete (res);
    return;
  } else {
#ifdef STATS
    proxy_misses++;
#endif
    sfsro_proxygetarg *arg = sbp->template getarg<sfsro_proxygetarg> ();
    struct in_addr pubaddr;
    pubaddr.s_addr = arg->pub_addr;
    remote_get_data(arg->fh, &pubaddr, arg->pub_port, res, 
		   wrap(this, &sfsroclient::partialproxygetdata_cb_call, sbp) );
    return;
  }
  
}

void
sfsroclient::partialproxygetdata_cb_call(svccb *sbp, sfsro_datares *res) {

  if (res == NULL) {
    sbp->reply(NULL);
    return;
  }
  
  sfsro_partialproxygetarg *arg = sbp->template getarg<sfsro_partialproxygetarg> ();
  ref<sfs_hash> fh = New refcounted<sfs_hash>(arg->fh);
  db.putdata(fh, res);

  //res is a full data block, we must segment it here
  sfsro_datares *split_res = New sfsro_datares();
  int reslen = res->resok->data.size();
  int fraclen = (reslen/64);
  int len = (fraclen)*(arg->len);
  if (arg->offset + arg->len == 64) len += (reslen % 64);
  int start = (fraclen)*(arg->offset);

  split_res->set_status(SFSRO_OK);
  split_res->resok->data.setsize(len);
  memcpy(split_res->resok->data.base(), (void *)((char *)res->resok->data.base() + start), len);

  sbp->reply(split_res);
}


