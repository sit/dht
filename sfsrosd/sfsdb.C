#include "sfsdb.h"
#include "sfsrosd.h"
#include "sfsrodb_core.h"

unsigned int mtu = 8192;

sfsrodb::sfsrodb ()
{
  dbp = NULL;
}

sfsrodb::sfsrodb (const char *dbfile)
{

  int fd = unixsocket_connect(dbfile);
  if (fd < 0)
    fatal << "error on " << dbfile << "\n";
  
  dbp = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);

  str key = str (NOTSOPRIVATE_KEY);
  if (!(sk = import_rabin_priv (key, NULL))) {
    fatal << "could not decode key\n";
  } 

}

void
sfsrodb::getinfo(sfs_fsinfo *fsinfo)
{
  
  // store file system in db
  sfs_hash root_fh;
  
  memset(root_fh.base(), 0, 20);
  fsinfo->set_prog (SFSRO_PROGRAM);
  fsinfo->sfsro->set_vers (SFSRO_VERSION);
  
  memcpy (fsinfo->sfsro->v1->info.iv.base (), "sqeamish_ossifrage_", SFSRO_IVSIZE);
  fsinfo->sfsro->v1->info.rootfh = root_fh;

  fsinfo->sfsro->v1->info.type = SFS_ROFSINFO;
  fsinfo->sfsro->v1->info.start = time (NULL);
  fsinfo->sfsro->v1->info.duration = 0xFFFFFF;

  fsinfo->sfsro->v1->sig = sk->sign (xdr2str (fsinfo->sfsro->v1->info));

}

void
sfsrodb::getconnectres (sfs_connectres *conres)
{
    
  // the plan is to synthesize the cres on the fly, customized for this host
  // don't tell dm

  conres->set_status (SFS_OK);
  conres->reply->servinfo.release = SFS_RELEASE;
  conres->reply->servinfo.host.type = SFS_HOSTINFO;
  char hostname[1024];
  strcpy(hostname, myname ().cstr());
  for (unsigned int i = 0; i < strlen(hostname); i++) hostname[i] = tolower(hostname[i]);
  conres->reply->servinfo.host.hostname = str (hostname);
  warn << conres->reply->servinfo.host.hostname << "\n";

  conres->reply->servinfo.host.pubkey = sk->n;
  conres->reply->servinfo.prog = SFSRO_PROGRAM;
  conres->reply->servinfo.vers = SFSRO_VERSION;
  bzero (&conres->reply->charge, sizeof (sfs_hashcharge));

}


void
sfsrodb::getdata (sfsro_getarg *g_arg, sfsro_datares *res, callback<void>::ref cb)
{

  //the shame of the below code will never wear off...
  
  char key1[20] = {0xfa,0xcd,0x7a,0x12,0x2b,0xcc,0xe4,0x65,0x6f,0x23,0x70,0x79,0x05,0x67,0x01,0x86,0xc0,0x5c,0x23,0xd5 };
  char key0[20] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  char data0[48] = { 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x2e, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 };
  
  char data1[128] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x3a, 0xb8, 0x0e, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x3a, 0xb8, 0x0e, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfa, 0xcd, 0x7a, 0x12, 0x2b, 0xcc, 0xe4, 0x65, 0x6f, 0x23, 0x70, 0x79, 0x05, 0x67, 0x01, 0x86, 0xc0, 0x5c, 0x23, 0xd5, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, };
  
  sfs_hash *fh = &g_arg->fh;

  char *dat = NULL;
  int len = 0;
  if (memcmp (key0, fh->base (), 20) == 0) {
    dat = data1;
    len = 128;
  } else if (memcmp (key1, fh->base (), 20) == 0) {
    dat = data0;
    len = 48;
  } 
  
  if (dat) {
    res->set_status (SFSRO_OK);
    res->resok->data.setsize (len);
    res->resok->offset = 0;
    res->resok->attr.size = len;
    memcpy (res->resok->data.base (), dat, len);
    (*cb)();
    return;
  }

  bigint n = fh2mpz((const void *)fh->base (), fh->size ());

  dhash_fetch_arg arg;
  arg.key = n;
  arg.start = g_arg->offset;
  arg.len = (g_arg->len > mtu) ? mtu : g_arg->len;
  
  dhash_res *dres = New dhash_res();
  dbp->call(DHASHPROC_LOOKUP, &arg, dres, wrap(this, &sfsrodb::getdata_one_cb, cb, res, dres, n, g_arg));

}

void
sfsrodb::getdata_one_cb (callback<void>::ref cb, sfsro_datares *res, dhash_res *dres, sfs_ID n, sfsro_getarg *g_arg,
			 clnt_stat err)  
{

 if ((err != 0) || (dres->status != DHASH_OK)) {
    res->set_status(SFSRO_ERRNOENT);
    (*cb)();
 } else { 
   warn << "read " << dres->resok->res.size () << " of " << dres->resok->attr.size << "bytes\n";
   res->set_status (SFSRO_OK);
   res->resok->data.setsize(dres->resok->attr.size);
   res->resok->offset = g_arg->offset;
   memcpy (res->resok->data.base (), dres->resok->res.base (), dres->resok->res.size ());
   if (dres->resok->res.size() <= g_arg->len) {
     res->resok->attr.size = dres->resok->attr.size;
     (*cb)();
   } else {
     dhash_fetch_arg arg;
     arg.key = n;
     unsigned int offset = g_arg->offset + dres->resok->res.size ();
     unsigned int *read = New unsigned int(offset);
     do {
       arg.start = offset;
       arg.len = (mtu + offset < (g_arg->offset + g_arg->len) ) ? mtu 
	 : (g_arg->offset + g_arg->len) - offset;
       dhash_res *adres = New dhash_res();
       dbp->call(DHASHPROC_LOOKUP, &arg, res, wrap(this, &sfsrodb::getdata_cb, cb, res, adres, read, 
						   g_arg->len));
       offset += arg.len;
     } while (offset < dres->resok->attr.size);
   }
 }
 delete dres;
}

void
sfsrodb::getdata_cb(callback<void>::ref cb, sfsro_datares *res, dhash_res *dres, 
		    unsigned int *read, unsigned int size,
		    clnt_stat err)  
{

  if (err != 0){
    res->set_status(SFSRO_ERRNOENT);
    (*cb)();
  } else {
    warn << "read " << dres->resok->res.size () << " of " << dres->resok->attr.size << "\n";  
    memcpy ((char *)res->resok->data.base () + dres->resok->offset, 
	    dres->resok->res.base (), dres->resok->res.size ());
    *read += dres->resok->res.size ();
    if (*read == size) {
      res->resok->attr.size = size;
      (*cb)();
      delete read;
    }
  }
  delete dres;
}
