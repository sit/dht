#include "chord.h"
#include "dhash.h"
#include "ddns.h"
#include "sfsmisc.h"

dns_type 
get_dtype (const char *type) 
{
  if (!strcasecmp (type, "A"))
    return A;
  if (!strcasecmp (type, "NS"))
    return NS;
  if (!strcasecmp (type, "MD"))
    return MD;
  if (!strcasecmp (type, "MF"))
    return MF;
  if (!strcasecmp (type, "CNAME"))
    return CNAME;
  if (!strcasecmp (type, "SOA"))
    return SOA;
  if (!strcasecmp (type, "MB"))
    return MB;
  if (!strcasecmp (type, "MG"))
    return MG;
  if (!strcasecmp (type, "MR"))
    return MR;
  if (!strcasecmp (type, "DNULL"))
    return DNULL;
  if (!strcasecmp (type, "WKS"))
    return WKS;
  if (!strcasecmp (type, "PTR"))
    return PTR;
  if (!strcasecmp (type, "HINFO"))
    return HINFO;
  if (!strcasecmp (type, "MINFO"))
    return MINFO;
  if (!strcasecmp (type, "MX"))
    return MX;
  if (!strcasecmp (type, "TXT"))
    return TXT;
  return DT_ERR;
}

ddns::ddns (const char *control_skt, int vnode) : 
  control_socket(control_skt), nlookup(0), nstore(0)
{
  int fd = unixsocket_connect (control_socket);
  if (fd < 0)
    fatal ("%s: %m\n", control_socket);
  dhash_clnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  dhash_stat ares;
  dhash_clnt->scall (DHASHPROC_ACTIVE, &vnode, &ares);
}

ptr<aclnt> 
ddns::get_dclnt () 
{
  int fd;
  
  if (dhash_clnt)
    return dhash_clnt;
  fd = unixsocket_connect (control_socket);
  if (fd < 0)
    fatal ("%s: %m\n", control_socket);
  dhash_clnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  return dhash_clnt;
}

ddns::~ddns () 
{
}

int 
ddns::ddnsRR2block (ref<ddnsRR> rr, char *data)
{
  data = (char *) malloc(10);
  return 0;
}

chordID 
ddns::getcID (domain_name dname)
{
  chordID key;
  char id[sha1::hashsize];
  sha1_hash (id, dname, sizeof (*dname));
  mpz_set_rawmag_be (&key, id, sizeof (id));
  return key;
}

#if 0
void
ddns::store (domain_name dname, dns_type dt, string rr_data) 
{
  nstore++;

  ref<dhash_storeres> res = New refcounted<dhash_storeres> (); 
  ref<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
  i_arg->key = getcID (dname);
  char *data = NULL;
  int datasize = insertRR (dname, dt, rr_data, data);
  i_arg->data.setsize (datasize);
  i_arg->type = DHASH_STORE;
  i_arg->attr.size = datasize;
  i_arg->offset = 0;
  memcpy (i_arg->data.base (), data, datasize);
  
  dhash_clnt -> call (DHASHPROC_INSERT, i_arg, res, wrap (this, &ddns::store_cb, 
							 dname, i_arg->key, dt, res));
  delete data;
  while (nstore > 0)
    acheck ();
}
#else
void 
ddns::store (domain_name dname, ref<ddnsRR> rr)
{
  nstore++;

  ref<dhash_storeres> res = New refcounted<dhash_storeres> (); 
  ref<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
  i_arg->key = getcID (dname);
  char *data = NULL;
  int datasize = ddnsRR2block (rr, data);
  i_arg->data.setsize (datasize);
  i_arg->type = DHASH_STORE;
  i_arg->attr.size = datasize;
  i_arg->offset = 0;
  memcpy (i_arg->data.base (), data, datasize);
  
  dhash_clnt -> call (DHASHPROC_INSERT, i_arg, res, wrap (this, &ddns::store_cb, 
							 dname, i_arg->key, res));
  delete data;
  while (nstore > 0)
    acheck ();
  
}

#endif

void 
ddns::store_cb (domain_name dname, chordID key, 
		ref<dhash_storeres> res, clnt_stat err) 
{
  if (err || res->status) {
    warn << "store_cb: Err: " << strerror (err) 
	 << " dhash_insert status: " << strerror (res->status) << "\n";
  } else 
    warn << "Successfully stored <" << dname << ", type" << ">\n"; 
  nstore--;
}

void
ddns::lookup (domain_name dname)
{
  nlookup++;
  ref<dhash_res> res = New refcounted<dhash_res> (DHASH_OK);
  
  dhash_fetch_arg arg;
  arg.key = getcID (dname);
  arg.len = DMTU;
  arg.start = 0;
  dhash_clnt->call (DHASHPROC_LOOKUP, &arg, res, wrap(this, &ddns::lookup_cb, 
							dname, arg.key, res));
  while (nlookup > 0)
    acheck ();
}

void
ddns::lookup_cb (domain_name dname, chordID key, 
		 ref<dhash_res> res, clnt_stat err)
{
  if (err || (res->status != DHASH_OK)) {
    if (res->status == DHASH_NOENT)
      warn << "No entry for " << dname << "\n";
    else 
      warn << "lookup_cb: Err: " << strerror (err) 
	   << " dhash_lookup status: " << res->status << "\n";
  } else {
    uint off = res->resok->res.size ();
    if (off == res->resok->attr.size) {
      warn << "Done: " << "res->size = " << off;
      if (off > 0)
	warnx << " res.data = " << (char *) res->resok->res.base ();
      warnx << "\n";
    } else warn << "Not done: do more \n";
  }
  delete dname;
  nlookup--;
}

