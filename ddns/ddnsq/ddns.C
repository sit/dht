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

void
copy2block (char *data, void *field, 
	    int fieldlen, int &datalen, int &datasize)
{
  warn << "datasize = " << datasize << "\n";
  warn << "datap - data = " << datalen << "\n";
  
  if (datasize - datalen < fieldlen) {
    datasize *= 2;
    data = (char *) realloc (data, datasize);
    assert(data);
  }
  memmove (data + datalen, (const char *) field, fieldlen);
  datalen += fieldlen;
  warn << "datap - data = " << datalen << "\n";
}

int 
ddns::ddnsRR2block (ref<ddnsRR> rr, char *data, int datasize)
{
  int datalen = 0;
  warn << "DMTU = " << DMTU << " data length = " << datasize << "\n";
  copy2block (data, (void *) rr->dname, 
	      strlen (rr->dname), datalen, datasize);
  copy2block (data, (void *) &rr->type, 
	      DNS_TYPE_SIZE, datalen, datasize);
  copy2block (data, (void *) &rr->cls, 
	      DNS_CLASS_SIZE, datalen, datasize);
  copy2block (data,(void *) &rr->ttl, 
	      TTL_SIZE, datalen, datasize);
  copy2block (data, (void *) &rr->rdlength, 
	      RDLENGTH_SIZE, datalen, datasize);

  switch (rr->type) {
  case A:
    copy2block (data, (void *) &rr->rdata.address, 
		rr->rdlength, datalen, datasize);
    break;
  default:
    return -1;
  }  
  return (datalen); /* real datasize */
}

chordID 
ddns::getcID (domain_name dname)
{
  chordID key;
  char id[sha1::hashsize];
  sha1_hash (id, dname, strlen (dname));
  mpz_set_rawmag_be (&key, id, sizeof (id));
  return key;
}

void 
ddns::store (domain_name dname, ref<ddnsRR> rr)
{
  nstore++;
  warn << "dname = " << dname << "\n";
  ref<dhash_storeres> res = New refcounted<dhash_storeres> (); 
  ref<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
  i_arg->key = getcID (dname);
  char *data = (char *) malloc(DMTU);
  int datasize = ddnsRR2block (rr, data, DMTU);
  warn << "final datasize = " << datasize << "\n";
  i_arg->data.setsize (datasize);
  i_arg->type = DHASH_STORE;
  i_arg->attr.size = datasize;
  i_arg->offset = 0;
  memmove (i_arg->data.base (), data, datasize);
  
  dhash_clnt -> call (DHASHPROC_INSERT, i_arg, res, wrap (this, &ddns::store_cb, 
							 dname, i_arg->key, res));
  delete data;
  while (nstore > 0)
    acheck ();
}

void 
ddns::store_cb (domain_name dname, chordID key, 
		ref<dhash_storeres> res, clnt_stat err) 
{
  if (err || res->status) {
    warn << "store_cb: Err: " << strerror (err) 
	 << " dhash_insert status: " << strerror (res->status) << "\n";
  } else 
    warn << "Successfully stored <" << dname << ", type" << ">\n"; 
  delete dname;
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
      warn << "Done: " << "res->size = " << off << "\n";
      if (off > 0) {
	ddnsRR rr;
	rr.dname = dname;
	int offset = strlen (dname);
	memmove (&rr.type, res->resok->res.base () + offset, DNS_TYPE_SIZE);
	offset += DNS_TYPE_SIZE;
	memmove (&rr.cls, res->resok->res.base () + offset, DNS_CLASS_SIZE);
	offset += DNS_CLASS_SIZE;
	memmove (&rr.ttl, res->resok->res.base () + offset, TTL_SIZE);
	offset += TTL_SIZE;
	memmove (&rr.rdlength, res->resok->res.base () + offset, RDLENGTH_SIZE);
	offset += RDLENGTH_SIZE;

	warn << "dname = " << rr.dname << " len = " << strlen(rr.dname) << "\n";
	warn << "type = " << rr.type << "\n";
	warn << "class = " << rr.cls << "\n";
	warn << "ttl = " << rr.ttl << "\n";
	warn << "rdlength " << rr.rdlength << "\n";

	switch (rr.type) {
	case A:
	  memmove (&rr.rdata.address, res->resok->res.base () + offset, rr.rdlength);
	  warn << "rdata.address = " 
	       << (rr.rdata.address >> 24) 
	       << "." << ((rr.rdata.address << 8)  >> 24) 
	       << "." << ((rr.rdata.address << 16) >> 24)
 	       << "." << ((rr.rdata.address << 24) >> 24) 
	       << "\n";
	  break;
	default:
	  break;
	}
      }
    } else warn << "Not done: do more \n";
  }
    //delete dname;
  nlookup--;
}






