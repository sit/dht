#include "ddns.h"

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
  if (!strcasecmp (type, "ALL"))
    return ALL;
  return DT_ERR;
}

void
copy2block (char *data, void *field, 
	    int fieldlen, int &datalen, int &datasize)
{
  if (datasize - datalen < fieldlen) {
    datasize *= 2;
    data = (char *) realloc (data, datasize);
    assert(data);
  }
  memmove (data + datalen, (const char *) field, fieldlen);
  //warn << "data + " << datalen << " = " << data+datalen << "\n";
  datalen += fieldlen;
}

void 
block2soa (soa_data *soa, char *data, int datalen)
{

  char *begstr = data;
  char *endstr = strchr (begstr, '\0');
  int fieldlen = endstr - begstr + 1;
  
  soa->mname = (domain_name) malloc (fieldlen);
  memmove (soa->mname, begstr, fieldlen);
  warn << "soa->mname = " << soa->mname << "\n";

  begstr = endstr + 1;
  endstr = strchr (begstr, '\0');

  fieldlen = endstr - begstr + 1;
  soa->rname = (domain_name) malloc (fieldlen);
  memmove (soa->rname, begstr, fieldlen);
  warn << "soa->rname = " << soa->rname << "\n";
  
  fieldlen = sizeof (uint32);
  begstr = endstr + 1;
  memmove (&soa->serial, begstr, fieldlen);
  warn << "soa->serial = " << soa->serial << "\n";
  begstr += fieldlen;
  memmove (&soa->refresh, begstr, fieldlen);
  warn << "soa->refresh = " << soa->refresh << "\n";
  begstr += fieldlen;
  memmove (&soa->retry, begstr, fieldlen);
  warn << "soa->retry = " << soa->retry << "\n";
  begstr += fieldlen;
  memmove (&soa->expire, begstr, fieldlen);
  warn << "soa->expire = " << soa->expire << "\n";
  begstr += fieldlen;
  memmove (&soa->minttl, begstr, fieldlen);
  warn << "soa->minttl = " << soa->minttl << "\n";

  warn << "datalen = " << datalen << " copiedlen = " << (begstr-data)+fieldlen << "\n\n";
}

void
block2wks (wks_data *wks, char *data, int datalen)
{
  char *begstr = data;
  memmove (&wks->address, begstr, IP32ADDR_SIZE);
  begstr += IP32ADDR_SIZE;

  int fieldlen = sizeof (uint32);
  memmove (&wks->protocol, begstr, fieldlen);
  begstr += fieldlen;

  fieldlen = datalen - IP32ADDR_SIZE - fieldlen;
  wks->bitmap = (string) malloc (fieldlen);
  memmove (wks->bitmap, begstr, fieldlen);
}

void
block2hinfo (hinfo_data *hinfo, char *data, int datalen)
{
  char *begstr = data;
  char *endstr = strchr (begstr, '\0');
  int fieldlen = endstr - begstr + 1;

  hinfo->cpu = (string) malloc (fieldlen);
  memmove (hinfo->cpu, begstr, fieldlen);
  begstr += fieldlen;
  
  fieldlen = datalen - fieldlen;
  hinfo->os = (string) malloc (fieldlen);
  memmove (hinfo->os, begstr, fieldlen);
}

void
block2minfo (minfo_data *minfo, char *data, int datalen)
{
  char *begstr = data;
  char *endstr = strchr (begstr, '\0');
  int fieldlen = endstr - begstr + 1;

  minfo->rmailbx = (domain_name) malloc (fieldlen);
  memmove (minfo->rmailbx, begstr, fieldlen);
  begstr += fieldlen;
  
  fieldlen = datalen - fieldlen;
  minfo->emailbx = (domain_name) malloc (fieldlen);
  memmove (minfo->emailbx, begstr, fieldlen);
}

void
block2mx (mx_data *mx, char *data, int datalen)
{
  char *begstr = data;
  int fieldlen = sizeof (uint32);
  
  memmove (&mx->pref, begstr, fieldlen);
  begstr += fieldlen;

  fieldlen = datalen - fieldlen;
  mx->exchange = (domain_name) malloc (fieldlen);
  memmove (mx->exchange, begstr, fieldlen);
}
