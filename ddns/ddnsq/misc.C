#include "ddns.h"

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
  warn << "wks->address = " << wks->address << "\n";
  begstr += IP32ADDR_SIZE;

  int fieldlen = sizeof (uint32);
  memmove (&wks->protocol, begstr, fieldlen);
  warn << "wks->protocol = " << wks->protocol << "\n";
  begstr += fieldlen;

  fieldlen = datalen - IP32ADDR_SIZE - fieldlen;
  wks->bitmap = (string) malloc (fieldlen);
  memmove (wks->bitmap, begstr, fieldlen);
  warn << "wks->bitmap = " << wks->bitmap << "\n";
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
