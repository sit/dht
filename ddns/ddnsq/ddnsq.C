#include "dhash.h"
#include "ddns.h"
#include <iostream>

#define nelem(a) (sizeof(a)/sizeof((a)[0]))
#define nil 0

ptr<ddns> ddns_clnt;

static int
splitfields(char *s, char **f, int mf, int sep)
{
  char *p;
  int infield, nf;
  
  infield = 0;
  nf = 0;
  for(p=s; *p; p++){
    if(*p == sep){
      *p = '\0';
      infield = 0;
    }else if(!infield){
      f[nf++] = p;
      if(nf >= mf)
        return nf;
      infield = 1;
    }
  }
  return nf;
}

static int
mkaddress(unsigned char *a, char *s)
{
  char *f[5];
  int i, nf;
	
  nf = splitfields(s, f, nelem(f), '.');
  if(nf != 4){
    cerr << "bad ip address format";
    return -1;
  }
  for(i=0; i<4; i++)
    a[i] = atoi(f[i]);
  return 0;
}
	
static ptr<ddnsRR>
mkrr(char *name, dns_type t, int nval, char **val)
{
  char *f[10];
  int nf, i;
  ptr<ddnsRR> *last, rr, rrlist;

  rrlist = nil;
  last = &rrlist;
	
  for(i=0; i<nval; i++){
	rr = New refcounted<ddnsRR>;
	rr->dname = strdup(name);
	rr->type = t;
	switch(t){
	case A:
	  rr->rdlength = 4;
	  if(mkaddress(rr->rdata.address, val[i]) < 0)
	    return nil;
	  break;
	case NS:
	case CNAME:
	case MD:
	case MF:
	case MG:
	case MR:
	case PTR:
	default:
	  rr->rdata.hostname = strdup(val[i]);
	  rr->rdlength = strlen(val[i])+1;
	  break;
	case SOA:
	  nf = splitfields(val[i], f, nelem(f), ';');
	  if(nf != 7){
	    cerr << "soa format: mname;rname;serial;refresh;retry;expire;minttl";
	    return nil;
	  }
	  rr->rdata.soa.mname = strdup(f[0]);
	  rr->rdata.soa.rname = strdup(f[1]);
	  rr->rdata.soa.serial = atoi(f[2]);
	  rr->rdata.soa.refresh = atoi(f[3]);
	  rr->rdata.soa.retry = atoi(f[4]);
	  rr->rdata.soa.expire = atoi(f[5]);
	  rr->rdata.soa.minttl = atoi(f[6]);
      rr->rdlength = strlen(f[0])+1 + strlen(f[1])+1 + 5*4;
      break;
    case WKS:
	  nf = splitfields(val[i], f, nelem(f), ';');
	  if(nf != 2){
	    cerr << "wks format: ipaddress;protocol";
	    return nil;
	  }
      if(mkaddress(rr->rdata.wks.address, f[0]) < 0)
        return nil;
      rr->rdata.wks.protocol = atoi(f[1]);
      rr->rdata.wks.bitmap = (char*)malloc(8);
      strcpy(rr->rdata.wks.bitmap, "Athicha!");
      rr->rdlength = 4+4+9;
      break;
    case HINFO:
	  nf = splitfields(val[i], f, nelem(f), ';');
	  if(nf != 2){
	    cerr << "hinfo format: cpu;os";
	    return nil;
	  }
	  rr->rdata.hinfo.cpu = strdup(f[0]);
	  rr->rdata.hinfo.os = strdup(f[1]);
	  rr->rdlength = strlen(f[0])+1 + strlen(f[1])+1;
	  break;
	case MINFO:
	  nf = splitfields(val[i], f, nelem(f), ';');
	  if(nf != 2){
	    cerr << "minfo format: rmailbx;emailbx";
	    return nil;
	  }
	  rr->rdata.minfo.rmailbx = strdup(f[0]);
	  rr->rdata.minfo.emailbx = strdup(f[1]);
	  rr->rdlength = strlen(f[0])+1 + strlen(f[1])+1;
	  break;
    case MX:
	  nf = splitfields(val[i], f, nelem(f), ';');
	  if(nf != 2){
	    cerr << "mx format: pref;exchange";
	    return nil;
	  }
	  rr->rdata.mx.pref = atoi(f[0]);
	  rr->rdata.mx.exchange = strdup(f[1]);
	  rr->rdlength = 4 + strlen(f[1])+1;
	  break;
	case TXT:
	  rr->rdata.txt_data = strdup(val[i]);
	  rr->rdlength = strlen(val[i])+1;
	  break;
    case DNULL:
      break;
	}
	*last = rr;
	last = &rr->next;
  }
  return rrlist;
}

static char*
cmdinsert(int argc, char **argv)
{
  if(argc < 3)
    return "usage: insert name type val...";

  ptr<ddnsRR> rr = mkrr(strdup(argv[0]), get_dtype(argv[1]), argc-2, &argv[2]);
  if(rr == nil)
    return "parse error";
  ddns_clnt->setactive(rand()&0xFFFF);
  ddns_clnt->store(strdup(argv[0]), rr);
  return nil;
}

static char*
cmddelete(int, char**)
{
  return "delete not implemented";
}

static char*
typestr(dns_type t)
{
  static char buf[20];
  
  switch(t){
  case A:
    return "a";
  case NS:
    return "ns";
  case CNAME:
    return "cname";
  case MD:
    return "md";
  case MF:
    return "mf";
  case MB:
    return "mb";
  case MG:
    return "mg";
  case MR:
    return "mr";
  case PTR:
    return "ptr";
  case SOA:
    return "soa";
  default:
    snprintf(buf, sizeof buf, "dnstype%d", t);
    return buf;
  }
}

static void
printrr(ptr<ddnsRR> rr)
{
  unsigned char *a;

  cout << rr->dname << " " << typestr(rr->type);
  for(; rr; rr=rr->next){
    cout << " ";
    switch(rr->type){
    case A:
      a = rr->rdata.address;
      cout << (int)a[0] << "." << (int)a[1] << "." << (int)a[2] << "." << (int)a[3];
      break;
    case NS:
    case CNAME:
    case MD:
    case MF:
    case MB:
    case MG:
    case MR:
    case PTR:
      cout << rr->rdata.hostname;
      break;
    case SOA:
      cout << rr->rdata.soa.mname << ";";
      cout << rr->rdata.soa.rname << ";";
      cout << rr->rdata.soa.serial << ";";
      cout << rr->rdata.soa.refresh << ";";
      cout << rr->rdata.soa.retry << ";";
      cout << rr->rdata.soa.expire << ";";
      cout << rr->rdata.soa.minttl;
      break;
    case WKS:
      a = rr->rdata.wks.address;
      cout << (int)a[0] << "." << (int)a[1] << "." << (int)a[2] << "." << (int)a[3] << ";";
      cout << rr->rdata.wks.protocol << ";";
      cout << rr->rdata.wks.bitmap;
      break;
    case HINFO:
      cout << rr->rdata.hinfo.cpu << ";" << rr->rdata.hinfo.os;
      break;
    case MINFO:
      cout << rr->rdata.minfo.rmailbx << ";" << rr->rdata.minfo.emailbx;
      break;
    case MX:
      cout << rr->rdata.mx.pref << ";" << rr->rdata.mx.exchange;
      break;
    case TXT:
      cout << rr->rdata.txt_data;
      break;
    case ALL:
      
      break;
    case DNULL:
      break;
    default:
      cout << "\tunknown type\n";
      break;
    }
  }
  cout << "\n";
}

static char*
cmdlookup(int argc, char **argv)
{
  if(argc != 2)
    return "usage: lookup name type";
  ddns_clnt->setactive(rand()&0xFFFF);
  ddns_clnt->lookup(strdup(argv[0]), get_dtype(argv[1]), wrap(printrr));
  return nil;
}
  
static struct {
  char *cmd;
  char *(*fn)(int, char**);
} tab[] = {
  {"insert", cmdinsert},
  {"delete", cmddelete},
  {"lookup", cmdlookup},
};

static void 
usage ()
{
  warnx << "usage: ddnsq\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  char line[256], *err, *f[20];
  int i, nf;
  
  const char *control_socket;
  switch(argc){
  case 1:
    control_socket = "/tmp/chord-sock";
    break;
  case 2:
    control_socket = argv[1];
    break;
  default:
    usage();
  }
    
  srand(getpid()+time(0));
  sfsconst_init ();
  ddns_clnt = New refcounted<ddns> (control_socket, 0);
  for(;;){
    cerr << ">>> ";
    cin.getline(line, sizeof line);
    if(cin.eof())
      break;
    nf = splitfields(line, f, nelem(f), ' ');
    if(nf == 0)
      continue;
    for(i=0; i<(int)nelem(tab); i++)
      if(strcmp(tab[i].cmd, f[0]) == 0)
        break;
    if(i==nelem(tab)){
      cerr << "?unknown command\n";
      continue;
    }
    if((err = tab[i].fn(nf-1, f+1)) != nil)
      cerr << "?" << err << "\n";
  }
  return 0;
}















