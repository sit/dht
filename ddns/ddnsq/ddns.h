#ifndef _DDNS_H_
#define _DDNS_H_

#include "async.h"
#include "chord.h"
#include "dhash.h"

typedef char* domain_name;
typedef char* string;
typedef uint32 ip32addr;
typedef int32 ttl_t;
typedef uint32 rdl_t; /* uint16, actually, but it seems to confuse libasync */

#define DMTU 1024
#define DOMAIN_LEN 256

enum ddns_stat {
  DDNS_OK = 0,
  DDNS_ERR = 1,
  DDNS_DHASHERR = 1
};

enum dns_type {
	A	= 1,
	NS	= 2,
	MD	= 3,
	MF	= 4,
	CNAME	= 5,
	SOA	= 6,
	MB	= 7,
	MG	= 8,
	MR	= 9,
	DNULL	= 10,
	WKS	= 11,
	PTR	= 12,
	HINFO	= 13,
	MINFO	= 14,
	MX	= 15,
	TXT	= 16,
	ALL     = 255,
	DT_ERR  = 0
};

enum dns_class {
	IN = 1,
	CS = 2, 
	CH = 3,
	HS = 4
};

struct soa_data {
  domain_name mname;
  domain_name rname;
  uint32 serial;
  uint32 refresh;
  uint32 retry;
  uint32 expire;
  uint32 minttl;
};

struct wks_data {
  unsigned char address[4];
  uint32 protocol; /* Actually, char */
  string bitmap;
};

struct hinfo_data {
  string cpu;
  string os;
};

struct minfo_data {
  domain_name rmailbx;
  domain_name emailbx;
};

struct mx_data {
  uint32 pref;
  domain_name exchange;
};
 
typedef union {
//case A:
  unsigned char address[4];
//case NS:
//case CNAME:
//case MD:
//case MF:
//case MB:
//case MG:
//case MR:
//case PTR:
  domain_name hostname;
//case SOA:
  soa_data soa;
//case WKS:
  wks_data wks;
//case HINFO:
  hinfo_data hinfo;
//case MINFO:
  minfo_data minfo;
//case MX:
  mx_data mx;
//case TXT:
  string txt_data;
//case DNULL:	
//default:
  string rdata;	
} dns_rdata;	

struct ddnsRR {
  domain_name dname;
  dns_type type; 
  dns_class cls;
  ttl_t ttl;
  rdl_t rdlength; 
  dns_rdata rdata;
  ptr<ddnsRR> next;
};

#define DNS_TYPE_SIZE  sizeof (dns_type)
#define DNS_CLASS_SIZE sizeof (dns_class)
#define IP32ADDR_SIZE  sizeof (ip32addr)
#define TTL_SIZE       sizeof (ttl_t)
#define RDLENGTH_SIZE  sizeof (rdl_t)

class ddns {
  typedef callback<void, ptr<ddnsRR> >::ref lcb_t;
  
  const char* control_socket;
  ptr<aclnt> dhash_clnt;
  ptr<aclnt> get_dclnt ();

  int nlookup, nstore;
  chordID getcID (domain_name, dns_type);
  int ddnsRR2block (ptr<ddnsRR>, char *, int);
  void store_cb (domain_name, chordID, ref<dhash_storeres>, clnt_stat);
  void lookup_cb (domain_name, chordID, ref<dhash_res>, ddns::lcb_t, clnt_stat);

 public:
  ddns (const char *, int);
  ~ddns ();
  void store (domain_name, ref<ddnsRR>);
  void lookup (domain_name, dns_type, ddns::lcb_t);
  
};

/* misc.C */

dns_type get_dtype (const char *type);
void copy2block (char *data, void *field, 
		 int fieldlen, int &datalen, int &datasize);
void block2soa (soa_data *, char *data, int datalen);
void block2wks (wks_data *, char *data, int datalen);
void block2hinfo (hinfo_data *, char *data, int datalen);
void block2minfo (minfo_data *, char *data, int datalen);
void block2mx (mx_data *, char *data, int datalen);

/* End misc.C */

#endif /* _DDNS_H_ */













