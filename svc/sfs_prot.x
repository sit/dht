/* $Id: sfs_prot.x,v 1.3 2001/02/25 05:28:46 fdabek Exp $ */

/*
 * This file was written by David Mazieres.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"

#ifdef SFSSVC
%#include "nfs3exp_prot.h"
#else /* !SFSSVC */
%#include "nfs3_prot.h"
const ex_NFS_PROGRAM = 344444;
const ex_NFS_V3 = 3;
#endif

#ifdef RPCC
# ifndef UNION_ONLY_DEFAULT
#  define UNION_ONLY_DEFAULT 1
# endif /* UNION_ONLY_DEFAULT */
#endif

#ifndef FSINFO
#define FSINFO sfs_fsinfo
#endif /* !FSINFO */

const SFS_PORT = 4;
const SFS_RELEASE = 4;		/* 100 * release no. where protocol changed */

enum sfsstat {
  SFS_OK = 0,
  SFS_BADLOGIN = 1,
  SFS_NOSUCHHOST = 2,
  SFS_NOTSUPP = 10004,
  SFS_TEMPERR = 10008,
  SFS_REDIRECT = 10020
};

/* Types of hashed or signed messages */
enum sfs_msgtype {
  SFS_HOSTINFO = 1,
  SFS_KSC = 2,
  SFS_KCS = 3,
  SFS_SESSINFO = 4,
  SFS_AUTHINFO = 5,
  SFS_SIGNED_AUTHREQ = 6,
  SFS_AUTHREGISTER = 7,
  SFS_AUTHUPDATE = 8,
  SFS_PATHREVOKE = 9,
  SFS_KEYCERT = 10,
  SFS_ROFSINFO = 11
};

/* Type of service requested by clients */
enum sfs_service {
  SFS_SFS = 1,			/* File system service */
  SFS_AUTHSERV = 2,		/* Crypto key server */
  SFS_REX = 3			/* Remote execution */
};

typedef string sfs_extension<>;
typedef string sfs_hostname<222>;
typedef opaque sfs_hash[20];
typedef opaque sfs_secret[16];
typedef unsigned hyper sfs_seqno;
typedef unsigned hyper sfs_time;

typedef bigint sfs_pubkey;
typedef bigint sfs_ctext;
typedef bigint sfs_sig;
typedef uint32 sfs_ipaddr;
typedef int32  sfs_ipport;

struct sfs_hashcharge {
  unsigned int bitcost;
  sfs_hash target;
};
typedef opaque sfs_hashpay[64];


/*
 * Hashed structures
 */

/* Two, identical copies of of the sfs_hostinfo structure are
 * concatenated and then hashed with SHA-1 to form the hostid. */
struct sfs_hostinfo {
  sfs_msgtype type;		/* = SFS_HOSTINFO */
  sfs_hostname hostname;
  sfs_pubkey pubkey;
};

struct sfs_connectinfo {
  unsigned release;		/* Client release */
  sfs_service service;
  sfs_hostname name;		/* Server hostname */
  sfs_hash hostid;		/* = SHA1 (sfs_hostinfo, sfs_hostinfo) */
  sfs_extension extensions<>;
};

struct sfs_servinfo {
  unsigned release;		/* Server release */
  sfs_hostinfo host;		/* Server hostinfo */
  unsigned prog;
  unsigned vers;
};

/* The two shared session keys, ksc and kcs, are the SHA-1 hashes of
 * sfs_sesskeydat with type = SFS_KCS or SFS_KSC.  */
struct sfs_sesskeydat {
  sfs_msgtype type;		/* = SFS_KSC or SFS_KCS */
  sfs_servinfo si;
  sfs_secret sshare;		/* Server's share of session key */
  sfs_connectinfo ci;
  sfs_pubkey kc;
  sfs_secret cshare;		/* Client's share of session key */
};

/* The sessinfo structure is hashed to produce a session ID--a
 * structure both the client and server know to be fresh, but which,
 * unlike the session keys, can safely be divulged to 3rd parties
 * during user authentication.  */
struct sfs_sessinfo {
  sfs_msgtype type;		/* = SFS_SESSINFO */
  opaque ksc<>;			/* = SHA-1 ({SFS_KSC, ...}) */
  opaque kcs<>;			/* = SHA-1 ({SFS_KCS, ...}) */
};

/* The authinfo structure is hashed to produce an authentication ID.
 * The authentication ID can be computed by an untrusted party (such
 * as a user's unprivileged authentication agent), but allows that
 * third party to verify or log the hostname and hostid to which
 * authentication is taking place.  */
struct sfs_authinfo {
  sfs_msgtype type;		/* = SFS_AUTHINFO */
  sfs_service service;
  sfs_hostname name;
  sfs_hash hostid;		/* = SHA-1 (sfs_hostinfo, sfs_hostinfo) */
  sfs_hash sessid;		/* = SHA-1 (sfs_sessinfo) */
};

/*
 * Public key ciphertexts
 */

struct sfs_kmsg {
  sfs_secret kcs_share;
  sfs_secret ksc_share;
};

/*
 * Signed messages
 */

struct sfs_keycert_msg {
  sfs_msgtype type;		/* = SFS_KEYCERT */
  unsigned duration;		/* Lifetime of certificate */
  sfs_time start;		/* Time of signature */
  sfs_pubkey key;		/* Temporary public key */
};

struct sfs_keycert {
  sfs_keycert_msg msg;
  sfs_sig sig;
};

struct sfs_signed_authreq {
  sfs_msgtype type;		/* = SFS_SIGNED_AUTHREQ */
  sfs_hash authid;		/* SHA-1 (sfs_authinfo) */
  sfs_seqno seqno;		/* Counter, value unique per authid */
  opaque usrinfo[16];		/* All 0s, or <= 15 character logname */
};

struct sfs_redirect {
  sfs_time serial;
  sfs_time expire;
  sfs_hostinfo hostinfo;
};

/* Note: an sfs_signed_pathrevoke with a NULL redirect (i.e. a
 * revocation certificate) always takes precedence over one with a
 * non-NULL redirect (a forwarding pointer). */
struct sfs_pathrevoke_msg {
  sfs_msgtype type;		/* = SFS_PATHREVOKE */
  sfs_hostinfo path;		/* Hostinfo of old self-certifying pathname */
  sfs_redirect *redirect;	/* Optional forwarding pointer */
};

struct sfs_pathrevoke {
  sfs_pathrevoke_msg msg;
  sfs_sig sig;
};

/*
 * RPC arguments and results
 */

typedef sfs_connectinfo sfs_connectarg;

struct sfs_connectok {
  sfs_servinfo servinfo;
  sfs_hashcharge charge;
};

union sfs_connectres switch (sfsstat status) {
 case SFS_OK:
   sfs_connectok reply;
 case SFS_REDIRECT:
   sfs_pathrevoke revoke;
 default:
   void;
};

struct sfs_encryptarg {
  sfs_hashpay payment;
  sfs_ctext kmsg;
  sfs_pubkey pubkey;
};
typedef sfs_ctext sfs_encryptres;

struct sfs_nfs3_subfs {
  nfspath3 path;
  nfs_fh3 fh;
};
struct sfs_nfs3_fsinfo {
  nfs_fh3 root;
  sfs_nfs3_subfs subfs<>;
};

union sfs_nfs_fsinfo switch (int vers) {
 case ex_NFS_V3:
   sfs_nfs3_fsinfo v3;
};


const SFSRO_IVSIZE = 16;

#define SFSRO_PROGRAM 344446
#define SFSRO_VERSION 1

struct sfsro1_signed_fsinfo {
  sfs_msgtype type; /* = SFS_ROFSINFO */
  unsigned start;       /* In seconds since UNIX epoch */
  unsigned duration;	/* seconds */
  opaque iv[SFSRO_IVSIZE];
  sfs_hash rootfh;
  sfs_hash fhdb;
};

struct sfsro1_fsinfo {
  sfsro1_signed_fsinfo info;
  sfs_sig sig;
};

union sfsro_fsinfo switch (int vers) {
 case SFSRO_VERSION:
   sfsro1_fsinfo v1;
};

union sfs_fsinfo switch (int prog) {
 case ex_NFS_PROGRAM:
   sfs_nfs_fsinfo nfs;
 case SFSRO_PROGRAM:
   sfsro_fsinfo sfsro;
 default:
   void;
};


typedef string sfs_idname<32>;

union sfs_opt_idname switch (bool present) {
 case TRUE:
   sfs_idname name;
 case FALSE:
   void;
};

struct sfs_idnums {
  int uid;
  int gid;
};

struct sfs_idnames {
  sfs_opt_idname uidname;
  sfs_opt_idname gidname;
};

enum sfs_loginstat {
  SFSLOGIN_OK = 0,		/* Login succeeded */
  SFSLOGIN_MORE = 1,		/* More communication with client needed */
  SFSLOGIN_BAD = 2,		/* Invalid login */
  SFSLOGIN_ALLBAD = 3		/* Invalid login don't try again */
};


union sfs_loginres switch (sfs_loginstat status) {
 case SFSLOGIN_OK:
   unsigned authno;
 case SFSLOGIN_MORE:
   opaque resmore<>;
 case SFSLOGIN_BAD:
 case SFSLOGIN_ALLBAD:
   void;
};

struct sfs_loginarg {
  sfs_seqno seqno;
  opaque certificate<>;		/* marshalled sfs_autharg */
};


/*
 * User-authentication structures
 */

enum sfsauth_stat {
  SFSAUTH_OK = 0,
  SFSAUTH_LOGINMORE = 1,	/* More communication with client needed */
  SFSAUTH_FAILED = 2,
  SFSAUTH_LOGINALLBAD = 3,	/* Invalid login don't try again */
  SFSAUTH_NOTSOCK = 4,
  SFSAUTH_BADUSERNAME = 5,
  SFSAUTH_WRONGUID = 6,
  SFSAUTH_DENYROOT = 7,
  SFSAUTH_BADSHELL = 8,
  SFSAUTH_DENYFILE = 9,
  SFSAUTH_BADPASSWORD = 10,
  SFSAUTH_USEREXISTS = 11,
  SFSAUTH_NOCHANGES = 12,
  SFSAUTH_NOSRP = 13,
  SFSAUTH_BADSIGNATURE = 14,
  SFSAUTH_PROTOERR = 15,
  SFSAUTH_NOTTHERE = 16,
  SFSAUTH_BADAUTHID = 17,
  SFSAUTH_KEYEXISTS = 18,
  SFSAUTH_BADKEYNAME = 19
};

enum sfs_authtype {
  SFS_NOAUTH = 0,
  SFS_AUTHREQ = 1
};

struct sfs_authreq {
  sfs_pubkey usrkey;		/* Key with which signed_req signed */
  sfs_sig signed_req;		/* Recoveraby signed sfs_signed_authreq */
  /* string usrinfo<15>;	/* Logname or "" if any */
};

union sfs_autharg switch (sfs_authtype type) {
 case SFS_NOAUTH:
   void;
 case SFS_AUTHREQ:
   sfs_authreq req;
};

enum sfs_credtype {
  SFS_NOCRED = 0,
  SFS_UNIXCRED = 1
};

struct sfs_unixcred {
  string username<>;
  string homedir<>;
  string shell<>;
  unsigned uid;
  unsigned gid;
  unsigned groups<>;
};

union sfsauth_cred switch (sfs_credtype type) {
 case SFS_NOCRED:
   void;
 case SFS_UNIXCRED:
   sfs_unixcred unixcred;
};

struct sfsauth_loginokres {
  sfsauth_cred cred;
  sfs_hash authid;
  sfs_seqno seqno;
};


union sfsauth_loginres switch (sfs_loginstat status) {
 case SFSLOGIN_OK:
   sfsauth_loginokres resok;
 case SFSLOGIN_MORE:
   opaque resmore<>;
 default:
   void;
};


/*
 * Secure Remote Password (SRP) protocol
 */

struct sfssrp_parms {
  bigint N;			/* Prime */
  bigint g;			/* Generator */
};

union sfsauth_srpparmsres switch (sfsauth_stat status) {
 case SFSAUTH_OK:
   sfssrp_parms parms;
 default:
   void;
};

typedef opaque sfssrp_bytes<>;
struct sfssrp_init_arg {
  string username<>;
  sfssrp_bytes msg;
};

union sfsauth_srpres switch (sfsauth_stat status) {
 case SFSAUTH_OK:
   sfssrp_bytes msg;
 default:
   void;
};

struct sfsauth_fetchresok {
  string privkey<>;
  sfs_hash hostid;
};

union sfsauth_fetchres switch (sfsauth_stat status) {
 case SFSAUTH_OK:
   sfsauth_fetchresok resok;
 default:
   void;
};

struct sfsauth_srpinfo {
  string info<>;
  string privkey<>;
};

struct sfsauth_registermsg {
  sfs_msgtype type;		/* = SFS_AUTHREGISTER */
  string username<>;		/* logname */
  string password<>;		/* password for an add */
  sfs_pubkey pubkey;
  sfsauth_srpinfo *srpinfo;
};

struct sfsauth_registerarg {
  sfsauth_registermsg msg;
  sfs_sig sig;
};

enum sfsauth_registerres {
  SFSAUTH_REGISTER_OK = 0,
  SFSAUTH_REGISTER_NOTSOCK = 1,
  SFSAUTH_REGISTER_BADUSERNAME = 2,
  SFSAUTH_REGISTER_WRONGUID = 3,
  SFSAUTH_REGISTER_DENYROOT = 4,
  SFSAUTH_REGISTER_BADSHELL = 5,
  SFSAUTH_REGISTER_DENYFILE = 6,
  SFSAUTH_REGISTER_BADPASSWORD = 7,
  SFSAUTH_REGISTER_USEREXISTS = 8,
  SFSAUTH_REGISTER_FAILED = 9,
  SFSAUTH_REGISTER_NOCHANGES = 10,
  SFSAUTH_REGISTER_NOSRP = 11,
  SFSAUTH_REGISTER_BADSIG = 12
};

struct sfsauth_updatemsg {
  sfs_msgtype type;		/* = SFS_AUTHUPDATE */
  sfs_hash authid;		/* SHA-1 (sfs_authinfo);
				   service is SFS_AUTHSERV */
  sfs_pubkey oldkey;
  sfs_pubkey newkey;
  sfsauth_srpinfo *srpinfo;
  /* maybe username? */
};

struct sfsauth_updatearg {
  sfsauth_updatemsg msg;
  sfs_sig osig;			/* computed with sfsauth_updatereq.oldkey */
  sfs_sig nsig;			/* computed with sfsauth_updatereq.newkey */
};

program SFS_PROGRAM {
	version SFS_VERSION {
		void 
		SFSPROC_NULL (void) = 0;

		sfs_connectres
		SFSPROC_CONNECT (sfs_connectarg) = 1;

		sfs_encryptres
		SFSPROC_ENCRYPT (sfs_encryptarg) = 2;

		FSINFO
		SFSPROC_GETFSINFO (void) = 3;

		sfs_loginres
		SFSPROC_LOGIN (sfs_loginarg) = 4;

		void
		SFSPROC_LOGOUT (unsigned) = 5;

		sfs_idnames
		SFSPROC_IDNAMES (sfs_idnums) = 6;

		sfs_idnums
		SFSPROC_IDNUMS (sfs_idnames) = 7;

		sfsauth_cred
		SFSPROC_GETCRED (void) = 8;
	} = 1;
} = 344440;

program SFSCB_PROGRAM {
	version SFSCB_VERSION {
		void 
		SFSCBPROC_NULL(void) = 0;
	} = 1;
} = 344441;

program SFSAUTH_PROGRAM {
	version SFSAUTH_VERSION {
		void 
		SFSAUTHPROC_NULL (void) = 0;

		sfsauth_loginres
		SFSAUTHPROC_LOGIN (sfs_loginarg) = 1;

		sfsauth_stat
		SFSAUTHPROC_REGISTER (sfsauth_registerarg) = 2;

		sfsauth_stat
		SFSAUTHPROC_UPDATE (sfsauth_updatearg) = 3;

		sfsauth_srpparmsres
		SFSAUTHPROC_SRP_GETPARAMS (void) = 4;

		sfsauth_srpres
		SFSAUTHPROC_SRP_INIT (sfssrp_init_arg) = 5;

		sfsauth_srpres
		SFSAUTHPROC_SRP_MORE (sfssrp_bytes) = 6;

		sfsauth_fetchres
		SFSAUTHPROC_FETCH (void) = 7;
	} = 1;
} = 344442;

#undef SFSRO_VERSION
#undef SFSRO_PROGRAM
