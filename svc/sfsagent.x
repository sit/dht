/* $Id: sfsagent.x,v 1.1 2001/01/18 16:21:51 fdabek Exp $ */

/*
 * This file was written by David Mazieres and Michael Kaminsky.  Its
 * contents is uncopyrighted and in the public domain.  Of course,
 * standards of academic honesty nonetheless prevent anyone in
 * research from falsely claiming credit for this work.
 */

%#include "sfs_prot.h"

typedef string sfs_filename<255>;

struct sfsagent_authinit_arg {
  int ntries;
  string requestor<>;
  sfs_authinfo authinfo;
  sfs_seqno seqno;
};

struct sfsagent_authmore_arg {
  sfs_authinfo authinfo;
  sfs_seqno seqno;
  opaque challenge<>;
};

union sfsagent_auth_res switch (bool authenticate) {
case TRUE:
  opaque certificate<>;
case FALSE:
  void;
};

typedef string sfsagent_path<1024>;
union sfsagent_lookup_res switch (bool makelink) {
 case TRUE:
   sfsagent_path path;
 case FALSE:
   void;
};

enum sfs_revocation_type {
  REVOCATION_NONE = 0,
  REVOCATION_BLOCK = 1,
  REVOCATION_CERT = 2
};
union sfsagent_revoked_res switch (sfs_revocation_type type) {
 case REVOCATION_NONE:
   void;
 case REVOCATION_BLOCK:
   void;
 case REVOCATION_CERT:
   sfs_pathrevoke cert;
};
	
struct sfsagent_symlink_arg {
  sfs_filename name;
  sfsagent_path contents;
};

typedef opaque sfsagent_seed[48];
const sfs_badgid = -1;


typedef string sfsagent_comment<1023>;
struct sfs_addkey_arg {
  bigint p;
  bigint q;
  sfs_time expire;
  sfsagent_comment comment;
};

enum sfs_remkey_type {
  SFS_REM_PUBKEY,
  SFS_REM_COMMENT
};
union sfs_remkey_arg switch (sfs_remkey_type type) {
 case SFS_REM_PUBKEY:
   sfs_pubkey pubkey;
 case SFS_REM_COMMENT:
   sfsagent_comment comment;
};

struct sfs_keylistelm {
  bigint key;
  sfs_time expire;
  sfsagent_comment comment;
  sfs_keylistelm *next;
};
typedef sfs_keylistelm *sfs_keylist;

typedef string sfsagent_progarg<>;
typedef sfsagent_progarg sfsagent_cmd<>;

struct sfsagent_certprog {
  string suffix<>;		/* Suffix to be removed from file names */
  string filter<>;		/* Regular expression filter on prefix */
  string exclude<>;		/* Regular expression filter on prefix */
  sfsagent_cmd av;		/* External program to run */
};

typedef sfsagent_certprog sfsagent_certprogs<>;

struct sfsagent_blockfilter {
  string filter<>;		/* Regular expression filter on hostname */
  string exclude<>;		/* Regular expression filter on hostname */
};
struct sfsagent_revokeprog {
  sfsagent_blockfilter *block;	/* Block hostid even without revocation cert */
  sfsagent_cmd av;		/* External program to run */
};

typedef sfsagent_revokeprog sfsagent_revokeprogs<>;

typedef sfs_hash sfsagent_norevoke_list<>;

struct sfsagent_rex_resok {
  sfs_kmsg kcs;
  sfs_kmsg ksc;
  sfs_seqno seqno;
};

union sfsagent_rex_res switch (bool status) {
 case TRUE:
   sfsagent_rex_resok resok;
 case FALSE:
   void;
};

struct sfsctl_getfh_arg {
  filename3 filesys;
  u_int64_t fileid;
};

union sfsctl_getfh_res switch (nfsstat3 status) {
 case NFS3_OK:
   nfs_fh3 fh;
 default:
   void;
};

struct sfsctl_getidnames_arg {
  filename3 filesys;
  sfs_idnums nums;
};

union sfsctl_getidnames_res switch (nfsstat3 status) {
 case NFS3_OK:
   sfs_idnames names;
 default:
   void;
};

struct sfsctl_getidnums_arg {
  filename3 filesys;
  sfs_idnames names;
};

union sfsctl_getidnums_res switch (nfsstat3 status) {
 case NFS3_OK:
   sfs_idnums nums;
 default:
   void;
};

union sfsctl_getcred_res switch (nfsstat3 status) {
 case NFS3_OK:
   sfsauth_cred cred;
 default:
   void;
};

struct sfsctl_lookup_arg {
  filename3 filesys;
  diropargs3 arg;
};

program AGENTCTL_PROG {
	version AGENTCTL_VERS {
		void
		AGENTCTL_NULL (void) = 0;

		bool
		AGENTCTL_ADDKEY (sfs_addkey_arg) = 1;

		bool
		AGENTCTL_REMKEY (sfs_remkey_arg) = 2;

		void
		AGENTCTL_REMALLKEYS (void) = 3;

		sfs_keylist
		AGENTCTL_DUMPKEYS (void) = 4;

		void
		AGENTCTL_CLRCERTPROGS (void) = 5;

		bool
		AGENTCTL_ADDCERTPROG (sfsagent_certprog) = 6;

		sfsagent_certprogs
		AGENTCTL_DUMPCERTPROGS (void) = 7;

		void
		AGENTCTL_CLRREVOKEPROGS (void) = 8;

		bool
		AGENTCTL_ADDREVOKEPROG (sfsagent_revokeprog) = 9;

		sfsagent_revokeprogs
		AGENTCTL_DUMPREVOKEPROGS (void) = 10;

		void
		AGENTCTL_SETNOREVOKE (sfsagent_norevoke_list) = 11;

		sfsagent_norevoke_list
		AGENTCTL_GETNOREVOKE (void) = 12;

		void
		AGENTCTL_SYMLINK (sfsagent_symlink_arg) = 13;

		void
		AGENTCTL_RESET (void) = 14;

		int
		AGENTCTL_FORWARD (sfs_hostname) = 15;

		void
		AGENTCTL_RNDSEED (sfsagent_seed) = 16;

		sfsagent_rex_res
		AGENTCTL_REX (sfs_hostname) = 17;
	} = 1;
} = 344428;

program SETUID_PROG {
	version SETUID_VERS {
		/* Note:  SETUIDPROC_SETUID requires an authunix AUTH. */
		int SETUIDPROC_SETUID (void) = 0;
	} = 1;
} = 344430;

program AGENT_PROG {
	version AGENT_VERS {
		void
		AGENT_NULL (void) = 0;

		int
		AGENT_START (void) = 1;

		int
		AGENT_KILL (void) = 2;

		int
		AGENT_KILLSTART (void) = 3;

		void
		AGENT_SYMLINK (sfsagent_symlink_arg) = 4;

		void
		AGENT_FLUSHNAME (sfs_filename) = 5;

		void
		AGENT_FLUSHNEG (void) = 6;

		void
		AGENT_REVOKE (sfs_pathrevoke) = 7;

		sfsagent_seed
		AGENT_RNDSEED (void) = 8;

		unsigned
		AGENT_AIDALLOC (void) = 9;

		int
		AGENT_GETAGENT (void) = 10;
	} = 1;
} = 344432;

program AGENTCB_PROG {
	version AGENTCB_VERS {
		void
		AGENTCB_NULL (void) = 0;

		sfsagent_auth_res
		AGENTCB_AUTHINIT (sfsagent_authinit_arg) = 1;

		sfsagent_auth_res
		AGENTCB_AUTHMORE (sfsagent_authmore_arg) = 2;

		sfsagent_lookup_res
		AGENTCB_LOOKUP (sfs_filename) = 3;

		sfsagent_revoked_res
		AGENTCB_REVOKED (filename3) = 4;

		void
		AGENTCB_CLONE (void) = 5;
	} = 1;
} = 344433;

program SFSCTL_PROG {
	version SFSCTL_VERS {
		void
		SFSCTL_NULL (void) = 0;

		void
		SFSCTL_SETPID (int) = 1;

		sfsctl_getfh_res
		SFSCTL_GETFH (sfsctl_getfh_arg) = 2;

		sfsctl_getidnames_res
		SFSCTL_GETIDNAMES (sfsctl_getidnames_arg) = 3;

		sfsctl_getidnums_res
		SFSCTL_GETIDNUMS (sfsctl_getidnums_arg) = 4;

		sfsctl_getcred_res
		SFSCTL_GETCRED (filename3) = 5;

		lookup3res
		SFSCTL_LOOKUP (sfsctl_lookup_arg) = 6;
	} = 1;
} = 344434;
