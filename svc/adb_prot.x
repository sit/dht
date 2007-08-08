%#include <chord_types.h>

/* {{{ ADB types */
enum adb_status {
  ADB_OK = 0,
  ADB_ERR = 1,
  ADB_NOTFOUND = 2,
  ADB_COMPLETE = 3,
  ADB_DISKFULL = 4
};

struct adb_vnodeid {
  /* store everything in machine byte order, because xdr will
   * translate them into byte order */
  u_int32_t machine_order_ipv4_addr;
  u_int32_t machine_order_port_vnnum; /* (port << 16) | vnnum */
};

struct adb_keyaux_t {
  chordID key;
  u_int32_t auxdata;
};

struct adb_master_metadata_t {
  u_int64_t size;
  u_int32_t expiration;
};

struct adb_metadata_t {
  u_int32_t size;       /* Object size in bytes */
  u_int32_t expiration; /* Seconds since epoch */
  u_int32_t auxdata;	/* Optional: for distinguishing versions */
  u_int32_t offset;	/* Offset in per-expiration file */
};
/* }}} */

/* {{{ ADBPROC_INITSPACE */
struct adb_initspacearg {
  str name;
  bool hasaux;
};
/* }}} */
/* {{{ ADBPROC_STORE */
struct adb_storearg {
  str name;
  chordID key;
  opaque data<>;
  u_int32_t auxdata;
  u_int32_t expiration;
};
/* }}} */
/* {{{ ADBPROC_FETCH */
struct adb_fetcharg {
  str name;
  chordID key;
  bool nextkey;
};
struct adb_fetchresok {
  chordID key;
  opaque data<>;
  u_int32_t expiration;
};
union adb_fetchres switch (adb_status status) {
 case ADB_NOTFOUND:
   void;
 case ADB_OK:
   adb_fetchresok resok;
 default:
   void;
};
/* }}} */
/* {{{ ADBPROC_DELETE */
struct adb_deletearg {
  str name;
  chordID key;
  u_int32_t auxdata;
};
/* }}} */
/* {{{ ADBPROC_GETKEYS */
struct adb_getkeysarg {
  str name;
  bool getaux;
  bool ordered;
  u_int32_t batchsize;
  chordID continuation;
};
struct adb_getkeysresok {
  adb_keyaux_t keyaux<>;
  bool hasaux;
  bool ordered;
  bool complete;
  chordID continuation;
};
union adb_getkeysres switch (adb_status status) {
 case ADB_ERR:
   void;
 case ADB_OK:
   adb_getkeysresok resok;
 default:
   void;
};
/* }}} */
/* {{{ ADBPROC_GETSPACEINFO */
struct adb_dbnamearg {
  str name;
};
struct adb_getspaceinfores {
  adb_status status;
  str fullpath; /* Full path to local database */
  bool hasaux;
};
/* }}} */
/* {{{ ADBPROC_EXPIRE */
struct adb_expirearg {
  str name;
  u_int32_t limit;
  u_int32_t deadline;
};
/* }}} */

program ADB_PROGRAM {
	version ADB_VERSION {
		void
		ADBPROC_NULL (void) = 0;

		adb_status
		ADBPROC_INITSPACE (adb_initspacearg) = 1;
		
		adb_status
		ADBPROC_STORE (adb_storearg) = 2;

		adb_fetchres
		ADBPROC_FETCH (adb_fetcharg) = 3;

		/* This RPC is designed for iterating the database.
		 * It does not wrap around after the highest ID. */
		adb_getkeysres
		ADBPROC_GETKEYS (adb_getkeysarg) = 4;

		adb_status
		ADBPROC_DELETE (adb_deletearg) = 5;

		adb_getspaceinfores
		ADBPROC_GETSPACEINFO (adb_dbnamearg) = 11;

		adb_status
		ADBPROC_SYNC (adb_dbnamearg) = 12;
		/* Sync databases etc to disk */

		adb_status
		ADBPROC_EXPIRE (adb_expirearg) = 13;
		/* May expire some objects */
	} = 1;
} = 344501;

/* vim:set foldmethod=marker: */
