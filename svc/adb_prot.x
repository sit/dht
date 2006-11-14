%#include <chord_types.h>

/* {{{ ADB types */
enum adb_status {
  ADB_OK = 0,
  ADB_ERR = 1,
  ADB_NOTFOUND = 2,
  ADB_COMPLETE = 3
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

struct adb_bsinfo_t {
  adb_vnodeid n;
  u_int32_t auxdata;
};

struct adb_bsloc_t {
  chordID block;
  adb_bsinfo_t hosts<>;
};

/* Used for storing to database */
struct adb_vbsinfo_t {
  adb_bsinfo_t d<>;
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
/* {{{ ADBPROC_GETKEYS (and ADBPROC_GETKEYSON) */
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
/* {{{ ADBPROC_GETBLOCKRANGE */
struct adb_getblockrangearg {
  str name;
  chordID start;
  chordID stop;
  int extant;
  int count;
};
struct adb_getblockrangeres {
  adb_status status;
  adb_bsloc_t blocks<>;
};
/* }}} */
/* {{{ ADBPROC_GETKEYSON (arg only) */
struct adb_getkeysonarg {
  str name;
  adb_vnodeid who;
  chordID start;
  chordID stop;
};
/* }}} */
/* {{{ ADBPROC_UPDATE */
struct adb_updatearg {
  str name;
  chordID key;
  adb_bsinfo_t bsinfo;
  bool present;
};
/* }}} */
/* {{{ ADBPROC_UPDATE_BATCH */
struct adb_updatebatcharg {
  adb_updatearg args<>;
};
/* }}} */
/* {{{ ADBPROC_GETINFO */
struct adb_getinfoarg {
  str name;
  chordID key;
};
struct adb_getinfores {
  adb_status status;
  adb_vnodeid nlist<>;
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

		adb_getblockrangeres
		ADBPROC_GETBLOCKRANGE (adb_getblockrangearg) = 6;
		/* Return count chordIDs of blocks between start and stop that have extant extant fragments */
		/* If count == -1, return as many as you want. */
		/* If extant == -1, return all blocks */
		/* XXX how to deal with live/dead nodes? */

		adb_getkeysres
		ADBPROC_GETKEYSON (adb_getkeysonarg) = 7;

		adb_status
		ADBPROC_UPDATE (adb_updatearg) = 8;
		/* Update b to be present on n */

		adb_status
		ADBPROC_UPDATEBATCH (adb_updatebatcharg) = 9;
		/* Update b<> to be present on n<> */

		adb_getinfores
		ADBPROC_GETINFO (adb_getinfoarg) = 10;
		/* Get list of nodes that have b */
	} = 1;
} = 344501;

/* vim:set foldmethod=marker: */
