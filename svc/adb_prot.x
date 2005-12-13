%#include <chord_types.h>

enum adb_status {
  ADB_OK = 0,
  ADB_ERR = 1,
  ADB_NOTFOUND = 2,
  ADB_COMPLETE = 3
};


struct adb_storearg {
  str name;
  chordID key;
  opaque data<>;
};

struct adb_fetcharg {
  str name;
  chordID key;
};

struct adb_getkeysarg {
  str name;
  chordID start;
};

struct adb_deletearg {
  str name;
  chordID key;
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

struct adb_getkeysresok {
  chordID keys<>;
  bool complete;
};

union adb_getkeysres switch (adb_status status) {
 case ADB_ERR:
   void;
 case ADB_OK:
   adb_getkeysresok resok;
 default:
   void;
};


program ADB_PROGRAM {
	version ADB_VERSION {
		void
		ADBPROC_NULL (void) = 0;
		
		adb_status
		ADBPROC_STORE (adb_storearg) = 1;

		adb_fetchres
		ADBPROC_FETCH (adb_fetcharg) = 2;

		adb_getkeysres
		ADBPROC_GETKEYS (adb_getkeysarg) = 3;

		adb_status
		ADBPROC_DELETE (adb_deletearg) = 4;
	} = 1;
} = 344501;
