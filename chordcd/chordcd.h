#include "arpc.h"
#include "sfsclient.h"
#include "bigint.h"
#include "chord.h"

class chord_server  {
  
  ptr<aclnt> lsdclnt;
  bigint rootfh;

  void getdata (chordID ID, str data);

 public:
  void dispatch (nfscall *sbp);
  bool setrootfh (str root);

  chord_server () {};
};
