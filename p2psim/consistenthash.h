#ifndef __CONSISTENTHASH_H
#define __CONSISTENTHASH_H 

#define NBCHID 32

class ConsistentHash {
public:
  typedef unsigned CHID;

  static bool between(CHID a, CHID b, CHID n) {
    bool r;
    if (a == b) {
      r = (n!=a);
    }else if (a < b) {
      r = (n > a) && (n < b);
    }else {
      r = (n > a) || (n < b);
    }
    return r;
  };

  static CHID successorID(CHID n, int p) {
    return (n + (1 << p));
  }

  static CHID getRandID() {
    return (CHID) ((random() << 16) ^ random());
  }

  static CHID ip2chid(IPAddress ip) {
    return (CHID) ip;  // XXXX fix to use sha1
  }
};

#endif // __CONSISTENTHASH_H
