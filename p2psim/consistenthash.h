#ifndef __CONSISTENTHASH_H
#define __CONSISTENTHASH_H 

#define NBCHID 32

class ConsistentHash {
public:
  typedef unsigned CHID;

  // return if n is in (a,b) on the circle
  static bool between(CHID a, CHID b, CHID n) {
    bool r;
    if (a == b) {
      r = (n != a); // n is the only node not in the interval (n,n)
    }else if (a < b) {
      r = (n > a) && (n < b);
    }else {
      r = (n > a) || (n < b);
    }
    return r;
  };

  // return if n is in (a,b] on the circle
  static bool betweenrightincl(CHID a, CHID b, CHID n) {
    bool r;
    if ((a == b) && (n == a)) {
      r = 1;
    } else if (a < b) {
      r = (n > a) && (n <= b);
    } else {
      r = (n > a) || (n <= b);
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
