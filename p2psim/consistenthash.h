#ifndef __CONSISTENTHASH_H
#define __CONSISTENTHASH_H 

#define NBCHID sizeof(CHID)

#include <openssl/sha.h>

class ConsistentHash {
public:
  typedef unsigned long long CHID;

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
    CHID r = random();
    r = (r << 32) | random ();
    return r;
  }

  static CHID ip2chid(IPAddress ip) {
    CHID r;
    unsigned char *buf = SHA1 ((const unsigned char *) &ip, sizeof(ip), NULL);
    memcpy (&r, buf, 8);
    return r;
  }
};

#endif // __CONSISTENTHASH_H
