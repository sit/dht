#ifndef __CONSISTENTHASH_H
#define __CONSISTENTHASH_H 

#define NBCHID (sizeof(CHID)*8)

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
    } else if (a <= b) {
      r = (n > a) && (n <= b);
     } else {
      r = (n > a) || (n <= b);
    }
    return r;
  };

  // return if n is in (a,b] on the circle
  static bool betweenleftincl(CHID a, CHID b, CHID n) {
    bool r;
    if ((a == b) && (n == a)) {
      r = 1;
    } else if (a <= b) {
      r = (n >= a) && (n < b);
     } else {
      r = (n >= a) || (n < b);
    }
    return r;
  };

  static CHID successorID(CHID n, int p) {
    CHID one = 1;
    return (n + (one << p));
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
 
  // return last bit pos in which n and m match.  if n==m, then pos is 0.
  // if n != m, then pos is NBCHID.
  static uint bitposmatch (CHID n, CHID m) {
    uint i;
    for (i = NBCHID - 1; i >= 0; i--) {
      if (getbit (n, i) != getbit (m, i)) {
	break;
      }
    }
    return i + 1;
  }

  static uint getbit (CHID n, uint p) {
    assert (p < NBCHID);
    uint r = ((n >> p) & 0x1);
    return r;
  };

  static CHID setbit (CHID n, uint p, uint v) {
    assert (p < NBCHID);
    CHID mask = ((CHID) 1) << p;
    mask = ~mask;
    CHID r = n & mask;
    CHID val = ((CHID) v) << p;
    r = r | val;
    return r;
  };

};

#endif // __CONSISTENTHASH_H
