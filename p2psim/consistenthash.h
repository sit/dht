#ifndef __CONSISTENTHASH_H
#define __CONSISTENTHASH_H 

typedef unsigned CHID;

class ConsistentHash {
  public:
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

  static CHID getRandID() {
    return (CHID) rand();
  }
};

#endif
