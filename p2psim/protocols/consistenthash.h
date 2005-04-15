/*
 * Copyright (c) 2003-2005 Jinyang Li
 *                    Thomer M. Gil (thomer@csail.mit.edu)
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __CONSISTENTHASH_H
#define __CONSISTENTHASH_H 

#define NBCHID (sizeof(ConsistentHash::CHID)*8)

#include <openssl/sha.h>
#include <assert.h>
#include "../p2psim/parse.h"

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

  // return if n is in [a,b) on the circle
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

  static CHID ipname2chid(const char *name) {
    unsigned char *buf = SHA1 ((const unsigned char *)name, strlen(name), NULL);
    CHID r = 0;
    for (int i = 0; i < 8; i++) {
      r += (CHID) buf[i];
      if (i < 7) r <<= 8;
    }
    // memcpy(&r, buf, 8);
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

  // get n bits from x starting at pos p
  static uint getbit (CHID x, uint p, uint n = 1) {
    assert (p < NBCHID);
    assert (n < 32);
    assert (n >= 1);
    uint m = (0x1 << n) - 1;
    uint r = ((x >> p) & m);
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

  static CHID log_b(CHID n, uint base) {
    unsigned int i = 0;
    while (n > 0) {
      n = n / base;
      i++;
    }
    return i - 1;
  };

  static CHID distance (CHID a, CHID b)
  {
    if (a < b) 
      return (b - a);
    else if (a == b) 
      return 0;
    else {
      CHID t;
      memset (&t, 255,sizeof(CHID));
      return (t - a) + b;
    }
  }
};


// XXX: this is the wrong ifdef
#ifdef HAVE_EXT_HASH_MAP
namespace __gnu_cxx {
#else
namespace std {
#endif
  // silently assume ConsistentHash::CHID is a 64-bit long long
  template<> struct hash<ConsistentHash::CHID> {
    size_t operator()(const ConsistentHash::CHID &x) const {
      unsigned lh = (unsigned) (x & 0x00000000ffffffff);
      unsigned uh = (unsigned) (x >> 32);
      return hash<unsigned>()(lh ^ uh);
    }
  };
}

#endif // __CONSISTENTHASH_H
