/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <assert.h>
#include <str.h>
#include <sha1.h>
#include "configurator.h"
#include "id_utils.h"
#include "misc_utils.h"

const chordID maxID ((chordID (1) << NBIT) - 1);

chordID
incID (const chordID &n)
{
  chordID s = n + 1;
  chordID b;
  b = 1;
  b <<= NBIT;
  if (s >= b)
    return s - b;
  else
    return s;
}

chordID
decID (const chordID &n)
{
  chordID p = n - 1;
  chordID b;
  b = 1;
  b <<= NBIT;
  b -= 1;
  p = p & b;
  return p;
}

chordID
successorID (const chordID &n, int p)
{
  chordID s;
  chordID t;
  chordID b;
  t = 1;
  b = 1;

  b <<= NBIT;
  b -= 1;
  s = n;
  t <<= p;
  s += t;
  s &= b;
  return s;
}

chordID
predecessorID (const chordID &n, int p)
{
  chordID s;
  chordID t;
  chordID b;
  
  t = 1;
  b = 1;

  b <<= NBIT;
  b -= 1;
  s = n;
  t <<= p;
  s -= t;
  s &= b;
  return s;
}

chordID
doubleID (const chordID &n, int logbase)
{
  chordID s = n;
  chordID b;
  b = 1;

  b <<= NBIT;
  b -= 1;
  s <<= logbase;
  s &= b;
  return s;
}

// Check whether n in (a,b) on the circle.
bool
between (const chordID &a, const chordID &b, const chordID &n)
{
  bool r;
  if (a == b) {
    r = n != a;   // n is the only node not in the interval (n,n)
  } else if (a < b) {
    r = (n > a) && (n < b);
  } else {
    r = (n > a) || (n < b);
  }
  return r;
}

// Check whether n in [a,b) on the circle.
bool
betweenleftincl (const chordID &a, const chordID &b, const chordID &n)
{
  bool r;
  if ((a == b) && (n == a)) {
    r = 1;
  } else if (a < b) {
    r = (n >= a) && (n < b);
  } else {
    r = (n >= a) || (n < b);
  }
  return r;
}

// Check whether n in (a,b] on the circle.
bool
betweenrightincl (const chordID &a, const chordID &b, const chordID &n)
{
  bool r;
  if ((a == b) && (n == a)) {
    r = 1;
  } else if (a < b) {
    r = (n > a) && (n <= b);
  } else {
    r = (n > a) || (n <= b);
  }
  return r;
}

// Check whether n in [a,b] on the circle.
bool
betweenbothincl (const chordID &a, const chordID &b, const chordID &n)
{
  bool r;
  if ((a == b) && (n == a)) {
    r = 1;
  } else if (a < b) {
    r = (n >= a) && (n <= b);
  } else {
    r = (n >= a) || (n <= b);
  }
  return r;
}
 

chordID
distance(const chordID &a, const chordID &b) 
{
  if (a < b) return (b - a);
  else {
    bigint t = bigint(1);
    t <<= NBIT;
    return (t - a) + b;
  }
}

chordID
diff (const chordID &a, const chordID &b)
{
  chordID diff = (b - a);
  if (diff > 0) return diff;
  else { 
    bigint t = bigint(1);
    t <<= NBIT;
    return t - diff;
  }
}

u_long
topbits (u_long n, const chordID &a)
{
  if (a == 0) return 0;

  assert (n <= 32);
  assert (n <= a.nbits ());
  u_long m = NBIT - a.nbits ();
  u_long r = 0;
  m = MIN (m, n);
  n = n - m;
  if (n > 0) {
    chordID x = a >> (a.nbits() - n);
    r = x.getui ();
  }
  return r;
}


chordID
shifttopbitout (u_long n, const chordID &a)
{
  if (a == 0) return a;

  chordID r = a;
  chordID mask = 1;
  mask <<= (NBIT - n);
  mask -= 1;
  r &= mask;
  r <<= n;
  //  warnx << "shifttopbit " << n << " : a(" << a.nbits() << ") " << a 
  // << " r(" << r.nbits() << ") " << r << "\n";
  return r;
}

u_long
n1bits (u_long n)
{
  u_long t = 0;
  for (int i = 0; i < 32; i++) {
    if (0x1 & n) t++;
    n = n >> 1;
  }
  return t;
}

int 
bitindexmismatch (chordID n, chordID p)
{
  int bm;

  if (n.nbits () != p.nbits ()) {
    bm = p.nbits() - 1;
  } else {
    for (bm = p.nbits () - 1; bm >= 0; bm--) {
      if (n.getbit (bm) != p.getbit (bm))
	break;
    }
  }
  return bm;
}

int
bitindexzeros (chordID p, int i, int nzero)
{
  int c = 0;
  int b0;

  for (b0 = i; b0 >= 0; b0--) {
    if (p.getbit (b0) == 0) {
      c++;
      if (c >= nzero) break;
    } else {
      c = 0;
    }
  }
  return b0;
}

chordID
createbits (chordID n, int pos, chordID x)
{
  chordID mask = 1;
  int b0x = NBIT - x.nbits ();
  mask <<= ((n.nbits () - pos));
  mask -= 1;
  mask <<= (pos + 1);
  chordID r = n & mask;
  chordID xtop = x;
  xtop >>= (x.nbits () - (pos + 1 - b0x));
  r |= xtop;
  return r;
}

u_long
log2 (u_long n)
{
  u_long l = 0;
  for (int i = 0; i < 32; i++) {
    if (0x1 & n) l = i;
    n = n >> 1;
  }
  return l;
}

bool
str2chordID (str c, chordID &newID)
{
  if (c.len () > NBIT / 4)
    return false;
  
  //_mpz_realloc( &newID, 6 );
  newID = 0;
  bigint x;
  for (unsigned int i = 0; i < c.len (); i++) {
    newID <<= 4;
    if (c[i] >= '0' && c[i] <= '9') {
      x = (c[i] - '0');
      newID |= x;
    } else if (c[i] >= 'a' && c[i] <= 'f') {
      x = (c[i] - 'a' + 10);
      newID |= x;
    } else if (c[i] >= 'A' && c[i] <= 'F') {
      x = (c[i] - 'A' + 10);
      newID |= x;
    } else {
      return false;
    }
  }
  return true;
}

chordID
make_chordID (str addr, int port, int index)
{
  chordID ID;
  // XXX we probably should marshall this!
  str ids = strbuf () << addr << "." << port << "." << index;
  char id[sha1::hashsize];
  sha1_hash (id, ids, ids.len());
  mpz_set_rawmag_be (&ID, id, sizeof (id));  // For big endian
  return ID;
}

chordID
make_chordID (const chord_node_wire &n)
{
  chord_node z = make_chord_node (n);
  return z.x;
}

chordID
make_randomID ()
{
  chordID ID;
  unsigned rand = random()>>1;
  str ids = strbuf () << rand;
  char id[sha1::hashsize];
  sha1_hash (id, ids, ids.len());
  mpz_set_rawmag_be (&ID, id, sizeof (id));  // For big endian
  return ID;
}

bool
is_authenticID (const chordID &x, chord_hostname n, int p, int vnode)
{  
  static int max_vnodes = 0;
  if (max_vnodes == 0) {
    bool ok = Configurator::only ().get_int ("chord.max_vnodes", max_vnodes);
    if (!ok) {
      // This should only be needed when libutil is not linked with libchord.
      warnx << "Using default number of max_vnodes: 1024\n";
      max_vnodes = 1024;
    }
  }

  // xxx presumably there's really a smaller actual range
  //     of valid ports.
  if (p < 0 || p > 65535)
    return false;

  if (vnode > max_vnodes)
    return false;

  chordID ID = make_chordID (n, p, vnode);
  return (ID == x);
}

void *
simple_realloc (void *memblock, size_t old_size, size_t new_size)
{
  void *newp = malloc (new_size);
  if (newp != NULL) {
    if (new_size > old_size) {
      memcpy (newp, memblock, old_size);
    } else {
      memcpy (newp, memblock, new_size);
    }
  } else {
      fprintf (stderr, "simple_realloc: Cannot reallocate memory (old_size=%u new_size=%u)\n", old_size, new_size);
      abort ();
  }

  free (memblock);
  return newp;
}
