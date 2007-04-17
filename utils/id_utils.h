#ifndef _ID_UTILS_H_
#define _ID_UTILS_H_
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

#include <chord_types.h>

// the identifier for the ihash class
struct hashID {
  hashID () {}
  hash_t operator() (const chordID &ID) const {
    return ID.getui ();
  }
};

extern const chordID maxID;

chordID incID (const chordID &n);
chordID decID (const chordID &n);
chordID successorID (const chordID &n, int p);
chordID predecessorID (const chordID &n, int p);
chordID doubleID (const chordID &n, int LOGBASE);
bool between          (const chordID &a, const chordID &b, const chordID &n);
bool betweenleftincl  (const chordID &a, const chordID &b, const chordID &n);
bool betweenrightincl (const chordID &a, const chordID &b, const chordID &n);
bool betweenbothincl (const chordID &a, const chordID &b, const chordID &n);
chordID diff(const chordID &a, const chordID &b);
chordID distance(const chordID &a, const chordID &b);
u_long topbits (u_long n, const chordID &a);
chordID shifttopbitout (u_long n, const chordID &a);
u_long n1bits (u_long n);
chordID createbits (chordID n, int b0, chordID x);
int bitindexzeros (chordID p, int bm, int b0);
int bitindexmismatch (chordID n, chordID p);
u_long log2 (u_long n);

bool str2chordID (str c, chordID &newID);

chordID make_chordID (str hostname, int port, int index = 0);
chordID make_chordID (const chord_node_wire &n);
chordID make_randomID ();
bool is_authenticID (const chordID &x, chord_hostname n, int p, int vnode);

void *simple_realloc( void *memblock, size_t old_size, size_t new_size );

#endif /* _ID_UTILS_H */
