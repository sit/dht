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

#include <chord.h>

#ifndef _CHORD_UTIL_H
#define _CHORD_UTIL_H

 str gettime ();
 u_int64_t getusec ();
 u_int32_t uniform_random(double a, double b);
 chordID incID (chordID &n);
 chordID decID (chordID &n);
 chordID successorID (chordID &n, int p);
 chordID predecessorID (chordID &n, int p);
 bool between (chordID &a, chordID &b, chordID &n);
 bool betweenleftincl (chordID &a, chordID &b, chordID &n);
 bool betweenrightincl (chordID &a, chordID &b, chordID &n);
 chordID diff(chordID a, chordID b);
 chordID distance(chordID a, chordID b);
 u_long topbits (int n, chordID a);
 u_long n1bits (u_long n);
 u_long log2 (u_long n);
 sfs_hostname my_addr ();
 chordID init_chordID (int index, int p);
 chordID make_chordID (str hostname, int port, int index = 0);
 bool is_authenticID (chordID &x, sfs_hostname n, int p, int vnode);

 void warnt(char *msg);
#endif /* _CHORD_UTIL_H */
