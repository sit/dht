/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
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
 void warnt(char *msg);
#endif /* _CHORD_UTIL_H */
