#include <chord.h>

#ifndef _CHORD_UTIL_H
#define _CHORD_UTIL_H

 str gettime();
 int uniform_random(double a, double b);
 sfs_ID incID (sfs_ID &n);
 sfs_ID decID (sfs_ID &n);
 sfs_ID successorID (sfs_ID &n, int p);
 sfs_ID predecessorID (sfs_ID &n, int p);
 bool gt_or_eq (sfs_ID &n, sfs_ID &n1);
 bool gt (sfs_ID &n, sfs_ID &n1);
 bool between (sfs_ID &a, sfs_ID &b, sfs_ID &n);

#endif
