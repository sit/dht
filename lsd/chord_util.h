#include <chord.h>

#ifndef _CHORD_UTIL_H
#define _CHORD_UTIL_H

 str gettime();
 int uniform_random(double a, double b);
 sfs_ID incID (sfs_ID &n);
 sfs_ID decID (sfs_ID &n);
 sfs_ID successorID (sfs_ID &n, int p);
 sfs_ID predecessorID (sfs_ID &n, int p);
 bool between (sfs_ID &a, sfs_ID &b, sfs_ID &n);
 bool betweenleftincl (sfs_ID &a, sfs_ID &b, sfs_ID &n);
 bool betweenrightincl (sfs_ID &a, sfs_ID &b, sfs_ID &n);
 sfs_ID diff(sfs_ID a, sfs_ID b);
 void warnt(char *msg);
#endif /* _CHORD_UTIL_H */
