/* $Id: condvar.h,v 1.1 2003/09/29 23:08:28 strib Exp $ */

#ifndef __CONDVAR_H
#define __CONDVAR_H

#include <lib9.h>
#include <thread.h>
#include "p2psim.h"

class ConditionVar {
public:
  ConditionVar();
  ~ConditionVar();

  void wait();
  void notify();

private:
  vector<Channel *> *_waiters;
};

#endif // __CONDVAR_H
