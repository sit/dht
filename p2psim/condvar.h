/* $Id: condvar.h,v 1.2 2003/09/30 19:01:31 strib Exp $ */

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
  void notifyAll();

private:
  vector<Channel *> *_waiters;
};

#endif // __CONDVAR_H
