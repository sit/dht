#ifndef __THREADMANAGER_H
#define __THREADMANAGER_H

#include "threaded.h"
#include "p2psim.h"

class ThreadManager {
public:
  static ThreadManager* Instance();
  ~ThreadManager();

  int create(void (*)(void*), void*, int ss = DEFAULT_THREAD_STACKSIZE);

private:
  ThreadManager();

  static ThreadManager* _instance;
};


#endif // __THREADMANAGER_H
