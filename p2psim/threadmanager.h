#ifndef __THREADMANAGER_H
#define __THREADMANAGER_H

#include "threaded.h"

class ThreadManager {
public:
  static ThreadManager* Instance();
  ~ThreadManager();

  int create(void (*)(void*), void*, int ss = 4096);

private:
  ThreadManager();

  static ThreadManager* _instance;
};


#endif // __THREADMANAGER_H
