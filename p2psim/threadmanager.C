#include "threadmanager.h"

ThreadManager *ThreadManager::_instance = 0;


ThreadManager*
ThreadManager::Instance()
{
  if(!_instance)
    _instance = new ThreadManager();
  return _instance;
}

ThreadManager::ThreadManager()
{
}

ThreadManager::~ThreadManager()
{
}

int
ThreadManager::create(void (*fn)(void*), void *args, int ss)
{
  int tid = ::threadcreate(fn, args, 3 * ss);
  return tid;
}
