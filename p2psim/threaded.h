#ifndef __THREADED_H
#define __THREADED_H

#include <lib9.h>
#include <thread.h>

class Threaded {
public:
  Threaded();
  virtual ~Threaded();
  static void Run(void*);
  Channel *exitchan() { return _exitchan; }

protected:
  void thread();
  Channel *_exitchan;
  int _thread;

private:
  virtual void run() = 0;
};

#endif // __THREADED_H
