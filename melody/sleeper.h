#ifndef SLEEPER
#define SLEEPER

class sleeper {
 public:
  tailq_entry <sleeper> sleep_link2;
  virtual void readcb_wakeup() = 0;
};

#endif
