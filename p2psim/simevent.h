#ifndef __SIMEVENT_H
#define __SIMEVENT_H

class SimEvent : public Event {
public:
  SimEvent();
  SimEvent(vector<string>*);
  ~SimEvent();

  virtual void execute();

private:
  string _op;
};

#endif // __SIMEVENT_H
