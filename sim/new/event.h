#ifndef INCL_EVENTS
#define INCL_EVENTS


typedef struct _event {
  int           nodeId;    /* nodeId */
  double        time;
  void          (*fun)();  /* function to be called when the evnt occurs */
  void           *params;  /* address to the parameters to be passed to fun */
  struct _event *next;  /* next event in the list */
} Event;

typedef struct _calQueue {
  int     size;   /* size of the calendar queueue */
  double  time;   /* current time */
  Event  **q;
} CalQueue;

#endif
