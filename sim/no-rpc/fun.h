#ifndef INCL_FUN
#define INCL_FUN

/* functions implemented by doc.c */
void insertDocument(Node *n, int *docId);
void insertDocumentLocal(Node *n, int *docId);
void findDocument(Node *n, int *docId);
void findDocumentLocal(Node *n, ID *docId);
void updateDocList(Node *n);
void *freeDocList(Node *n);
void printDocList(Node *n);

/* functions implemented by join.c */
void join(Node *n, int *nodeId);

/* functions implemented by join.c */
void leave(Node *n, void *dummy);
void faultyNode(Node *n, void *dummy);

/* functions implemented by stabilize.c */
void stabilize(Node *n);

/* functions implemented by misc.c */
void setFinger(Node *n, int i, int id);
void replaceFinger(Node *n, int oldId, int newId);
int fingerStart(Node *n, int i);
int successorId(int id, int i);
int predecessorId(int id, int i);
int isGreater(int a, int b, int numBits);
int isGreaterOrEqual(int a, int b, int numBits);
int between(int x, int a, int b, int numBits);

/* functions implemented by node.c */
Node *newNode(ID id);
void updateNodeState(Node *n, int id);
void initNodeHashTable();
Node *addNode(int id);
void deleteNode(Node *n);
Node *getNode(int id);
int getRandomNodeId();
void printAllNodesInfo();
void updateSuccessor(Node *n, int id);


/* functions implemented by event.c */
void genEvent(int nodeType, void (*fun)(), void *params, double time);
Event *getEvent(CalQueue *evCal, double time);
Event **initEventQueue();
void removeEvent(CalQueue *evCal, Event *ev);

/* functions implemented by in.c */
void readInputFile(char *file);

/* functions implemented by misc.c */
int *newInt(int val);

/* functions implemented by sim.c */
void exitSim(void);

/* functions implemented by finger.c */
Finger *getFinger(FingerList *fList, ID id);
void insertFinger(Node *n, ID id);
void getNeighbors(Node *n, ID x, ID *pred, ID *succ);
void printFingerList(Node *n);
ID   getSuccessor(Node *n);
ID   getPredecessor(Node *n);

/* functions implemented by request.c */
Request *newRequest(ID x, int type, int style, ID initiator);
void insertRequest(Node *n, Request *r);
Request *getRequest(Node *n);
void printReqList(Node *n);


/* functions in util.c */
int   initRand(unsigned seed);
void  initVecRand();
double funifRand(double a, double b);
double fExp(double mean);
int    unifRand(int a, int b);
int    intExp(int mean);
double fPareto(double alpha);
double fParetoMean(double mean);
double funifVecRand(double a, double b);
double fVecExp(double mean);
double fVecParetoMean(double mean);
void panic(char *str);


#endif /* INC_FUN */
