/*
 *
 * Copyright (C) 2001 Ion Stoica (istoica@cs.berkeley.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#ifndef INCL_FUN
#define INCL_FUN

/* functions implemented by doc.c */
void insertDocument(Node *n, int *docId);
void insertDocumentLocal(Node *n, int *docId);
void findDocument(Node *n, int *docId);
void findDocumentLocal(Node *n, ID *docId);
void updateDocList(Node *n, Node *s);
void moveDocList(Node *n1, Node *n2);
void getDocList(Node *n);
void *freeDocList(Node *n);
void printDocList(Node *n);
void printPendingDocs();

/* functions implemented by leave.c */
void join(Node *n, int *nodeId);

/* functions implemented by join.c */
void leave(Node *n, void *dummy);
void nodeFailure(Node *n, void *dummy);
void join1(ID id, ID succ);

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
int between(int x, int a, int b);

/* functions implemented by node.c */
Node *newNode(ID id);
void initNodeHashTable();
Node *addNode(int id);
void deleteNode(Node *n);
Node *getNode(int id);
int getRandomActiveNodeId();
int getRandomNodeId();
void printAllNodesInfo();
void updateSuccessor(Node *n, int id);
void netFailure(Node *n, int *percentage);
ID popNode(Request *r);
void pushNode(Request *r, ID nid);



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
void removeFinger(FingerList *fList, Finger *f);

/* functions implemented by request.c */
Request *newRequest(ID x, int type, int style, ID initiator);
void insertRequest(Node *n, Request *r);
Request *getRequest(Node *n);
void printReqList(Node *n);
void copySuccessorFingers(Node *n);
void processRequest(Node *n);

/* functions in util.c */
void  initRand(unsigned seed);
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
