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

#include <stdio.h>
#include <stdlib.h>
#include "incl.h"

extern Node *NodeHashTable[HASH_SIZE];

int insertDocInList(DocList *docList, Document *doc);
Document *findDocInList(DocList *docList, ID docId);
void removeDocFromList(DocList *docList, ID docId);

DocList PendingDocs = {NULL, 0};

Document *newDoc(int docId)
{
  Document *doc;

  if ((doc = (Document *)calloc(1, sizeof(Document))) == NULL)
    panic("newDoc: memory allocation error!\n");
  
  doc->id = docId;
  
  return doc;
}


// invoke a document lookup operation for docId at node n
void findDocument(Node *n, ID *docId)
{ 
  Request *r;

  if (n->status != PRESENT) {
    printf("findDocument: n=%d has not joined or has been deleted in the meantime\n", n->id);
    return;
  }
  r = newRequest(*docId, REQ_TYPE_FINDDOC, REQ_STYLE_ITERATIVE, n->id);

  insertRequest(n, r);
}


// check whether document docId is stored at node n
void findDocumentLocal(Node *n, ID *docId)
{
  if (n->status != PRESENT) {
    printf("findDocumentLocal: node %d has not joined or has been deleted in the meantime!\n", 
	   n->id);
    return;
  }

  if (findDocInList(n->docList, *docId)) {
    static int i = 0;
    printf("%f Document %d found on node %d (%d)\n", 
	   Clock, *docId, n->id, i++);
  } else {
    static int i1 = 0, i2 = 0;
    // differentiate between failures because douments
    // were never inserted, and failures due to 
    // lookup and node failures
    if (findDocInList(&PendingDocs, *docId)) 
      // this document was never inserted because the insertion failed;
      // thus this shouldn't be caunted as a lookup failure
      printf("%f Document %d not found on node %d (%d)\n", 
	     Clock, *docId, n->id, i1++);
    else {
      // this is an actual lookup failure
      printf("%f Document %d NOT found on node %d (%d)\n", 
	     Clock, *docId, n->id, i2++);
    }
  }
}

// invoke the insertion of document docId at node n 
void insertDocument(Node *n, ID *docId)
{
  Request *r;

  if (n->status != PRESENT) {
    printf("insertDocument: n=%d has not joined or has been deleted in the meantime\n", n->id);
    insertDocInList(&PendingDocs, newDoc(*docId));
    return;
  }
  r = newRequest(*docId, REQ_TYPE_INSERTDOC, REQ_STYLE_ITERATIVE, n->id);

  insertRequest(n, r);

  // keep the list of documents not stored yet.
  // if the insertion is succesful the document is removed 
  // from the list 
  insertDocInList(&PendingDocs, newDoc(*docId));
}
  

// insert document docId locally at node n */
void insertDocumentLocal(Node *n, ID *docId)
{
  Document *doc;

  if (n->status != PRESENT) {
    printf("insertReply: node has not joined or has been deleted in the meantime!\n");
  } else {
    // allocate space for new document 
    doc = newDoc(*docId);

    if (!insertDocInList(n->docList, doc)) {
      // no room to insert document 
      printf("cannot insert document %x at node %x\n", doc->id, n->id); 
      free(doc);
      return;
    } else {
      printf("The document %d inserted on node %d at %f\n", 
	     *docId, n->id, Clock);
      removeDocFromList(&PendingDocs, *docId);
    }
  }
}


// insert specified document at the head of document list. 
// return TRUE if document successfully inserted, and FALSE otherwise.
// if document already in the list, do nothing and return TRUE
int insertDocInList(DocList *docList, Document *doc)
{
  Document *tmp;

  if (docList->size >= MAX_NUM_DOCS) {
    /* no room available in document list */
    return FALSE;
  }

  /* insert document at the head of the list */
  tmp = docList->head;
  docList->head = doc;
  doc->next = tmp;

  return TRUE;
}


// search for document docId in docList
Document *findDocInList(DocList *docList, ID docId)
{
  Document *doc;

  for (doc = docList->head; doc; doc = doc->next)
    if (doc->id == docId)
      return doc;

  return NULL;
}


// remove document docId (if any) from docList
void removeDocFromList(DocList *docList, ID docId)
{
  Document *d, *temp;

  if (!docList->head)
    return;

  if (docList->head->id == docId) {
    temp = docList->head;
    docList->head = temp->next;
    free(temp);
    return;
  }

  for (d = docList->head; d->next; d = d->next) {
    if (d->next->id == docId) {
      temp = d->next;
      d->next = temp->next;
      free(temp);
      return;
    }
  }
}


void *freeDocList(Node *n)
{
  Document *doc, *tmp;

  while ((doc = n->docList->head)) {
    tmp = n->docList->head;
    n->docList->head = n->docList->head->next;
    free(tmp);
  }

  return NULL;
}
  


// move documents from n's successor to n 
void updateDocList(Node *n, Node *s)
{
  Document *doc, *d;

  if (!n || !s)
    return;
  if (n == s || (n->status != PRESENT))
    return;

  if (!s) {
    printf("updateDocList: node has been deleted in the meantime!\n");
    return;
  }

  // move all documents x stored on s, and that are not between n and s,
  // to n, i.e., n is now closest to documents x than s
  doc = s->docList->head;
  while (doc) {
    // check whether doc is  [n, s) ... 
    if (!between(doc->id, n->id, s->id) && doc->id != s->id) {
      // ... if not, move document from s' document list to n's 
      // document list 
      s->docList->head = doc->next;
      insertDocInList(n->docList, doc);
    } else
      break;
    doc = s->docList->head;
  }
  
  if (!(d = doc))
    return;

  while ((doc = d->next)) {
    if (!between(doc->id, n->id, s->id) && doc->id != s->id) {
      // move document from s' document list to n's document list 
      d->next = d->next->next;
      insertDocInList(n->docList, doc);
    } else
      d = d->next;
  }
}



// move documents from n1 to n2 
void moveDocList(Node *n1, Node *n2)
{
  Document *doc;

  if (n1 == n2)
    return;

  if (!n1) {
    printf("moveDocList: node has been deleted in the meantime!\n");
    return;
  }

  // move all documents x stored on s, and that are not between n and s,
  // to n, i.e., n is now closest to documents x than s
  doc = n1->docList->head;

  while (doc) {
    n1->docList->head = doc->next;
    insertDocInList(n2->docList, doc);
    doc = n1->docList->head;
  }
}


void printDocList(Node *n)
{
  Document *doc;

  printf("   doc list: ");
  for (doc = n->docList->head; doc; doc = doc->next) {
    printf("%d", doc->id);
    if (doc->next)
      printf(", ");
  }
  printf("\n");
}


// print list of documents that have not been inserted so far
// (i.e., at the end of the simulation this list contains mostly 
//  documents whose insertion has failed)
void printPendingDocs()
{
  Document *doc;

  printf("Pending documents: ");

  for (doc = (&PendingDocs)->head; doc; doc = doc->next) {
    printf("%d", doc->id);
    if (doc->next)
      printf(", ");
  }
  printf("\n");
}
               
