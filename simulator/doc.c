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
    printf("document %d not found at time %f:  initiator node %d not present\n", 
	   *docId, Clock, n->id);
    return;
  }
  r = newRequest(*docId, REQ_TYPE_FINDDOC, REQ_STYLE_ITERATIVE, n->id);

  insertRequest(n, r);
}


// check whether document docId is stored at node n
void findDocumentLocal(Node *n, ID *docId)
{
  if (n->status != PRESENT) {
    printf("document %d not found at node %d at time %f: node not present \n", 
	   *docId, n->id, Clock);
    return;
  }

  if (findDocInList(n->docList, *docId)) 
    printf("document %d found at node %d at time %f\n", 
	   *docId, n->id, Clock);
  else {
    // differentiate between failures because douments
    // were never inserted, and failures due to 
    // lookup and node failures
    if (findDocInList(&PendingDocs, *docId)) 
      // this document was never inserted because the insertion failed;
      // thus this shouldn't be counted as a lookup failure
      printf("document %d not present in system at time %f\n", 
	     *docId, Clock);
    else {
      // this is an actual lookup failure
      printf("document %d not found at node %d at time %f: document not stored at the node\n", 
	     *docId, n->id, Clock);
    }
  }
}

// invoke the insertion of document docId at node n 
void insertDocument(Node *n, ID *docId)
{
  Request *r;

  if (n->status != PRESENT) {
    printf("document %d not inserted at time %f: initiator node %d not present\n", 
	   *docId, Clock, n->id);
    insertDocInList(&PendingDocs, newDoc(*docId));
    free(docId); // MW: adding
    return;
  }
  r = newRequest(*docId, REQ_TYPE_INSERTDOC, REQ_STYLE_ITERATIVE, n->id);

  insertRequest(n, r);

  // keep the list of documents not stored yet.
  // if the insertion is succesful the document is removed 
  // from the list 
  insertDocInList(&PendingDocs, newDoc(*docId));

  free(docId); // MW: adding
}
  

// insert document docId locally at node n */
void insertDocumentLocal(Node *n, ID *docId)
{
  Document *doc;

  if (n->status != PRESENT) {
    printf("document %d not inserted at node %d at time %f: node not present\n",
	   *docId, n->id, Clock);
  } else {
    // allocate space for new document 
    doc = newDoc(*docId);

    // MW: other calls to insertDocInList don't check its return value;
    // that leads to memory leaks (since the doclist might not accept the
    // doc). for consistency, let's also just assume the insert went fine
    // here. (and insertDocInList will change so that it never rejects
    // documents.)
    //
#if 0
    if (!insertDocInList(n->docList, doc)) {
      // no room to insert document 
      printf("document %x not inserted at node %x at time %f: no more storage space\n", 
	     doc->id, n->id, Clock); 
      free(doc);
      return;
    } else {
      printf("document %d inserted at node %d at time %f\n", 
	     *docId, n->id, Clock);
      removeDocFromList(&PendingDocs, *docId);
    }
#endif
    insertDocInList(n->docList, doc);
    printf("document %d inserted at node %d at time %f\n", 
	     *docId, n->id, Clock);
    removeDocFromList(&PendingDocs, *docId);
  }
}


// insert specified document at the head of document list. 
// return TRUE if document successfully inserted, and FALSE otherwise.
// if document already in the list, do nothing and return TRUE
int insertDocInList(DocList *docList, Document *doc)
{
  // MW: see comment above. the return value of this function is rarely
  // checked. our callers expect that the insert went fine, so let's meet
  // their expectations!
#if 0
  if (docList->size >= MAX_NUM_DOCS) {
    /* no room available in document list */
    return FALSE;
  }
#endif

  // insert document in order; don't insert if it's already there
  Document** pp;

  if (fake_doc_list_ops) {
      free(doc);
      return TRUE;
  }

  doc->next = NULL;

  for (pp = &docList->head; *pp; pp = &((*pp)->next)) {
	
      if (doc->id < (*pp)->id)  {
	  doc->next = *pp;
	  break;
      } else if (doc->id == (*pp)->id) 
	  return TRUE;
  }

  *pp = doc;

  return TRUE;
}


// search for document docId in docList
Document *findDocInList(DocList *docList, ID docId)
{
  Document *doc;

  if (fake_doc_list_ops) {
      // fool the code that is calling us
      return (Document*)4;
  }

  for (doc = docList->head; doc; doc = doc->next) {
    if (doc->id == docId)
      return doc;
    else if (doc->id > docId) // we passed it
      return NULL;
  }

  return NULL;

}


// remove document docId (if any) from docList
void removeDocFromList(DocList *docList, ID docId)
{
  // in-order search/removal
  Document** pp;
  Document* d;

  if (fake_doc_list_ops)
      return;

  for (pp = &(docList->head); *pp; pp = &((*pp)->next)) {
    if (docId == (*pp)->id) {
	d = *pp;	
	*pp = (*pp)->next;
	free(d);
	return;
    } else if (docId < (*pp)->id) 
	return;
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
#ifdef TRACE
    printf("updateDocList: node %d has been deleted in the meantime!\n",
	   s->id);
#endif // TRACE
    return;
  }

  // move all documents x stored at s that preceeds n
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
#ifdef TRACE
    printf("moveDocList: node %d has been deleted in the meantime!\n",
	   n1->id);
#endif
    return;
  }

  // move all documents x stored at s which preceeds n
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

  printf("pending documents: ");

  for (doc = (&PendingDocs)->head; doc; doc = doc->next) {
    printf("%d", doc->id);
    if (doc->next)
      printf(", ");
  }
  printf("\n");
}
               
