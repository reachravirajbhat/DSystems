/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include "limits.h"

/**
 * Macros
 */
#define TCLEAN 3     // Transaction Cleanup timer value
#define TRANSQ 0     // Compile flag for ordered transaction queue

#if TRANSQ
/**
 * CLASS NAME: TransactionQEnt
 *
 * DESCRIPTION: This class encapsulates transaction entry that needs to 
 *              be maintained in a queue as it came out of sequence. It
 *              stores following information:
 *              1) Transaction ID
 *              2) Message that was received
 *
 */
class TransactionQEnt
{
public:
   // Transaction ID of the transaction to be queued
   int        transId;

   // Message received
   Message    msg;

   // Constructors
   TransactionQEnt();
   TransactionQEnt(int _transId, Message _msg);

   // Destructors
   ~TransactionQEnt();
}
#endif

/**
 * CLASS NAME: QuorumChk
 *
 * DESCRIPTION: This class enacapsulates all the information required
 *              to check the quorum for CRUD operations, including:
 *              1) Message sent out
 *              2) quorum count
 *              3) Primary, Secondary and Tertiary Nodes to which
 *                 message has been sent out
 */
class QuorumChk
{
public:
   // Quorum Count
   int             qCnt;

   // Message type sent out
   MessageType     msgType;

   // Timestamp when this transaction was started/re-started
   int             timestamp;

   // Key and Value sent in the message
   string          key;
   string          value;

   // Primary, Secondary and Tertiary Nodes to whom this message
   // has been sent. This field is stored for retransmission of 
   // CRUD messages if CRUD response is not received. Retransmission
   // is not implemented yet.
   vector<Node>    destNodes;

   // Constructors
   QuorumChk();

   // Used for CREATE and UPDATE operations
   QuorumChk(int _qCnt, int _timestamp, MessageType _msgType, string _key, 
             string _value, vector<Node> _destNodes);
 
   // Used for READ and DELETE operations
   QuorumChk(int _qCnt, int _timestamp, MessageType _msgType, string _key, 
             vector<Node> _destNodes);
   QuorumChk(const QuorumChk& another);
   QuorumChk& operator=(const QuorumChk& another);

   // Destructor
   ~QuorumChk();
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 *              including:
 *              1) Ring
 *              2) Stabilization Protocol
 *              3) Server side CRUD APIs
 *              4) Client side CRUD APIs
 */
class MP2Node 
{
private:
   // Vector holding the next two neighbors in the ring 
   // who have my replicas
   vector<Node> hasMyReplicas;

   // Vector holding the previous two neighbors in the 
   // ring whose replicas I have
   vector<Node> haveReplicasOf;

   // Hash Table
   HashTable * ht;

   // Member representing this member
   Member *memberNode;

   // Params object
   Params *par;

   // Object of EmulNet
   EmulNet * emulNet;

   // Object of Log
   Log * log;
 
   // Fields added by me
   // Map of all outstanding transactions
   map<int, QuorumChk> openTransactions;

#if TRANSQ
   // Map of queued transactions, indexed by node address
   map<string, TransactionQEnt>  outOfSeqTransQ;
#endif

   // Running transaction counter to be used for messages sent out
   // by this node.
   int   curTransId;

public:
   // Ring
   vector<Node> ring;

   MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, 
           Log *log, Address *addressOfMember);
   Member * getMemberNode() 
   {
      return this->memberNode;
   }

   // ring functionalities
   void updateRing();
   vector<Node> getMembershipList();
   size_t hashFunction(string key);
   void findNeighbors();

   // client side CRUD APIs
   void clientCreate(string key, string value);
   void clientRead(string key);
   void clientUpdate(string key, string value);
   void clientDelete(string key);

   // receive messages from Emulnet
   bool recvLoop();
   static int enqueueWrapper(void *env, char *buff, int size);

   // handle messages from receiving queue
   void checkMessages();

   // coordinator dispatches messages to corresponding nodes
   void dispatchMessages(Message message);

   // find the addresses of nodes that are responsible for a key
   vector<Node> findNodes(string key);

   // server
   bool createKeyValue(string key, string value, ReplicaType replica);
   string readKey(string key);
   bool updateKeyValue(string key, string value, ReplicaType replica);
   bool deletekey(string key);

   // stabilization protocol - handle multiple failures
   void stabilizationProtocol(vector<Node> curMemList);

   // New methods added by me
   int  getNextTransId();
   void updateReplicaNodesAndRing(vector<Node> curMemList, bool ringFlag,
                                  bool replicasFlag);
   void printAddress(Address *addr);
   void printReplicaNodes(vector<Node> replicas);
   void printRing(vector<Node> curRing);
   vector<Node> giveReplicaNodes(ReplicaType repType);
   int  cmpNodeLists(vector<Node> list1, vector<Node> list2);
   void clientDelete(string key, vector<Node> replicas);
   void cleanTransactions();

   ~MP2Node();
};

#endif /* MP2NODE_H_ */
