/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, 
		 EmulNet * emulNet, Log * log, 
		 Address * address) 
{
   this->memberNode         = memberNode;
   this->par                = par;
   this->emulNet            = emulNet;
   this->log                = log;
   this->openTransactions.clear();
#if TRANSQ
   this->outOfSeqTransQ.clear();
#endif
   ht                       = new HashTable();
   this->memberNode->addr   = *address;
   this->curTransId         = 0;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() 
{
   this->openTransactions.clear();
#if TRANSQ
   this->outOfSeqTransQ.clear();
#endif
   delete ht;
   delete memberNode;
}

/**
 * QuorumChk Empty Constructor
 */
QuorumChk::QuorumChk()
{
   this->qCnt      = 0;
   this->msgType   = (MessageType) 0;
   this->timestamp = 0;
   this->key.clear();
   this->value.clear();
   this->destNodes.clear();
}

/**
 * QuorumChk Constructor
 */
QuorumChk::QuorumChk(int _qCnt, int _timestamp, MessageType _msgType,
                     string _key, string _value, vector<Node> _destNodes)
{
   this->qCnt      = _qCnt;
   this->msgType   = _msgType;
   this->timestamp = _timestamp;
   this->key       = _key;
   this->value     = _value;
   for (int i = 0; i < _destNodes.size(); i++)
      this->destNodes.emplace_back(Node(_destNodes.at(i).nodeAddress));
}

/**
 * QuorumChk Constructor
 */
QuorumChk::QuorumChk(int _qCnt, int _timestamp, MessageType _msgType,
                     string _key, vector<Node> _destNodes)
{
   this->qCnt      = _qCnt;
   this->msgType   = _msgType;
   this->timestamp = _timestamp;
   this->key       = _key;
   this->value.clear();
   for (int i = 0; i < _destNodes.size(); i++)
      this->destNodes.emplace_back(Node(_destNodes.at(i).nodeAddress));
}

/**
 * QuorumChk Copy Constructor
 */
QuorumChk::QuorumChk(const QuorumChk& another)
{
   this->qCnt        = another.qCnt;
   this->msgType     = another.msgType;
   this->timestamp   = another.timestamp;
   this->key         = another.key;
   this->value       = another.value;
   for (int i = 0; i < another.destNodes.size(); i++)
      this->destNodes.emplace_back(Node(another.destNodes.at(i).nodeAddress));
}

/**
 * QuorumChk Operator Constructor
 */
QuorumChk& QuorumChk::operator=(const QuorumChk& another)
{
   this->qCnt        = another.qCnt;
   this->msgType     = another.msgType;
   this->timestamp   = another.timestamp;
   this->key         = another.key;
   this->value       = another.value;
   for (int i = 0; i < another.destNodes.size(); i++)
      this->destNodes.emplace_back(Node(another.destNodes.at(i).nodeAddress));
   return *this;
}

/**
 * QuorumChk Destructor
 */
QuorumChk::~QuorumChk()
{
   this->destNodes.clear();
   this->key.clear();
   this->value.clear();
}

#if TRANSQ
/**
 * TransactionQEnt Empty Constructor
 */
TransactionQEnt::TransactionQEnt() 
{
   this->transID = 0;
}

/**
 * TransactionQEnt Constructor
 */
TransactionQEnt::TransactionQEnt(int _transId, Message _msg)
{
   this->transID = _transID;
   this->msg     = _msg;
}

/**
 * TransactionQEnt Destructor
 */
TransactionQEnt::~TransactionQEnt()
{
   this->transID = 0;
}
#endif

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 *              1) Gets the current membership list from the Membership 
 *                 Protocol (MP1Node). The membership list is returned 
 *                 as a vector of Nodes. See Node class in Node.h
 *              2) Constructs the ring based on the membership list
 *              3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() 
{
   /*
    * Implement this. Parts of it are already implemented
    */
   vector<Node>               curMemList;

   /*
    *  Step 1. Get the current membership list from Membership Protocol / MP1
    */
   curMemList.clear();
   curMemList = getMembershipList();

   /*
    * Step 2: Construct the ring
    */
   // Sort the list based on the hashCode
   sort(curMemList.begin(), curMemList.end());

   // Check if list of replica nodes for this node or the nodes for which
   // this node is a replica has been built. If not (this can happen during
   // initial stages of node bring up), then update the ring and the replica
   // nodes and exit.
   if ((hasMyReplicas.size() == 0) || (haveReplicasOf.size() == 0))
   {
      // Update both ring and replicas
      updateReplicaNodesAndRing(curMemList, true, true);
      return;
   }

   /*
    * Step 3: Run the stabilization protocol IF REQUIRED
    */
   // Check if membership list is different from current ring. If yes, then
   // we need to run stabilization protocol.
   if (cmpNodeLists(curMemList, ring))
      stabilizationProtocol(curMemList);
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from 
 *              the Membership protocol/MP1 and
 *              i) generates the hash code for each member
 *              ii) populates the ring member in MP2Node class
 *              
 *              It returns a vector of Nodes. Each element in the 
 *              vector contain the following fields:
 *              a) Address of the node
 *              b) Hash code obtained by consistent hashing of the 
 *                 Address
 */
vector<Node> MP2Node::getMembershipList() 
{
   unsigned int    i;
   vector<Node>    curMemList;

   curMemList.clear();
   for (i = 0 ; i < this->memberNode->memberList.size(); i++) 
   {
      Address  addressOfThisMember;
      int      id   = this->memberNode->memberList.at(i).getid();
      short    port = this->memberNode->memberList.at(i).getport();

      memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
      memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));

      // Create a Node using the constructor, which generates hashcode
      // in the constructor in Node.cpp
      curMemList.emplace_back(Node(addressOfThisMember));
   }
   return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position 
 *              on the ring
 *              HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) 
{
   std::hash<string>   hashFunc;
   size_t              ret = hashFunc(key);

   return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 *              The function does the following:
 *              1) Constructs the message
 *              2) Finds the replicas of this key
 *              3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) 
{
   /*
    * Implement this
    */
   vector<Node>      destNodes = findNodes(key);
   Node              node;

   if (destNodes.size() >= 3)
   {
      // Build a CREATE message and send it to PRIMARY node
      Message *msg = new Message(getNextTransId(), memberNode->addr, 
                                 CREATE, key, value, PRIMARY);
      node         = destNodes[PRIMARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Update the replica type in message to SECONDARY and send
      // the message to SECONDARY Node
      msg->replica = SECONDARY;
      node         = destNodes[SECONDARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Update the replica type in message to TERTIARY and send
      // the message to TERTIARY Node
      msg->replica = TERTIARY;
      node         = destNodes[TERTIARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Add an entry into the Quorum check table to tally the REPLY
      // message quorum
      openTransactions[curTransId] = QuorumChk(0, par->getcurrtime(), CREATE,
		                               key, value, destNodes);
      delete msg;
   }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
   /*
    * Implement this
    */
   vector<Node>      destNodes = findNodes(key);
   Node              node;

   if (destNodes.size() >= 3)
   {
      // Build a READ message and send it to PRIMARY node
      Message *msg = new Message(getNextTransId(), memberNode->addr, 
                                 READ, key);
      node         = destNodes[PRIMARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Send the message to SECONDARY Node
      node         = destNodes[SECONDARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Send the message to TERTIARY Node
      node         = destNodes[TERTIARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Add an entry into the Quorum check table to tally the REPLY
      // message quorum
      openTransactions[curTransId] = QuorumChk(0, par->getcurrtime(), READ,
                                               key, destNodes);
      delete msg;
   }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 *              The function does the following:
 *              1) Constructs the message
 *              2) Finds the replicas of this key
 *              3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
   /*
    * Implement this
    */
   vector<Node>      destNodes = findNodes(key);
   Node              node;

   if (destNodes.size() >= 3)
   {
      // Build a UPDATE message and send it to PRIMARY node
      Message *msg = new Message(getNextTransId(), memberNode->addr, 
                                 UPDATE, key, value, PRIMARY);
      node         = destNodes[PRIMARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Update the replica type in message to SECONDARY and send
      // the message to SECONDARY Node
      msg->replica = SECONDARY;
      node         = destNodes[SECONDARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Update the replica type in message to TERTIARY and send
      // the message to TERTIARY Node
      msg->replica = TERTIARY;
      node         = destNodes[TERTIARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Add an entry into the Quorum check table to tally the REPLY
      // message quorum
      openTransactions[curTransId] = QuorumChk(0, par->getcurrtime(), UPDATE,
		                               key, value, destNodes);
      delete msg;
   }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 *              The function does the following:
 *              1) Constructs the message
 *              2) Finds the replicas of this key
 *              3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
   /*
    * Implement this
    */
   vector<Node>      destNodes = findNodes(key);
   Node              node;

   if (destNodes.size() >= 3)
   {
      // Build a DELETE message and send it to PRIMARY node
      Message *msg = new Message(getNextTransId(), memberNode->addr, 
                                 DELETE, key);
      node         = destNodes[PRIMARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Send the message to SECONDARY Node
      node         = destNodes[SECONDARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Send the message to TERTIARY Node
      node         = destNodes[TERTIARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Add an entry into the Quorum check table to tally the REPLY
      // message quorum
      openTransactions[curTransId] = QuorumChk(0, par->getcurrtime(),
                                               DELETE, key, destNodes);
      delete msg;
   }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: Overloaded client side DELETE API
 *              The function does the following:
 *              1) Constructs the message
 *              2) Sends a message to the replica list provided
 *              This function is used during stabilization protocol to 
 *              send DELETE message to old replica list, as the updated
 *              ring may not have one of more nodes in old replicas.
 */
void MP2Node::clientDelete(string key, vector<Node> replicas)
{
   Node    node;

   if (replicas.size() >= 3)
   {
      // Build a DELETE message and send it to PRIMARY node
      Message *msg = new Message(getNextTransId(), memberNode->addr, 
                                 DELETE, key);
      node         = replicas[PRIMARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Send the message to SECONDARY Node
      node         = replicas[SECONDARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Send the message to TERTIARY Node
      node         = replicas[TERTIARY];
      emulNet->ENsend(&memberNode->addr, &node.nodeAddress,
		      msg->toString());

      // Add an entry into the Quorum check table to tally the REPLY
      // message quorum
      openTransactions[curTransId] = QuorumChk(0, par->getcurrtime(), 
                                               DELETE, key, replicas);
      delete msg;
   }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 *              The function does the following:
 *              1) Inserts key value into the local hash table
 *              2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) 
{
   /*
    * Implement this
    */
   // Insert key, value, replicaType into the hash table
   Entry    *entry = new Entry(value, par->getcurrtime(), replica);
   bool     result;

   result = ht->create(key, entry->convertToString());

   delete entry;
   return result;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 *              This function does the following:
 *              1) Read key from local hash table
 *              2) Return value
 */
string MP2Node::readKey(string key) 
{
   /*
    * Implement this
    */
   string    value;
   Entry     *entry;

   // Read key from local hash table
   value    = ht->read(key);

   // Check if we found entry.
   if (value.size())
   {
      // Yes, we found entry. Now strip the delimitor and timestamp from
      // the entry and get just the value.
      entry    = new Entry(value);
      value    = entry->value;
      delete entry;
   }
   return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 *              This function does the following:
 *              1) Update the key to the new value in the local hash table
 *              2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) 
{
   /*
    * Implement this
    */
   // Update key in local hash table and return true or false
   Entry    *entry = new Entry(value, par->getcurrtime(), replica);
   bool     result;

   result = ht->update(key, entry->convertToString());
   delete entry;
   return result;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 *              This function does the following:
 *              1) Delete the key from the local hash table
 *              2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) 
{
   /*
    * Implement this
    */
   // Delete the key from the local hash table
   return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 *              This function does the following:
 *              1) Pops messages from the queue
 *              2) Handles the messages according to message types
 */
void MP2Node::checkMessages() 
{
   /*
    * Implement this. Parts of it are already implemented
    */
   char      *data;
   int       size;
   Message   *msg;
   Message   *respMsg;
   bool      result;
   string    value;

   /*
    * Declare your local variables here
    */

   // dequeue all messages and handle them
   while ( !memberNode->mp2q.empty() ) 
   {
      /*
       * Pop a message from the queue
       */
      data = (char *)memberNode->mp2q.front().elt;
      size = memberNode->mp2q.front().size;
      memberNode->mp2q.pop();

      string message(data, data + size);

      /*
       * Handle the message types here
       */
      msg = new Message(message);

#if TRANSQ
      // Check if there is any out of sequence transactions queued on this
      // node. If yes, see if we have received the missing transaction. If
      // yes, then after processing this message, we can process queued
      // messages as well.
      if (outOfSeqTransQ.size())
      {
	 // The queue is maintained in ascending order of the transaction
	 // ID, i.e., first entry in the queue (or map) has the lowest
	 // transaction ID number. Check if the first entry in the queue
	 // (i.e., queued transaction with lowest transaction ID) is
	 // greater than the received message. If yes, process current
	 // message received, else, queue it at appropriate sorted position
	 // in the queue.
	 if (outOfSeqTransQ.at(0).second.transID > msg->transID)
         {
         }
	 else
         {
            pos = 0;
	    trans  = outOfSeqTransQ.at(pos);
	    while ((pos < outOfSeqTransQ.size()) &&
                   (msg->transID > trans.second.transID))
            {
               pos++;
            }
	    if (pos < outOfSeqTransQ.size())
            {
            }
         }
      }
#endif

      switch(msg->type)
      {
         case CREATE:
            // Create the key value entry in the hash table and send REPLY
	    // message back to the node that sent this CREATE message with
	    // same transaction ID, so that REPLY can be correlated with
	    // stored Quorum Check map.
            result  = createKeyValue(msg->key, msg->value, msg->replica);
#ifdef DEBUGLOG
            if (result == true)
               log->logCreateSuccess(&memberNode->addr, false,
                                     msg->transID, msg->key, msg->value);
            else
               log->logCreateFail(&memberNode->addr, false,
                                  msg->transID, msg->key, msg->value);
#endif
            respMsg = new Message(msg->transID, memberNode->addr, 
                                  REPLY, result);
            emulNet->ENsend(&memberNode->addr, &msg->fromAddr,
		            respMsg->toString());
	    delete respMsg;
            break;
            
         case READ:
	    // Read the value from the hash table and send it back in
	    // READREPLY message with same transaction ID, so that 
	    // READREPLY can be correlated with stored Quorum check map.
            value  = readKey(msg->key);

	    // Check if we found an entry corresponding to this key.
	    if (value.size())
            {
               // Yes, we found an entry. Send back corresponding value in 
	       // READREPLY message.
#ifdef DEBUGLOG
               log->logReadSuccess(&memberNode->addr, false,
                                   msg->transID, msg->key, value);
#endif
               respMsg = new Message(msg->transID, memberNode->addr, value);
            }
	    else
            {
               // No, we did not find an entry. Send back REPLY message
	       // with failure code.
#ifdef DEBUGLOG
               log->logReadFail(&memberNode->addr, false,
                                msg->transID, msg->key);
#endif
               respMsg = new Message(msg->transID, memberNode->addr, 
                                     REPLY, false);
            }
            emulNet->ENsend(&memberNode->addr, &msg->fromAddr,
                               respMsg->toString());
            delete respMsg;
            break;
            
         case UPDATE:
            // Update the key value entry in the hash table and send REPLY
	    // message back to the node that sent this UPDATE message with
	    // same transaction ID, so that REPLY can be correlated with
	    // stored Quorum Check map.
            result  = updateKeyValue(msg->key, msg->value, msg->replica);
#ifdef DEBUGLOG
            if (result == true)
               log->logUpdateSuccess(&memberNode->addr, false,
                                     msg->transID, msg->key, msg->value);
            else
               log->logUpdateFail(&memberNode->addr, false,
                                  msg->transID, msg->key, msg->value);
#endif
            respMsg = new Message(msg->transID, memberNode->addr, 
                                  REPLY, result);
            emulNet->ENsend(&memberNode->addr, &msg->fromAddr,
		            respMsg->toString());
	    delete respMsg;
            break;
            
         case DELETE:
	    // Delete the key from local hash table.
            result = ht->deleteKey(msg->key);
#ifdef DEBUGLOG
            if (result == true)
               log->logDeleteSuccess(&memberNode->addr, false,
                                     msg->transID, msg->key);
            else
               log->logDeleteFail(&memberNode->addr, false,
                                  msg->transID, msg->key);
#endif
            respMsg = new Message(msg->transID, memberNode->addr, 
                                  REPLY, result);
            emulNet->ENsend(&memberNode->addr, &msg->fromAddr,
		            respMsg->toString());
	    delete respMsg;
            break;
            
         case REPLY:
         case READREPLY:
            // Find the QuorumChk entry related to the transaction ID
            // received in REPLY message.
            map<int, QuorumChk>::iterator     trans;

            trans = openTransactions.find(msg->transID);

            // Check if the entry is found. If yes, do further processing.
            // If not, do nothing.
            if (trans != openTransactions.end())
            {
               // Found open transaction. Increment the quorum count.
               trans->second.qCnt++;

               // Check if we have reached Quorum, i.e., as we have three
               // replicas, quorum in this case means receiving reply from
               // at least two replicas, or quorum count being two or higher.
               if (trans->second.qCnt >= 2)
               {
                  // Log CRUD success at coordinator as we have received a
		  // quorum of response.
#ifdef DEBUGLOG
                  switch (trans->second.msgType)
                  {
                     case CREATE:
                        if (msg->success == true)
                           log->logCreateSuccess(&memberNode->addr, true,
                                                 msg->transID,
						 trans->second.key,
                                                 trans->second.value);
			else
                           log->logCreateFail(&memberNode->addr, true,
                                              msg->transID,
					      trans->second.key,
                                              trans->second.value);
                        break;

                     case READ:
                        if ((msg->success == true) || (msg->type == READREPLY))
                           log->logReadSuccess(&memberNode->addr, true,
                                               msg->transID,
					       trans->second.key,
                                               msg->value);
			else
                           log->logReadFail(&memberNode->addr, true,
                                            msg->transID,
					    trans->second.key);
                        break;

                     case UPDATE:
                        if (msg->success == true)
                           log->logUpdateSuccess(&memberNode->addr, true,
                                                 msg->transID,
						 trans->second.key,
                                                 trans->second.value);
			else
                           log->logUpdateFail(&memberNode->addr, true,
                                              msg->transID,
					      trans->second.key,
                                              trans->second.value);
                        break;

                     case DELETE:
                        if (msg->success == true)
                           log->logDeleteSuccess(&memberNode->addr, true,
                                                 msg->transID,
						 trans->second.key);
			else
                           log->logDeleteFail(&memberNode->addr, true,
                                              msg->transID,
					      trans->second.key);
                        break;
                  }
#endif

                  // Remove the transaction from the map, as we don't need
                  // to keep track of the quorum count anymore.
                  openTransactions.erase(trans);
               }
            }
            break;
      }
      delete msg;
   }

   /*
    * This function should also ensure all READ and UPDATE operation
    * get QUORUM replies
    */
   // Traverse through open transaction list to check if any of the
   // transactions have expired without receiving quorum response.
   cleanTransactions();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 *              This function is responsible for finding the replicas 
 *              of a key
 */
vector<Node> MP2Node::findNodes(string key) 
{
   size_t        pos = hashFunction(key);
   vector<Node>  addr_vec;

   addr_vec.clear();
   if (ring.size() >= 3) 
   {
      // if pos <= min || pos > max, the leader is the min
      // We are checking if the hashcode of the key provided is
      // less than then first node in the ring or greater than the
      // last node in the ring. If yes, then return the first three
      // nodes in the ring as the nodes that have the replicas for
      // the key provided.
      if (pos <= ring.at(0).getHashCode() || 
          pos > ring.at(ring.size()-1).getHashCode()) 
      {
         addr_vec.emplace_back(Node(ring.at(0).nodeAddress));
         addr_vec.emplace_back(Node(ring.at(1).nodeAddress));
         addr_vec.emplace_back(Node(ring.at(2).nodeAddress));
      }
      else 
      {
         // go through the ring until pos <= node
         // Let us find the first node in the ring which has a hash
         // code greater than that of the hash code of the key 
         // provided. Once found, return the next three nodes in
         // the ring.
         for (int i=1; i<ring.size(); i++)
	 {
            Node addr = ring.at(i);
            if (pos <= addr.getHashCode()) 
	    {
               addr_vec.emplace_back(Node(ring.at(i).nodeAddress));
               addr_vec.emplace_back(Node(ring.at((i+1)%ring.size()).nodeAddress));
               addr_vec.emplace_back(Node(ring.at((i+2)%ring.size()).nodeAddress));
               break;
            }
         }
      }
   }
   return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() 
{
   if ( memberNode->bFailed ) 
   {
      return false;
   }
   else 
   {
      return emulNet->ENrecv(&(memberNode->addr), 
		             this->enqueueWrapper, NULL, 1, 
			     &(memberNode->mp2q));
   }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) 
{
   Queue    q;

   return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node 
 *              joins and leaves
 *              
 *              It ensures that there always 3 copies of all keys in 
 *              the DHT at all times
 *
 *              The function does the following:
 *              1) Ensures that there are three "CORRECT" replicas of 
 *                 all the keys in spite of failures and joins
 *                 
 *              Note:- "CORRECT" replicas implies that every key is 
 *                     replicated in its two neighboring nodes in the 
 *                     ring
 */
void MP2Node::stabilizationProtocol(vector<Node> curMemList) 
{
   /*
    * Implement this
    */
   map<string, string>::iterator  curHTEnt;
   Entry                          *htEnt;
   vector<Node>                   newReplicas, origReplicas;

   // Update only the ring. Do not update replica node, as we need the
   // original replica list so that we can send delete message to original
   // replicas.
   updateReplicaNodesAndRing(curMemList, true, false);

   // Traverse the hash list entries for this node and check whether the
   // entries still belong to this node and has the replica type changed.
   for (curHTEnt = ht->hashTable.begin(); 
        curHTEnt != ht->hashTable.end();
        curHTEnt++)
   {
      newReplicas.clear();
      origReplicas.clear();
      htEnt         = (Entry *)new Entry(curHTEnt->second);

      // Find appropriate replica nodes for the key in this hash list entry.
      newReplicas   = findNodes(curHTEnt->first);

      // Build original replica nodes list for this hash list entry.
      origReplicas  = giveReplicaNodes(htEnt->replica);

      // Compare the two replica lists. If they are not the same, we need to
      // delete the entries from original replica nodes and create a new ones
      // in new replica nodes.
      if (cmpNodeLists(origReplicas, newReplicas))
      {
         clientCreate(curHTEnt->first, htEnt->value);
         clientDelete(curHTEnt->first, origReplicas);
      }

      delete htEnt;
   }

   // Update the replica nodes only, as the ring has been updated earlier
   updateReplicaNodesAndRing(curMemList, false, true);
}

/**
 * FUNCTION NAME:  getNextTransId
 *
 * DESCRPTION:  Increments the running message transaction counter and
 *              returns the next transaction ID to be used.
 *
 */
int MP2Node::getNextTransId()
{
   curTransId++;
   curTransId %= (INT_MAX-1);
   return (curTransId);
}

/**
 * FUNCTION NAME: updateReplicaNodesAndRing
 *
 * DESCRIPTION:   Updates the ring using sorted membership list provided
 *                as parameter and updates the nodes that should be replica
 *                of this node and nodes for which this node should be
 *                replica.
 *
 */
void MP2Node::updateReplicaNodesAndRing(vector<Node> curMemList,
                                        bool ringFlag, bool replicasFlag)
{
   vector<Node>::iterator         curMem;
   Node                           node;
   int                            i, secondary, tertiary;

   // Check if we should update the ring.
   if (ringFlag == true)
   {
      // Yes, Update the ring
      ring.clear();
      int i1 = ring.size();
      for (i = 0; i < curMemList.size(); i++)
      {
         node = curMemList.at(i);
         ring.emplace_back(Node(node.nodeAddress));
      }
   }
 
   // Check if we should update the replicas. If not, do nothing else and
   // return
   if (replicasFlag == false)
      return;

   // Clear this node's replica list and build it again using the updated
   // ring information
   hasMyReplicas.clear();
   haveReplicasOf.clear();
   for (i=0; i<ring.size(); i++)
   {
      // Check if we have reached self node in the ring.
      Node curNode = ring.at(i);
      if (memcmp(memberNode->addr.addr, curNode.nodeAddress.addr,
                 sizeof(memberNode->addr.addr)) == 0)
      {
         // Reached self node in the ring. Add the next two entries in
	 // the ring as my Replica nodes.
         hasMyReplicas.emplace_back(Node(ring.at((i+1)%ring.size()).nodeAddress));
         hasMyReplicas.emplace_back(Node(ring.at((i+2)%ring.size()).nodeAddress));

	 // Now update the nodes for which this node will have secondary
	 // or tertiary replicas
	 //
	 // Initialize the index for nodes for which this node will have
	 // secondary and tertiary keys.
         secondary = i - 1;
	 tertiary  = i - 2;

	 // Check if the secondary and tertiary indexes have to be wrapped
	 // around in the ring.
         if (i == 1) 
         {
	    tertiary  = ring.size() - 1;
	 }
         else
         {
            if (i == 0)
            {
               secondary = ring.size() - 1;
	       tertiary  = ring.size() - 2;
	    }
	 }
         haveReplicasOf.emplace_back(Node(ring.at(secondary).nodeAddress));
         haveReplicasOf.emplace_back(Node(ring.at(tertiary).nodeAddress));
	 break;
      }
   }
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP2Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/**
 * FUNCTION NAME: printReplicaNodes
 *
 * DESCRIPTION:   Given a list of replica nodes, print the address of
 *                replica nodes.
 *
 */
void MP2Node::printReplicaNodes(vector<Node> replicas)
{
   for (int i = 0; i < replicas.size(); i++)
   {
      printf("[%d] ", par->getcurrtime());
      switch(i)
      {
         case PRIMARY:
            printf("PRIMARY Node: ");
            break;

         case SECONDARY:
            printf("SECONDARY Node: ");
            break;

         case TERTIARY:
            printf("TERTIARY Node: ");
            break;

         default:
            printf("Error in the replica list: More than three nodes!!!");
            return;
      }
      printAddress(&replicas[i].nodeAddress);
   }
   return;
}

/**
 * FUNCTION NAME: printRing
 *
 * DESCRIPTION:   Prints the nodes in the ring
 *
 */
void MP2Node::printRing(vector<Node> curRing)
{
   for (int i = 0; i < curRing.size(); i++)
   {
      printf("[%d] Node %d: ", par->getcurrtime(), i);
      printAddress(&curRing[i].nodeAddress);
   }
   return;
}

/**
 * FUNCTION NAME: giveReplicaNodes
 *
 * DESCRIPTION: Given the replica type as input, this function returns
 *              list of nodes, where this node is at the right replica
 *              place in the list.
 *
 */
vector<Node> MP2Node::giveReplicaNodes(ReplicaType  repType)
{
   vector<Node> origRepList;

   origRepList.clear();

   // Check if this node's replica lists have been set or not. If not,
   // return empty list.
   if (!hasMyReplicas.size() || !haveReplicasOf.size())
      return origRepList;
   switch (repType)
   {
      case PRIMARY:
         // Return this node as Primary Replica followed by the nodes
	 // that it believes has its replicas.
	 origRepList.emplace_back(Node(memberNode->addr));
	 origRepList.emplace_back(Node(hasMyReplicas.at(0).nodeAddress));
	 origRepList.emplace_back(Node(hasMyReplicas.at(1).nodeAddress));
         break;
         
      case SECONDARY:
         // Return this node as Secondary Replica along with one node
	 // which it believes is before it in the ring and one node
	 // that it believes is next to it in the ring.
	 origRepList.emplace_back(Node(haveReplicasOf.at(0).nodeAddress));
	 origRepList.emplace_back(Node(memberNode->addr));
	 origRepList.emplace_back(Node(hasMyReplicas.at(0).nodeAddress));
         break;
         
      case TERTIARY:
         // Return this node as Tertiary Replica along with two nodes
	 // which it believes is before it in the ring 
	 origRepList.emplace_back(Node(haveReplicasOf.at(1).nodeAddress));
	 origRepList.emplace_back(Node(haveReplicasOf.at(0).nodeAddress));
	 origRepList.emplace_back(Node(memberNode->addr));
         break;
   }
   return origRepList;
}

/**
 * FUNCTION NAME: cmpNodeLists
 *
 * DESCRIPTION: Compares two replica list and returns 0 if they are the same
 *              else it returns 1.
 *
 */
int MP2Node::cmpNodeLists(vector<Node> list1, vector<Node> list2)
{
   Node l1Node, l2Node;

   if (list1.size() != list2.size())
      return 1;

   for (int i=0; i<list1.size(); i++)
   {
      l1Node = list1.at(i);
      l2Node = list2.at(i);
      if (memcmp(l1Node.nodeAddress.addr,
                 l2Node.nodeAddress.addr,
                 sizeof(l1Node.nodeAddress.addr)))
	 return 1;
   }
   return 0;
}

/**
 * FUNCTION NAME:  cleanTransactions
 *
 * DESCRIPTION:    This function traverses the list of all open transactions
 *                 for this node and check if TCLEAN timer has expired for
 *                 the transaction. If yes, then it means that this node has
 *                 not received the quorum reply and we should remove that
 *                 transaction from open transactions list and flag failure
 *                 for that transaction on coordinator side. 
 *
 *                 As an enhancement, we can implement retransmission of the
 *                 transaction in future to increase reliability of transac-
 *                 tions.
 *
 */
void MP2Node::cleanTransactions()
{
   map<int, QuorumChk>::iterator  trans, tempTrans;

   // Traverse open transaction list
   trans = openTransactions.begin(); 

   while (trans != openTransactions.end())
   {
      // Check if this transaction has not received quorum response in
      // TCLEAN time.
      if ((trans->second.timestamp + TCLEAN) < par->getcurrtime())
      {
         // This transaction has not received quorum response. Clean the
	 // transaction and flag coordinator side error message.
#ifdef DEBUGLOG
         switch (trans->second.msgType)
         {
            case CREATE:
               log->logCreateFail(&memberNode->addr, true, trans->first,
                                  trans->second.key, trans->second.value);
               break;

            case READ:
               log->logReadFail(&memberNode->addr, true,
                                trans->first, trans->second.key);
                        break;

            case UPDATE:
               log->logUpdateFail(&memberNode->addr, true, trans->first,
                                  trans->second.key, trans->second.value);
               break;

            case DELETE:
               log->logDeleteFail(&memberNode->addr, true,
                                  trans->first, trans->second.key);
               break;

            default:
               break;
         }
#endif
         // Remove the transaction from the map, as this transaction has
	 // failed.
	 // 
	 tempTrans = trans;
	 trans++;
         openTransactions.erase(tempTrans);
      }
      else
      {
         // Move to the next transaction
         trans++;
      }
   }
}
