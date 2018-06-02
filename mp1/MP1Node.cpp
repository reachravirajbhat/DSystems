/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) 
{
   /*
    * This function is partially implemented and may require changes
    */
   int id = *(int*)(&memberNode->addr.addr);
   int port = *(short*)(&memberNode->addr.addr[4]);

   memberNode->bFailed          = false;
   memberNode->inited           = true;
   memberNode->inGroup          = false;

   // node is up!
   memberNode->nnb              = 0;
   memberNode->heartbeat        = 0;
   memberNode->pingCounter      = TFAIL;
   memberNode->timeOutCounter   = -1;

   // Initialize Timer values
   tHBVal        = THB;
   tFailVal      = TFAIL;
   tCleanupVal   = TREMOVE;

   // Initialize the Member List with self node
   initMemberListTable(memberNode);

   return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr  *msg;
    Address     newNodeAddr;
    int         newNodeAddrId;
    short       newNodeAddrPort;
    long        heartBeat;
    vector<MemberListEntry>::iterator  curMem; // Pointer to current member
                                               // while traversing member
					       // list
#ifdef DEBUGLOG
    static char s[1024];
#endif

    msg = (MessageHdr *)data;
    switch (msg->msgType)
    {
	case JOINREQ:
	    // Add the node sending JOINREQ to this node's member List and
	    // send the information about all members in this node's member
	    // list to the peer node.

	    // Copy the address of the node from which we received JOINREQ
	    // message.
	    newNodeAddr.init();
	    memcpy((char *)&newNodeAddr.addr, 
	           (char *)(msg+1), sizeof(newNodeAddr.addr));
            memcpy(&newNodeAddrId, (char *)(msg+1), sizeof(int));
            memcpy(&newNodeAddrPort, (char *)(msg+1)+sizeof(int), sizeof(short));
	    // Copy the heartbeat information.
	    memcpy(&heartBeat, 
	           (char *)(msg+1)+1+sizeof(newNodeAddr.addr),
	           sizeof(long));

#ifdef DEBUGLOG
            sprintf(s, "Handling JOINREQ message of size(%d) from: [%d.%d.%d.%d:%d]",
		    size,
                    newNodeAddr.addr[0], 
		    newNodeAddr.addr[1], 
		    newNodeAddr.addr[2], 
		    newNodeAddr.addr[3], 
		    *(short *)&newNodeAddr.addr[4]);
            log->LOG(&memberNode->addr, s);
#endif

	    // Check if the sender node exists in this node's member
	    // list. If not, then enter the node into member list.
	    insertNodeIntoMemberList(&newNodeAddr,
			             newNodeAddrId,
				     newNodeAddrPort,
				     heartBeat);
       
            // Send JOINREP message
	    sendMessage(JOINREP, &newNodeAddr);
	    // printMemberList();
	    break;
	
	case JOINREP:
	    // Since this node received JOINREP from Introducer, we can
	    // mark it as part of the Group.
	    memberNode->inGroup = true;

	    // Parse the message and update Membership list
	    processMessage(msg->msgType, data, size);
	    // printMemberList();
	    break;

	case HEARTBEAT:
	    // Parse the message and update Membership list
	    processMessage(msg->msgType, data, size);
	    // printMemberList();
	    break;

	default:
	    break;
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period 
 *              and then delete the nodes
 * 		Propagate your membership list
 */
void MP1Node::nodeLoopOps() 
{
   /*
    * Your code goes here
    */
   int                randomNbr;
   int                nodeCtr;
   int                selfId;
   short              selfPort;
   MemberListEntry    member;
   MemberListEntry    newMem;
   Address            destAddr;
   vector<MemberListEntry>::iterator  curMem; // Pointer to current member
                                              // while traversing member
				              // list

  
   // Copy self node ID and port number for comparison
   memcpy((char *)&selfId, (char *)&memberNode->addr.addr[0], sizeof(int));
   memcpy((char *)&selfPort, (char *)&memberNode->addr.addr[4], sizeof(short));

   // Check if your heartbeat timer has expired and this node has some
   // nodes in its membership list other than itself. If yes, then send 
   // out HEARTBEAT message
   if ((par->getcurrtime() - memberNode->heartbeat) >= tHBVal)
   {
      // Update this node's heartbeat and timestamp to current time
      memberNode->heartbeat  = par->getcurrtime();

      // Also update the heartbeat and timestamp to current time in
      // self entry in membership list
      for (curMem = memberNode->memberList.begin(); 
           curMem != memberNode->memberList.end(); 
           curMem++)
      {
	 if (((*curMem).id == selfId) && ((*curMem).port == selfPort))
         {
            (*curMem).heartbeat = par->getcurrtime();
            (*curMem).timestamp = par->getcurrtime();
	    break;
         }
      }

      // Check if there are any entries in the neighbor list. If none,
      // that means that either this node is still waiting for JOINREP
      // message from introducer or all the entries in the membership
      // list have timed out and been deleted in the last cycle. In this
      // case send a HEARTBEAT message to introducer node to restart
      // Gossip membership. Exit after doing that as there is nothing
      // else to process.
      if ((memberNode->nnb == 1) && (!memberNode->bFailed))
      {
         Address   introNodeAddr;

         introNodeAddr = getJoinAddress();
         sendMessage(HEARTBEAT, &introNodeAddr);
         return;
      }
 
      // Generate a random number to identify a random neighbor to send
      // HEARTBEAT message to.
      randomNbr = rand() % memberNode->nnb;

      // Initialize the counter which counts the nodes being traversed in
      // the membeship list. This counter is used to compare with random
      // neighbor counter generated and when it matches, we send HEARTBEAT
      // message. This is a way to randomize neighbors to whom we send
      // HEARTBEAT message.
      nodeCtr   = 0;

      for (curMem = memberNode->memberList.begin(); 
           curMem != memberNode->memberList.end(); 
           curMem++)
      {
	 member = *curMem;

	 // Check if this entry is not corresponding to self node. 
	 if ((member.id != selfId) || (member.port != selfPort))
         {
	    // First, check if this node has not been updated in TFAIL period.
	    // If yes, then remove this entry from the membership list.
	    if ((par->getcurrtime() - member.timestamp) >= 
                (tFailVal+tCleanupVal))
            {
	       // printf("Removing failed node [%d] from membership list of [%d]\n",
		 //      member.id, selfId);
#ifdef DEBUGLOG
               Address     delNodeAddr;

               // Build address of the node being deleted
               memcpy((char *)&delNodeAddr.addr[0], 
                      (char *)&member.id,
                      sizeof(int));
               memcpy((char *)&delNodeAddr.addr[4], 
                      (char *)&member.port,
                      sizeof(short));

               // Log that a node is being deleted from membership list
               log->logNodeRemove(&memberNode->addr, &delNodeAddr);
#endif

	       // Remove this node from membership list
	       curMem = memberNode->memberList.erase(curMem);
	       memberNode->nnb--;

	       // Since vector erase function returns iterator to next
	       // element in the vector, skip back to previous element
	       // as the for loop will increment the vector as next step.
	       // Otherwise, we will skip an element in this loop.
	       curMem--;
            }
	    else
            {
	       // Check if the current node counter matches random neighbor
	       // number or a number after that. This is to limit sending
	       // HEARTBEAT message to only two random neighbors
               if ((nodeCtr == randomNbr) || 
                   (nodeCtr == (randomNbr+1)))
               {
	          // Prepare destination address and send HEARTBEAT message
	          // to it
                  memcpy(&destAddr.addr[0], (char *)&member.id, sizeof(int));
                  memcpy(&destAddr.addr[4], (char *)&member.port, 
		         sizeof(short));
	          sendMessage(HEARTBEAT, &destAddr);
	       }

	       // Increment the counter which counts the nodes being traversed
	       // in the membership list.
	       nodeCtr++;
	    }
         }
      }
   }
   return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) 
{
   MemberListEntry    newMem;

   memberNode->memberList.clear();

   // Create a self entry in this node's membership
   // list with self address and initialize heartBeat
   // to current local time.
   memcpy(&newMem.id, (char *)&memberNode->addr.addr[0], sizeof(int));
   memcpy(&newMem.port, (char *)&memberNode->addr.addr[4], sizeof(short));
   newMem.heartbeat = par->getcurrtime();
   newMem.timestamp = par->getcurrtime();
   memberNode->memberList.push_back(newMem);
   memberNode->nnb++;
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/**
 * FUNCTION NAME: printMemberList
 *
 * DESCRIPTION: Prints the list of all members in the member list of this
 *              node along with heartbeat information.
 */
void MP1Node::printMemberList()
{
   vector<MemberListEntry>::iterator  curMem; // Pointer to current member
                                               // while traversing member
					       // list
   MemberListEntry    member;

   printf("%d members at time %d in Member List of node ", 
          memberNode->nnb, par->getcurrtime());
   printAddress(&memberNode->addr);

   // Run through all members in this node's member list
   for (curMem = memberNode->memberList.begin(); 
        curMem != memberNode->memberList.end(); 
        curMem++)
   {
      member = *curMem;
      printf("  %d:%d, heartbeat:%ld, timestamp:%ld\n", 
             member.id, member.port,member.heartbeat, member.timestamp);
   }
}

/** 
 * FUNCTION NAME: insertNodeIntoMemberList
 *
 * DESCRIPTION:   Traverses the Member List of the current node to check
 *                if a member entry to node address provided exists in
 *                the member list.
 *
 *                YES - Check if the heartbeat received is more recent. If
 *                      yes, then update the heartbeat and update local
 *                      timestamp for this node entry. Exit.
 *                
 *                NO  - Create a new node entry in the member list and 
 *                      record the heartbeat received and local timestamp.
 */
void MP1Node::insertNodeIntoMemberList(Address *newNodeAddr, 
		                       int     newNodeAddrId,
				       short   newNodeAddrPort,
				       int     heartBeat)
{
   MemberListEntry    member;
   MemberListEntry    newMem;
   int                memberExists; // Flag indicating if new member
                                    // already exists in this node's
                                    // membership list.
   vector<MemberListEntry>::iterator  curMem; // Pointer to current member
                                              // while traversing member
				              // list

   // Run through all members in this node's member list
   memberExists = 0;
   for (curMem = memberNode->memberList.begin(); 
        curMem != memberNode->memberList.end(); 
        curMem++)
   {
      member = *curMem;

      // check if the address of the current member node
      // in the received JOINREP message is same as the
      // current node being traversed in this node's
      // membership list
      if ((newNodeAddrId == member.id) && 
          (newNodeAddrPort == member.port))
      {
         // Check if the heartbeat received is more recent
         // than the one stored in current traversed node.
         // If yes, then update the heartbeat in current
         // traversed node.
         if (heartBeat > member.heartbeat)
         {
            (*curMem).heartbeat = heartBeat;
	    (*curMem).timestamp = par->getcurrtime();

	    // Update local variable to get new value recently updated
	    member = *curMem;
	 }

         // Mark that member has been found and break from
         // for loop
	 memberExists = 1;
	 break;
      }
   }

   // Check if the member was found in member list of this
   // node. If not, add a new entry
   if (memberExists == 0)     
   {
      // Create a new member list entry in this node's
      // membership list with the address and heartbeat
      // information received
      newMem.id        = newNodeAddrId;
      newMem.port      = newNodeAddrPort;
      newMem.heartbeat = heartBeat;
      newMem.timestamp = par->getcurrtime();
      memberNode->memberList.push_back(newMem);
      memberNode->nnb++;
#ifdef DEBUGLOG
      // Log that a node has been added to membership list
      log->logNodeAdd(&memberNode->addr, newNodeAddr);
#endif
    }
}

/**
 * FUNCTION NAME: sendMessage
 *
 * DESCRIPTION:   This function sends either JOINREP or HEARTBEAT
 *                message based on the message type specified. 
 *
 *                Message format is:
 *                      <
 *                       msgType:  4 bytes
 *                       srcAddr:  6 bytes
 *                       
 *                       Repeated for every member in Member List
 *                         memAddr:    6 bytes
 *                         unknown:    1 byte
 *                         heartBeat:  4 bytes
 *                      >
 */
void MP1Node::sendMessage(MsgTypes msgType, Address *destNodeAddr)
{
   char         respMsg[1024];
   int          respMsgIdx;
   MessageHdr   tmpMsgHdr;
   vector<MemberListEntry>::iterator  curMem; // Pointer to current member
                                              // while traversing member
				              // list

   memset(respMsg, 0, 1024);
   respMsgIdx        = 0;
   tmpMsgHdr.msgType = msgType;
   memcpy((char *)respMsg,
          &tmpMsgHdr, sizeof(tmpMsgHdr));
   respMsgIdx       += sizeof(tmpMsgHdr);

   // Copy the source address
   memcpy((char *)&respMsg[respMsgIdx],
          memberNode->addr.addr, 
          sizeof(memberNode->addr));
   respMsgIdx += sizeof(memberNode->addr);

   // Run through all members in the member list
   for (curMem = memberNode->memberList.begin(); 
        curMem != memberNode->memberList.end(); 
        ++curMem)
   {
      MemberListEntry newMem;

      newMem = *curMem;
       
      // Check if the local timestamp of this node has not been
      // updated for TFAIL period. If yes, then this node has likely
      // failed and we should not propagate it in the membership list.
      // It will get deleted after TFAIL+TREMOVE period.
      if ((par->getcurrtime() - newMem.timestamp) < tFailVal)
      {
         // Copy the member ID
         memcpy((char *)&respMsg[respMsgIdx],
                (char *)&newMem.id, sizeof(newMem.id));
         respMsgIdx += sizeof(newMem.id);

         // Copy the member port
         memcpy((char *)&respMsg[respMsgIdx],
                (char *)&newMem.port, sizeof(newMem.port));
         respMsgIdx += sizeof(newMem.port);

         // Skip 1 Byte in between: Need to figure out what this is for
         respMsgIdx += 1;

         // Copy the member heartbeat value
         memcpy((char *)&respMsg[respMsgIdx],
                (char *)&newMem.heartbeat, sizeof(newMem.heartbeat));
         respMsgIdx += sizeof(newMem.heartbeat);
      }
   }

   // send message out
   emulNet->ENsend(&memberNode->addr,  // From address 
	           destNodeAddr,       // To address
		   (char *)respMsg,    // Message to be sent
		   respMsgIdx);        // Message size

}


/**
 * FUNCTION NAME: processMessage
 *
 * DESCRIPTION:   Parses the message received (JOINREP or HEARTBEAT)
 *                and updates the membership list.
 */
void MP1Node::processMessage(MsgTypes msgType, char *msg, int size)
{
   char          respMsg[1024];
   int           respMsgIdx;
   long          heartBeat;
   Address       srcNodeAddr;
   int           srcNodeAddrId;
   short         srcNodeAddrPort;
#ifdef DEBUGLOG
   static char   s[1024];
   static char   m[1024];  
#endif


   // Skip 4 bytes of message type from message and parse the source 
   // address of the node from which we received message
   memset(respMsg, 0, 1024);
   respMsgIdx        = 4;
   memcpy((char *)respMsg, (char *)msg, size);
   srcNodeAddr.init();

   // Copy the source address as plain string
   memcpy((char *)&srcNodeAddr.addr, 
          (char *)&respMsg[respMsgIdx], 
          sizeof(srcNodeAddr.addr));

   // Copy the address as identifier and port number separately
   memcpy(&srcNodeAddrId, (char *)&respMsg[respMsgIdx], 
          sizeof(int));
   respMsgIdx += sizeof(int);
   memcpy(&srcNodeAddrPort, (char *)&respMsg[respMsgIdx], 
          sizeof(short));
   respMsgIdx += sizeof(short);

#ifdef DEBUGLOG
   switch(msgType)
   {
      case JOINREP:
	 sprintf(m, "JOINREP");
	 break;

      case HEARTBEAT:
	 sprintf(m, "HEARTBEAT");
	 break;

      default:
	 sprintf(m, "ERROR");
	 break;
   }
      
   sprintf(s, "Handling %s message of size(%d) from: [%d.%d.%d.%d:%d]",
	   m,
           size,
           srcNodeAddr.addr[0], 
	   srcNodeAddr.addr[1], 
	   srcNodeAddr.addr[2], 
	   srcNodeAddr.addr[3], 
	   *(short *)&srcNodeAddr.addr[4]);
   log->LOG(&memberNode->addr, s);
#endif

   // Message has 10 byte header that include 4 bytes of
   // message type and 6 bytes of source address.
   //
   // After this 10 byte header, we will have 11 bytes (6 bytes for
   // member node address, 1 unknown byte and 4 bytes for heartbeat)
   // for each member reported in the member list
   //
   // Let us check if there are any member entries beyond the header.
   // If not, exit. If yes, then traverse through the message to get
   // member information and check if that member information exists
   // in this node's member list. If yes, update the heartbeat and 
   // exit. If not, then add an entry for this node in the member 
   // list.
   //
   // Traverse the message for member entries
   while (respMsgIdx < size)
   {
      Address            curNodeAddr;
      int                curNodeAddrId;
      short              curNodeAddrPort;
      MemberListEntry    newMem, member;

      // Copy next member's address information.
      // Initialize node address variable
      curNodeAddr.init();

      // Copy 6 bytes of flat address
      memcpy((char *)&curNodeAddr.addr, 
             (char *)&respMsg[respMsgIdx], 
             sizeof(curNodeAddr.addr));

      // Copy four bytes of ID and update message byte pointer
      memcpy(&curNodeAddrId, 
             (char *)&respMsg[respMsgIdx], 
	     sizeof(int));
      respMsgIdx += sizeof(int);

      // Copy two bytes of Port and update message byte pointer
      memcpy(&curNodeAddrPort, 
             (char *)&respMsg[respMsgIdx], 
	     sizeof(short));
      respMsgIdx += sizeof(short);

      // Update message byte pointer to account for 1 unknown byte
      respMsgIdx++;

      // Copy the heartbeat information.
      memcpy(&heartBeat, 
             (char *)&respMsg[respMsgIdx],
	     sizeof(long));
      respMsgIdx += sizeof(long);

      // Check if the address of the current member node
      // in the received JOINREP message is same as this
      // node's address. If not, then do other processing,
      // else move to the next member node in the received
      // JOINREP message
      if (strcmp(curNodeAddr.addr, memberNode->addr.addr))
      {
         // Check if current node exists in member list.
         // If not, then add this entry and move to the
         // next member node in the received JOINREP message
         insertNodeIntoMemberList(&curNodeAddr,
		                  curNodeAddrId,
			          curNodeAddrPort,
			          heartBeat);
      }
   }
}
