#include "btree_mgr.h"
#include <stdint.h>
#include "buffer_mgr.h"
#include<stdlib.h>
#include<assert.h>
#include <memory.h>
//
// Created by Jiaxin Dai on 11/19/20.
//




// at most 20 keys for each node
#define MAXKEYSPACE 15


/*
* typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;
*/

/*
Use the bitmap struct in assign3 to store the disk blocks state
if 1 then the block is occupied, 0 if the block is free
Every time we need to creat a new tree node, we need to find a free
block first to store it.
This is because one node located in one block
*/


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//Bit map in the beginning of a page block, if the slot is free the bit mapped to it is 0, else is 1
typedef struct bitvector_s {
	uint32_t numEntries; // length of how many record we have, each record is one bit in char.one char has 8 bits
	uint32_t lenBytes;  // length of databits
	uint8_t* databytes; // a char array where we put bits in here, something like 010000110
}BitVector;


//creat a bitvector which can contain number of numEntries in total
BitVector* ConstructorBitVector(uint32_t numEntries);
/*{

	BitVector* this = malloc(sizeof(BitVector));
	this->numEntries = numEntries;
	this->lenBytes = numEntries / 8;
	if (numEntries % 8 > 0) numEntries += 1;// need one more byte 
	this->databytes = malloc(this->lenBytes * sizeof(uint8_t));
	memset(this->databytes, 0, this->lenBytes);// set defalut value 0
	return this;
}*/
// delete and free memory
void DeletorBitVector(BitVector* this);
/*{
	if (this->lenBytes > 0)
	{
		free(this->databytes);
	}
	free(this);
	this = NULL;
}*/
//// get new SerializedBV from BitVector, BitVector this, won't be deleted,
// return the length of SerializedBV
uint32_t SerializeBitVector(BitVector* this, uint8_t** serializedBV);
/*{
	if (!this || !serializedBV)
	{
		printf("Null pointer in SerializeBitVector\n");
		return -1;
	}

	*serializedBV = (uint8_t*)malloc(8 + (this->lenBytes));
	//memcpy(*serializedBV, this->numEntries, sizeof(uint32_t));
	//memcpy(*serializedBV+4, this->lenBytes, sizeof(uint32_t));
	*((uint32_t*)*serializedBV) = this->numEntries;
	*((uint32_t*)(*serializedBV + 4)) = this->lenBytes;
	memcpy(*serializedBV + 8, this->databytes, this->lenBytes);
	return 4 + 4 + (this->lenBytes);
}*/

// get new BitVector from SerializedBV, SerializedBV won't be deleted,
BitVector* DeSerializeBitVector(uint8_t* SerializedBV);/* {
	if (SerializedBV == NULL)
	{
		printf("Null pointer in DeSerializeBitVector\n");
		return NULL;
	}
	uint32_t numEntries = *((uint32_t*)SerializedBV);
	BitVector* this = ConstructorBitVector(numEntries);
	uint32_t numbytes = *((uint32_t*)(SerializedBV + 4));
	this->lenBytes = numbytes;
	memcpy(this->databytes, SerializedBV + 8, numbytes);
	return this;

}*/
/// ///////////////////////
//get a bit value, 0 for false, 1 for true. other value for error
int GetBit(BitVector* this, uint32_t index);/*
{
	if (this == NULL) {
		printf("this point is NULL in BitVEctor::SetBit\n");
		return -1;
	}
	else if (index >= this->numEntries) {
		printf("Out of bound in BitVEctor::SetBit\n");
		return -2;
	}

	// find the BYTE where this bit is located in
	uint8_t databyte;
	databyte = this->databytes[index / 8];
	// find the bit then
	uint32_t indexInByte = index % 8;
	// a mask code to get the bit value;
	// say if we want the 4th(index is 3) of 0010 1010
	// put mask code as 0000 0001, left shift 4 , become 0000 1000
	//0000 1000 & 0010 1010 , And opration to get th  4 th byte, then right shift it to
	//first bit. Coincert is back to int value, if it is 1, the it is true, 0 is false
	uint8_t mask = 1 << (7 - indexInByte);

	uint8_t value = (databyte & mask);
	value = value >> (7 - indexInByte);
	return value;
}*/

// set the bit, 0 for success, other for error 
int SetBit(BitVector* this, uint32_t index, bool value);/*
{
	if (this == NULL) {
		printf("this point is NULL in BitVEctor::SetBit\n");
		return -1;
	}
	else if (index >= this->numEntries) {
		printf("Out of bound in BitVEctor::SetBit\n");
		return -2;
	}
	// find the BYTE where this bit is located in
	uint8_t databyte;
	databyte = this->databytes[index / 8];
	// find the bit then
	uint32_t indexInByte = index % 8;
	// a mask code to get the bit value;
	// say if we want the 4th(index is 3) of 0010 1010
	// put mask code as 0000 0001, left shift 4 , become 0000 1000
	//0000 1000 & 0010 1010 , And opration to get th  4 th byte, then right shift it to
	//first bit. Coincert is back to int value, if it is 1, the it is true, 0 is false
	uint8_t mask = 1 << (7 - indexInByte);
	if (value) {
		//true
		databyte = databyte | mask;//  0010 1010 | 0000 1000 , set 4 th bit to 1 any way
	}
	else {
		//false
		databyte = databyte & (~mask);//0010 1010 & (1111 0111),set 4 th bit to 0 any way
	}
	// put data byte back to data array
	this->databytes[index / 8] = databyte;
	return 0;
}*/

//find the first false bit in bit vector
//we use this fucntion to find a free block and free slot
int FindFirst0BitVector(BitVector* this);/*
{
	if (this == NULL) {
		printf("this point is NULL in BitVEctor::FindFirst0BitVector\n");
		return -2;
	}
	uint32_t freeindes = -1;
	for (uint32_t i = 0; i < this->numEntries; i++)
	{
		if (!GetBit(this, i))
		{
			freeindes = i;
			break;
		}
	}
	return freeindes;
}*/
////////////////////end of class BitVector

typedef BitVector BlockUseFlag;// BlockUseFlag is used to indicate if a Block is occupied by a node



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



//this enum indicate a B+ tree node is leaf node or non leaf node
typedef enum TreeNodeType_enum
{
	LEAF = 0,
	NONLEAF = 1
}TreeNodeType;


////////////////////

typedef int Key_type;// record type, since we only support int, the key of the record should be int
						// it may defined as other type
////Like  DataType, we use a unino to store the value of a node.
//A tree node has two part, key and value, if its a nonleaf node, the key must be it's child's block number, with this block number
// we can read the child node from disk
//If this node is leaf node, the value of this node must be the data record it  stored.
//This type is stored at a block one by one, each one is a record in assign 3's record manager
typedef union nodevalue_union {
	int child_index;
	RID value;
} Nodevalue;
//////////////////

//this struct represents a tree node in memory
typedef struct TreeNode_inMem_struct
{
	int self; // which block this Treenode located in disk
	int parent;//which block its parent node located in
	int left; // where left node, same as above..
	int right;// right ... like above

	TreeNodeType nodetype;// leafnode nonleaf node?

	int numKeys;// how many values this node have
	Key_type* keys;// keys
	Nodevalue* nodevalues;// NOTE: one more value, number of values= numkeys+1

}TreeNode;

// creat a new TreeNode with the blocknum where it located in disk
TreeNode* Constructor_TreeNode(int blocknum, TreeNodeType nodetype)
{
	TreeNode* this = malloc(sizeof(TreeNode));
	this->self = blocknum;
	this->parent = -1;
	this->left = -1;
	this->right = -1;
	this->nodetype = nodetype;
	this->numKeys = 0;
	// won't allocate memory for keys
	this->keys = malloc(MAXKEYSPACE * sizeof(Key_type));
	this->nodevalues = malloc((MAXKEYSPACE + 1) * sizeof(Key_type));
	return this;
}

void DeConstructor_TreeNode(TreeNode* treenode)
{
	if (treenode == NULL)return;
	//if (treenode->numKeys > 0) {
	free(treenode->keys);
	free(treenode->nodevalues);
	//}
	free(treenode);
	treenode = NULL;
}


size_t  Serialize_Treenode(TreeNode* treenode, char** treeNode_data) {
	size_t data_len = 0;
	data_len += 4 * sizeof(int);// self .....right  4 block number
	data_len += sizeof(TreeNodeType);
	data_len += sizeof(int);//numkeys

	//remember that we have one more values than the number of key
	//   key1 key2
	//  /    |    \
	//Value1  2    3       for non leaf node

	//   key1   key2
	//    |      |     |
	//   value 1  2    dummy, for leaf node still numkey+1

	data_len += treenode->numKeys * (sizeof(Key_type));
	data_len += (treenode->numKeys+1 )*sizeof(Nodevalue);
	*treeNode_data = malloc(data_len);
	char* ptr = *treeNode_data;
	size_t offset = 0;
	*((int*)(ptr + offset)) = treenode->self;
	offset += sizeof(int);

	*((int*)(ptr + offset)) = treenode->parent;
	offset += sizeof(int);

	*((int*)(ptr + offset)) = treenode->left;
	offset += sizeof(int);

	*((int*)(ptr + offset)) = treenode->right;
	offset += sizeof(int);

	memcpy(ptr + offset, &(treenode->nodetype), sizeof(TreeNodeType));
	offset += sizeof(TreeNodeType);

	*((int*)(ptr + offset)) = treenode->numKeys;
	offset += sizeof(int);

	memcpy(ptr + offset, treenode->keys, sizeof(Key_type) * treenode->numKeys);
	offset += sizeof(Key_type) * treenode->numKeys;

	memcpy(ptr + offset, treenode->nodevalues, sizeof(Nodevalue) * (treenode->numKeys + 1));
	offset += sizeof(Nodevalue) * (treenode->numKeys + 1);

	return data_len;
}

// return a allocated node, and won't free treeNode_data
TreeNode* DeSerialize_Treenode(char* treeNode_data) {

	size_t data_len = 0; // how many bytes we need to allocated for Tree node
	data_len += 4 * sizeof(int);// self .....right  4 block number
	data_len += sizeof(TreeNodeType);
	// Ok, we need to know how many keys in this node
	int numkey = *((int*)(treeNode_data + data_len));
	data_len += sizeof(TreeNodeType);
	data_len += numkey * (sizeof(Key_type));
	data_len += (numkey + 1) * sizeof(Nodevalue);

	TreeNode* this = Constructor_TreeNode(0,NONLEAF);

	size_t offset = 0;
	this->self = *((int*)(treeNode_data + offset));
	offset += sizeof(int);

	this->parent = *((int*)(treeNode_data + offset));
	offset += sizeof(int);

	this->left = *((int*)(treeNode_data + offset));
	offset += sizeof(int);

	this->right = *((int*)(treeNode_data + offset));
	offset += sizeof(int);

	this->nodetype = *((TreeNodeType*)(treeNode_data + offset));
	offset += sizeof(TreeNodeType);

	this->numKeys = *((int*)(treeNode_data + offset));
	offset += sizeof(int);

	memcpy(this->keys, treeNode_data + offset, sizeof(Key_type) * this->numKeys);
	offset += sizeof(Key_type) * this->numKeys;

	memcpy(this->nodevalues, treeNode_data + offset, sizeof(Nodevalue) * (this->numKeys + 1));
	offset += sizeof(Nodevalue) * (this->numKeys + 1);

	return this;
}




// all content btree info defined here, this is themgmtData of BTreeHandle
typedef struct Bplustree_Imp_struct
{
	//used in helper function
	int numNodes;// how many nodes, includes nonleaf and leaf nodes
	int numEntries;// how many entries, i.e. the values leaf nodes have, 

	int rootnode;// block number of root node
	//root node is the only thing we need, we can find any key from the root

	int MAXNUMKEY;// Max number of keys,for underflow and overflow
	BitVector* bitvector;// indicated which block is free which is not
	///////////////structs that use fordealing with disk
	BM_BufferPool* bufferPool;
	BM_PageHandle* pageHandler;
}BplusTree;

//return a BplusTree* with allocated memory
BplusTree* Constructor_BplusTree(int MAXNUMKEY, BM_BufferPool* bufferPool, BM_PageHandle* pageHandler)
{
	BplusTree* bplustree = malloc(sizeof(BplusTree));
	bplustree->numNodes = 0;
	bplustree->numEntries = 0;
	bplustree->rootnode = -1;//no root node 
	bplustree->MAXNUMKEY = MAXNUMKEY;
	bplustree->bitvector = ConstructorBitVector(1024);// manage 1024 block,need 4+4+256 byte, we have 4k in a block
	bplustree->bufferPool = bufferPool;
	bplustree->pageHandler = pageHandler;
	return bplustree;
}

//allocated a treenode from pageblock blocknum
TreeNode* GetNodeFromDisk(BTreeHandle* tree, int blocknum)
{

	BplusTree* bplustree = tree->mgmtData;
	//debug
	if (blocknum < 0) {
		return NULL;
	}
	//
	// free  bufferpool and pagehandle in bplustree
	BM_BufferPool* bufferpool = bplustree->bufferPool;
	BM_PageHandle* pagehandle = bplustree->pageHandler;

	pinPage(bufferpool, pagehandle, blocknum);

	TreeNode* node = DeSerialize_Treenode(pagehandle->data);
	unpinPage(bufferpool, pagehandle);
	return node;
}

// update node in disk won't free node
void UpdateNodeinDisk(BTreeHandle* tree, TreeNode* node)
{
	BplusTree* bplustree = tree->mgmtData;
	BM_BufferPool* bufferpool = bplustree->bufferPool;
	BM_PageHandle* pagehandle = bplustree->pageHandler;
	int blocknum = node->self;
	char* seri_data;
	size_t length = Serialize_Treenode(node, &seri_data);
	pinPage(bufferpool, pagehandle, blocknum);
	memcpy(pagehandle->data, seri_data, length);
	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);
	SetBit(bplustree->bitvector, blocknum, 1);// make this block is used
	free(seri_data);
}

void Deconstructor_BplusTree(BplusTree* bplustree)
{
	//delete bplustree->bitvector
	DeletorBitVector(bplustree->bitvector);
	//leave bufferpool and pagehandle alone, free them outside
	free(bplustree);
	bplustree = NULL;
	return;
}

//serialize bplus tree to *seri_data, return length of the data, will allocate memory for seri_data
size_t Serialize_Bplustree(BplusTree* bplustree, char** seri_data_ptr)
{
	size_t lenBtree = sizeof(int) * 4;
	char* bvector_data;
	size_t lenBvector = SerializeBitVector(bplustree->bitvector, &bvector_data);
	lenBtree += lenBvector;

	*seri_data_ptr = malloc(lenBtree);
	size_t offset = 0;
    char* seri_data=*seri_data_ptr;


	*((int*)seri_data) = bplustree->numNodes;
	offset += (sizeof(bplustree->numNodes));

	*((int*)(seri_data + offset)) = bplustree->numEntries;
	offset += (sizeof(bplustree->numNodes));

    *((int*)(seri_data + offset)) = bplustree->rootnode;
	offset += (sizeof(bplustree->rootnode));

    *((int*)(seri_data + offset))= bplustree->MAXNUMKEY;
	offset += (sizeof(bplustree->MAXNUMKEY));

	memcpy(seri_data + offset, bvector_data, lenBvector);

	free(bvector_data);// these data is copied, free this mem block
	return lenBtree;
}


//DeSerialize bplus tree, won't free source data seri_data
BplusTree* DeSerialize_Bplustree(char* seri_data, BM_BufferPool* bufferPool, BM_PageHandle* pageHandler)
{

	BplusTree* bplustree = malloc(sizeof(BplusTree));
	size_t offset = 0;
	bplustree->numNodes = *((int*)(seri_data + offset));
	offset += sizeof(bplustree->numNodes);

	bplustree->numEntries = *((int*)(seri_data + offset));
	offset += sizeof(bplustree->numEntries);

	bplustree->rootnode = *((int*)(seri_data + offset));
	offset += sizeof(bplustree->rootnode);

	bplustree->MAXNUMKEY = *((int*)(seri_data + offset));
	offset += sizeof(bplustree->MAXNUMKEY);

	bplustree->bitvector = DeSerializeBitVector(seri_data + offset);// this func will allocate a new BitVector;

	bplustree->bufferPool = bufferPool;
	bplustree->pageHandler = pageHandler;

	//won't free seri_data
	return bplustree;
}

//find the first free block, the new node will be stored here
//return a unused block num, and update numNode in btree
int CreateNewNodeinDisk(BplusTree* bptree)
{
	int blocknum = FindFirst0BitVector(bptree->bitvector);
	SetBit(bptree->bitvector, blocknum, 1);
	bptree->numNodes++;
	return blocknum;
}

//destroy a node in Disk,
//Just set the flag of this block in bitmap to 0
// and update numNode in btree
void DestoryNodeinDisk(BplusTree* bptree, int blocknum)
{

	SetBit(bptree->bitvector, blocknum, 0);
	bptree->numNodes--;
}


RC initIndexManager(void* mgmtData)
{
	return RC_OK;
}

RC shutdownIndexManager()
{
	return RC_OK;
}

RC createBtree(char* idxId, DataType keyType, int n)
{
	//creat a page file first, idxid is used as the page file name
	if (createPageFile(idxId) != RC_OK)
	{
		printf(" creatBtree: Cannot creat page file\n");
		return RC_FILE_NOT_FOUND;
	}

	//open a buffer pool manipulate data in created pagefile
	BM_BufferPool* bufferpool = malloc(sizeof(BM_BufferPool));
	BM_PageHandle* pagehandle = MAKE_PAGE_HANDLE();
	if (initBufferPool(bufferpool, idxId, 25, RS_FIFO, NULL) != RC_OK)
	{
		// one page is 4KB, use 25 pages i.e. 100kb
		printf("ERROR: creatBtree : initBufferPool failed\n");
		free(bufferpool);//free before return
		free(pagehandle);
		return RC_FILE_NOT_FOUND;
	}

    //init bplustree
	BplusTree* bptree = Constructor_BplusTree(n, bufferpool, pagehandle);
	SetBit(bptree->bitvector, 0, 1);// !!!!!!!!!first block is occupied by tree meta
	char* btree_data;
	size_t len_btree_data = Serialize_Bplustree(bptree, &btree_data);

	pinPage(bufferpool, pagehandle, 0);// btree meta info allocated in first page

	//first write datatype
	size_t offset = 0;
	memcpy(pagehandle->data, &keyType, sizeof(DataType));
	offset += sizeof(keyType);
	// write bplus tree
	memcpy(pagehandle->data + offset, btree_data, len_btree_data);

	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);
	free(btree_data);

	Deconstructor_BplusTree(bptree);// delete bptree

	forceFlushPool(bufferpool);
	free(bufferpool);
	free(pagehandle);
	return RC_OK;
}

RC openBtree(BTreeHandle** tree, char* idxId)
{
	//open a buffer pool manipulate data in crearted [agefile
	BM_BufferPool* bufferpool = malloc(sizeof(BM_BufferPool));
	BM_PageHandle* pagehandle = MAKE_PAGE_HANDLE();
	if (initBufferPool(bufferpool, idxId, 25, RS_FIFO, NULL) != RC_OK)
	{
		// one page is 4KB, use 25 pages i.e. 1MB
		printf("ERROR: openBtree : initBufferPool failed\n");
		free(bufferpool);//free before return
		free(pagehandle);
		return RC_FILE_NOT_FOUND;
	}
	*tree = malloc(sizeof(BTreeHandle));
	(*tree)->idxId = idxId;

	pinPage(bufferpool, pagehandle, 0);
	//first write datatype
	size_t offset = 0;
	memcpy(&((*tree)->keyType), pagehandle->data, sizeof(DataType));
	offset += sizeof(DataType);
	// then the our defined bplustree
	BplusTree* bplustree = DeSerialize_Bplustree(pagehandle->data + offset, bufferpool, pagehandle);
	(*tree)->mgmtData = bplustree;
	unpinPage(bufferpool, pagehandle);
	return RC_OK;
}

RC closeBtree(BTreeHandle* tree)
{
	BplusTree* bplustree = tree->mgmtData;

	// free  bufferpool and pagehandle in bplustree
	BM_BufferPool* bufferpool = bplustree->bufferPool;
	BM_PageHandle* pagehandle = bplustree->pageHandler;

	//update btree again in page 0
	pinPage(bufferpool, pagehandle, 0);// btree meta info allocated in first page

	size_t offset = 0;
	offset += sizeof(DataType);// not likely changed, just skip
	// update bplus tree
	char* btree_data;
	size_t len_btree_data = Serialize_Bplustree(tree->mgmtData, &btree_data);
	memcpy(pagehandle->data + offset, btree_data, len_btree_data);

	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);

	forceFlushPool(bufferpool);//write buffer 2 disk before we delete
	shutdownBufferPool(bufferpool);
	free(bufferpool);// need free these two outside
	free(pagehandle);
	//deconstruct BplusTree
	Deconstructor_BplusTree(bplustree);
	tree = NULL;
	return RC_OK;
}

RC deleteBtree(char* idxId)
{
	RC rc = destroyPageFile(idxId);
	if (rc != RC_OK) {
		printf("ERROR: deleteBtree destoryPageFile return ERROR\n");
		return rc;
	}
	return RC_OK;
}

RC getNumNodes(BTreeHandle* tree, int* result)
{
	BplusTree* bplustree = tree->mgmtData;
	*result = bplustree->numNodes;
	return RC_OK;
}

RC getNumEntries(BTreeHandle* tree, int* result)
{
	BplusTree* bplustree = tree->mgmtData;
	*result = bplustree->numEntries;
	return RC_OK;
}

RC getKeyType(BTreeHandle* tree, DataType* result)
{
	*result = tree->keyType;
	return RC_OK;
}


//for nonleafnode
//CASE 1,if return 1 then the key is on the node's index th subnode
// to find this node it is easy just use node.values[i+1];
// NOTE: 
	 //   key1 key2
	//  /    |    \
	//Value1  2    3       for non leaf node
// node.keys=[key1,key2]
// node.values=[1,   2,  3]
 //Case 2: if scanned finish but cann't find a i, this means this key is bigger than all nodes in node.keys
 // return index should be the [node->numkey], return -1
//Case 3: not found but found a bigger return 0
int SearchKeyinNonLeafNode(Key_type key, TreeNode* node, int* index)
{
	//assert(key->dt != DT_INT);// only support int

	/*
	* use dummy linear search,scan from left to right
	*/
	size_t i = 0;
	for (; i < node->numKeys; i++)
	{
		if ((node->keys[i]) == key)
		{
			*index = i;//found
			return 1;
		}
		else if ((node->keys[i]) > key)
		{
			*index = i;//not found
			return 0;
		}
	}
	*index = i;//i=numkey at this time:Case 2
	return -1;

}

//for leafnode
// NOTE: 
	 //key1 key2
	//  |   |    
	//Value1  2
//Case1: found return index and 1;
//Case2: nofound, but found a key bigger than this key, return 0(no found),and the first index bigger than it
//         (this is uselful when we try to add key to this node)
//Case3: nofound and no bigger key in this node, the ket input is the biggest, return -1 (no found, and scaaned to end)
// return index=numkey (if we are try to add a key, the key MAY be added here)
int SearchKeyinLeafNode(Key_type key, TreeNode* node, int* index)
{
	//assert(key->dt != DT_INT);// only support int

	/*
	* use dummy linear search,scan from left to right
	*/
	size_t i = 0;
	for (; i < node->numKeys; i++)
	{
		if ((node->keys[i]) == key)
		{
			*index = i;//found
			return 1;
		}
		else if ((node->keys[i]) > key)
		{
			*index = i;//found a bigger
			return 0;
		}
	}
	*index = i;// scanned to end, still no found
	return -1;

}

// Find a key in nonleafNode, return child node block number this key in
int FindChildinNonLeafNode(Key_type key, TreeNode* node)
{
	int keyindex;
	int rt = SearchKeyinNonLeafNode(key, node, &keyindex);
	if (rt == 1)
	{
		// found a key  equal than this key, 
		// we should go right child of this index
		return keyindex+1;
	}
	else if (rt == 0) {
		// found a key bigger than this , go left
		return keyindex;
	}else{
		return node->numKeys;
	}

}
// Find a key in leafNode, return 1 if found and set index
// if not found just return -1
int FindinLeafNode(Key_type key, TreeNode* node)
{
	int dummyindex;
	int rt = SearchKeyinLeafNode(key, node, &dummyindex);
	if (rt == 1)
	{
		return dummyindex;
	}
	else {
		return -1;
	}
}

//find a key
RC findKey(BTreeHandle* tree, Value* key, RID* result)
{
	BplusTree* bplustree = tree->mgmtData;
	if (bplustree->numEntries == 0) {
		//empty tree
		return RC_IM_KEY_NOT_FOUND;
	}

	// get back root node from disk
	TreeNode* root = GetNodeFromDisk(tree, bplustree->rootnode);

	TreeNode* node = root;// start from node
	RC ret=RC_OK;
	while (node != NULL)
	{
		if (node->nodetype == NONLEAF)
		{
			// search and find which subnode we should search into
			int index= FindChildinNonLeafNode(key->v.intV, node);
			
			TreeNode* newnode = GetNodeFromDisk(tree, node->nodevalues[index].child_index);
			DeConstructor_TreeNode(node);//release the old one
			node = newnode;//set the new one and keep searching
		}
		else {
			//search in this leaf node,
			// find the first subnode that bigger or equal to the key
			// if key < 
			int index = FindinLeafNode(key->v.intV, node);
			if (index != -1)
			{//found
				ret = RC_OK;// set return value
				//fetch value
				result->page = node->nodevalues[index].value.page;
				result->slot = node->nodevalues[index].value.slot;
			}
			else {
				ret = RC_IM_KEY_NOT_FOUND;
			}
			DeConstructor_TreeNode(node);
			break;
		}
	}
	return ret;
}

//same like tree.numnodes++
void IncrOneEntriesinTree(BTreeHandle* tree)
{
	BplusTree* bplustree = tree->mgmtData;
	bplustree->numEntries++;
}

void DecriOneEntriesinTree(BTreeHandle* tree) {
	BplusTree* bplustree = tree->mgmtData;
	bplustree->numEntries--;
}

void SetLeafKeyValue(TreeNode* node, int pos, Value* key, RID rid)
{
	node->keys[pos] = key->v.intV;
	node->nodevalues[pos].value.page = rid.page;
	node->nodevalues[pos].value.slot = rid.slot;
}
void SetLeafKey(TreeNode* node, int pos, Key_type key, RID rid)
{
	node->keys[pos] = key;
	node->nodevalues[pos].value.page = rid.page;
	node->nodevalues[pos].value.slot = rid.slot;
}
void SetNonLeafKeyValue(TreeNode* node, int pos, Value* key, int left, int right)
{
	node->keys[pos] = key->v.intV;
	node->nodevalues[pos].child_index = left;
	node->nodevalues[pos + 1].child_index = right;
}

void SetNonLeafKey(TreeNode* node, int pos, Key_type key, int left, int right)
{
	node->keys[pos] = key;
	node->nodevalues[pos].child_index = left;
	node->nodevalues[pos + 1].child_index = right;
}


void InsertInNonleafNode() {
	//same like insert to leaf node, but if split, split key will be pushed to parent,won't be copied to new right node
}



//insert the split key to parent
void InserttoParentAfterSplit(BTreeHandle* tree, Key_type splitkey, TreeNode* leftnode, TreeNode* rightnode)
{

	BplusTree* bplustree = tree->mgmtData;
	// if no parent, simple creat a new node as parent, and return
	if (leftnode->parent == -1)
	{
		int parentBlock = CreateNewNodeinDisk(bplustree); //allocate space in disk
		TreeNode* parentnode = Constructor_TreeNode(parentBlock, NONLEAF);// allocate space in memory
		bplustree->rootnode = parentnode->self;// new root node

		parentnode->numKeys = 1;
		parentnode->keys[0] = splitkey;
		parentnode->nodevalues[0].child_index = leftnode->self;/*different from leaf node, 1key have 2 child node*/
		parentnode->nodevalues[1].child_index = rightnode->self;

		leftnode->parent = parentnode->self;
		rightnode->parent = parentnode->self;

		UpdateNodeinDisk(tree, parentnode);
		DeConstructor_TreeNode(parentnode);
		return;
	}
	else {
		// else search a place in parent to insert the new split key

		TreeNode* parentnode = GetNodeFromDisk(tree, leftnode->parent);

		if (parentnode->numKeys != bplustree->MAXNUMKEY)
		{// if no overflow just add and return 
			int insertPos;
			int rtCode = SearchKeyinNonLeafNode(splitkey, parentnode, &insertPos);
			if (/*rtCode == */1)
			{// found a key bigger than it at pos
				// move key[pos,end]->ky[pos,end+1]
				//  [ 1, 3]           add  [2]              [1,  2, 3 ]
				//   |   |   \              | |      --->   |    |   |  \
				//  [<1] [1,3] [>3]       [1,2][2,3]       [<1][1,2][2,3] [>3]
			// So we need modify two key value now
			//Note: node [1,3 ] is gone, there's no such node now, it will be replaced as [2,3];
				for (size_t i = parentnode->numKeys; i > insertPos; i--)
				{
					SetNonLeafKey(parentnode, i, parentnode->keys[i-1], parentnode->nodevalues[i - 1].child_index, parentnode->nodevalues[i].child_index);
				}

				SetNonLeafKey(parentnode, insertPos, splitkey, leftnode->self, rightnode->self);

				//Update sbling info for the two new node
				//ConnectSiblings(tree, insertPos, leftnode, rightnode, parentnode);
				parentnode->numKeys++;
				UpdateNodeinDisk(tree, parentnode);
				DeConstructor_TreeNode(parentnode);
				return;
			}
			else {
				// no bigger key, insert at numkey
				//same like above

			}

		}
		else {
			//else have to split again,
			   //then push the split to parent of parent

			   // First, find split index, call it pushup

			int insertPos;
			int rtCode = SearchKeyinNonLeafNode(splitkey, parentnode, &insertPos);
			for (size_t i = parentnode->numKeys; i > insertPos; i--)
			{
				SetNonLeafKey(parentnode, i, parentnode->keys[i - 1], parentnode->nodevalues[i - 1].child_index, parentnode->nodevalues[i].child_index);;
			}

			SetNonLeafKey(parentnode, insertPos, splitkey, leftnode->self, rightnode->self);

			// now try to find pop up key
			// found a key bigger than it at pos                                          [2]
				// move key[pos,end]->ky[pos,end+1]                                      |    |
				//  [ 1, 3]           add  [2]              [1,  2, 3 ]              [1]      [3]
				//   |   |   \              | |      --->   |    |   |  \    ------> |  |    |   |
				//  [<1] [1,3] [>3]       [1,2][2,3]       [<1][1,2][2,3] [>3]     [<1][1,2][2,3][>3]
				//                    
			 //  split as keys [0, popup-1]   popup [popup+1, numkey]
			 //          values[0,.........   popup][popup+1, numkey,numkey+1]
			 //                         left                right


			// for[0,1,2,3] and n=3, pop n/2+1=2 pop 2, for[0,1,2,3,4] and n=4, pop 4/2=2 pop2
			int popup;
			if (parentnode->numKeys % 2 == 0) {
				//even 
				popup = parentnode->numKeys / 2;
			}
			else {//odd
				popup = parentnode->numKeys / 2 + 1;
			}

			TreeNode* LeftParent = parentnode;// use ParentNode as the new LeftParentPart, and creat a new right Parent node
			int rightpartBlock = CreateNewNodeinDisk(bplustree);//allocate in disk
			TreeNode* RightParent = Constructor_TreeNode(rightpartBlock, NONLEAF);
			//connect eachother
			int oldright = parentnode->right;
			if (oldright != -1)
			{
				RightParent->right = oldright;
				TreeNode* rightrightnode = GetNodeFromDisk(tree, oldright);
				rightrightnode->left = RightParent->self;
				UpdateNodeinDisk(tree, rightrightnode);
				DeConstructor_TreeNode(rightrightnode);
			}

			LeftParent->right = RightParent->self;
			RightParent->left = LeftParent->self;
			
			RightParent->numKeys = parentnode->numKeys / 2;
			LeftParent->numKeys = popup;
			// copy key and values
			for (size_t i = 0; i < RightParent->numKeys; i++)
			{
				SetNonLeafKey(RightParent,  i, parentnode->keys[i + popup+1], parentnode->nodevalues[i + popup+1].child_index,
					parentnode->nodevalues[i + popup + 2].child_index);
			}
			// popup and set new parent
			InserttoParentAfterSplit(tree, parentnode->keys [ popup], LeftParent, RightParent);
			//update nodes in disk and free treenode in memory
			UpdateNodeinDisk(tree, LeftParent);// store new root in disk
			UpdateNodeinDisk(tree, RightParent);// store new root in disk
			DeConstructor_TreeNode(LeftParent);
			DeConstructor_TreeNode(RightParent);
		}

	}

}

RC InsertInLeafnode(BTreeHandle* tree, TreeNode* node, int atEnd, int pos, Key_type key, RID rid)
{
	BplusTree* bplustree = tree->mgmtData;
	// return a place where we may addkey
					// first we need to check if node need split


		// operation==0---> insert before pos
		// so we need to move [pos,end]->[pos+1,end +1];
		// operation==-1, --> insert at the end. i.e. pos=numkey
	if (node->numKeys != bplustree->MAXNUMKEY) {
		// not full, just do simple add, no split
		/*if (atEnd == 0) {// insert at end
			//node->keys[node->numKeys]
			SetLeafKeyValue(node, pos, key, rid);// insert before pos
		}
		else {// insert at middle
			//move[pos, end]->[pos + 1, end + 1];
			// memcpy(node->keys[pos+1],node->keys[pos+1]) may have problem? we do insite copy here
			for (int i =node->numKeys; i > pos; i++)
			{
				SetLeafKeyValue(node, i, node->keys[i - 1], node->nodevalues->value);
			}

			SetLeafKeyValue(node, pos, key, rid);// insert at pos
		}*/

		for (int i = node->numKeys; i > pos; i--)// lookes like this snipcode is same as above, need a try
		{
			SetLeafKey(node, i, node->keys[i - 1], node->nodevalues->value);
		}

		SetLeafKey(node, pos, key, rid);// insert at pos
		node->numKeys++;
		UpdateNodeinDisk(tree, node);
		DeConstructor_TreeNode(node);
	}
	else {
		// need a split, this is more complex,
		/*
		* lets say we have [0,1,2,4,5], and a key 3 to be insert, maxkeynum==numkey==5
		  this should be split evenly as [0,1,2],[3,4,5], split=((numkey+1)/2)=3,(int/2 will go down, this no double)

		  i.e. we creat two new node[0,1,2],[4,5], then simple insert 3 into [4,5], then update parent node things

		  when maxkeynum==even like 4, say we have [0,1,2,4], need insert 3
		  split=(maxnumkey)/2+1=3, split as [0,1,2] [3,4], insert 3 at 4 get [0,1,2],[3,4]
		*/
		//OK, first we need creat a new array as keys and values, as we don't know the insert key will go left or right
		//in example above the 3 always go right

		//the treenode in mem can hold more keys than MAXKEYS, we can still insert in memory
		for (int i = node->numKeys; i > pos; i--)//like above, 
		{
			SetLeafKey(node, i, node->keys[i - 1], node->nodevalues[i-1].value);
		}

		SetLeafKey(node, pos, key, rid);// insert at pos

		// then find th split, as state above, split is the first key will go right tree
		int split = (node->numKeys) / 2 + 1;// if odd 3/2+1=2 split as [0,1],[2,3]
										   // if even 4/2+1=3, [0,1,2],[3,4]
		// Now we have to creat two new leaf node, and copy [0,split-1] to left
		//[split,numkey+1] to right, to be simple, just creat one node as right, use the 
		// orignal one as left

		TreeNode* leftnode = node;
		int rightBlock = CreateNewNodeinDisk(bplustree);
		TreeNode* rightnode = Constructor_TreeNode(rightBlock, LEAF);// creat a leaf node

		//point to each other, parent node thing will be done later
		int oldright = node->right;
		if (oldright != -1)
		{
			rightnode->right = oldright;
			TreeNode* rightrightnode = GetNodeFromDisk(tree, oldright);
			rightrightnode->left = rightnode->self;
			UpdateNodeinDisk(tree, rightrightnode);
			DeConstructor_TreeNode(rightrightnode);
		}
		leftnode->right = rightnode->self;
		rightnode->left = leftnode->self;
		rightnode->parent = node->parent;
		// copy keys now, left part is done,just set right part
		int orginal_nodeKeys = node->numKeys;
		leftnode->numKeys = orginal_nodeKeys / 2 + 1;// left get one more
		rightnode->numKeys = orginal_nodeKeys / 2;
		for (size_t i = 0; i < rightnode->numKeys; i++)
		{
			SetLeafKey(rightnode, i, node->keys[split + i], node->nodevalues[split + i].value);
		}

		//then we need to add the split key to parent node

		InserttoParentAfterSplit(tree, rightnode->keys[0], leftnode, rightnode);

		//update nodes in disk and free treenode in memory
		UpdateNodeinDisk(tree, leftnode);// store new root in disk
		UpdateNodeinDisk(tree, rightnode);// store new root in disk
		DeConstructor_TreeNode(leftnode);
		DeConstructor_TreeNode(rightnode);
	}
	return RC_OK;
}
RC insertKey(BTreeHandle* tree, Value* key, RID rid)
{
	//same like find key, we need to go deep down to leaf node to insert
	//Step 1:
	// if tree is empty, just creat a root node and return
	BplusTree* bplustree = tree->mgmtData;
	RC ret=RC_OK;
	if (bplustree->numNodes == 0)
	{
		//creat a new root node and return
		int blocknum = CreateNewNodeinDisk(bplustree);

		TreeNode* newroot = Constructor_TreeNode(blocknum, LEAF);// creat a leaf node

		newroot->numKeys = 1;
		newroot->keys[0] = key->v.intV;
		newroot->nodevalues[0].value.page = rid.page;
		newroot->nodevalues[0].value.slot = rid.slot;
		UpdateNodeinDisk(tree, newroot);// store new root in disk
		IncrOneEntriesinTree(tree);// update tree header info
		bplustree->rootnode = blocknum;
		DeConstructor_TreeNode(newroot);
		return RC_OK;
	}
	else {
		//find root first, then go deep until a leaf node
		TreeNode* node = GetNodeFromDisk(tree, bplustree->rootnode);

		while (node != NULL)
		{
			if (node->nodetype == NONLEAF)
			{
				// search and find which subnode we should search into
				int index = FindChildinNonLeafNode(key->v.intV, node);
				TreeNode* newnode = GetNodeFromDisk(tree, node->nodevalues[index].child_index);
				DeConstructor_TreeNode(node);//release the old one
				node = newnode;//set the new one and keep searching
			}
			else {
				//search in this leaf node,
				// find the first subnode that bigger or equal to the key
				// if key < 
				int index;
				int rtcode = SearchKeyinLeafNode(key->v.intV, node, &index);
				if (rtcode == 1) {
					// find this key
					ret = RC_IM_KEY_ALREADY_EXISTS;
					DeConstructor_TreeNode(node);
				}
				else {
					ret = InsertInLeafnode(tree, node, rtcode, index, key->v.intV, rid);
					IncrOneEntriesinTree(tree);
				}

				break;
			}
		}
	}
	return ret;
}

void RemoveNonLeafNodeFromNonLeafNode(BTreeHandle* tree, TreeNode* node, int pos)
{
	BplusTree* bplustree = tree->mgmtData;
	// check if the node will underflow

	// if we only have one root node, then just delete,  can't borrow or merge
	if (node->parent == -1) {
		//assert(node->parent != -1);
		for (int i = pos; i < node->numKeys - 1; i++)
		{
			SetNonLeafKey(node, i, node->keys[ i + 1], 
				 node->nodevalues[i+1].child_index, node->nodevalues[i+2].child_index);

		}
		node->numKeys--;
		if (node->numKeys == 0) {
			bplustree->rootnode = node->nodevalues[0].child_index;// let first child be new root
			TreeNode* childnode = GetNodeFromDisk(tree, node->nodevalues[0].child_index);
			childnode->parent = -1;
			UpdateNodeinDisk(tree, childnode);
			DeConstructor_TreeNode(childnode);
			DestoryNodeinDisk(bplustree, node->self);
            
		}
		UpdateNodeinDisk(tree, node);
		DeConstructor_TreeNode(node);
		return;
	}
	int threshold;
	if (bplustree->MAXNUMKEY % 2 == 0) {
		// n=4, when below 2, underflow
		threshold = bplustree->MAXNUMKEY / 2;
	}
	else {
		//n=5,when below 2, underflow
		threshold = bplustree->MAXNUMKEY / 2;
	}

	if (node->numKeys - 1 >= threshold)
	{//no underflow,simple case
		//[0,1,2,3] delete 2 get[0,1,3],pos=2

		for (int i = pos; i < node->numKeys - 1; i++)
		{
			SetNonLeafKey(node, i, node->keys[ i + 1],
				node->nodevalues[i + 1].child_index, node->nodevalues[i + 2].child_index);
		}

		node->numKeys--;

		UpdateNodeinDisk(tree, node);
		DeConstructor_TreeNode(node);
	}
	else {
		// underflow, very complex 
			// 1. try borrow from left
			// 2.  try borrow from right
			// 3.  try merge with left
			// 4.  try merge with right
		TreeNode* LeftNode = GetNodeFromDisk(tree, node->left);
		TreeNode* RightNode = GetNodeFromDisk(tree, node->right);
		TreeNode* ParentNode = GetNodeFromDisk(tree, node->parent);

		// we need to find the index of the node in parant, note: the key in parent node, is just big OR equal than right, less than left
		//     [3,5]  <------ note the five here
		//   |   \     \
		//[1,2] [3,4] [6,7]
		int keyIndexinParent;
		int found = SearchKeyinNonLeafNode(node->keys[0], ParentNode, &keyIndexinParent);

		if (LeftNode != NULL && (LeftNode->numKeys - 1 >= threshold)) {
			// 1. try borrow from left
			//              [3]                        [2]
			//            |     \                     |   \
			// left:[0,1,2] node:[3] ,n=5 -->left:[0,1], node:[2,3], still need update parent node
			// delete value first
			for (int i = pos; i < node->numKeys - 1; i++)
			{
				SetNonLeafKey(node, i, node->keys[i + 1],
					node->nodevalues[i + 1].child_index, node->nodevalues[i + 2].child_index);
			}
			node->numKeys--;

			// borrow from left

				// node job
			for (int i = node->numKeys; i > 0; i--)//copy from end to start
			{
				SetNonLeafKey(node, i, node->keys[ i - 1],
					node->nodevalues[i - 1].child_index, node->nodevalues[i].child_index);
			}
			//SetLeafKeyValue(node, 0, LeftNode->keys, LeftNode->nodevalues[0].value);
			SetNonLeafKey(node, 0, LeftNode->keys[0],
				LeftNode->nodevalues[0].child_index, LeftNode->nodevalues[0+1].child_index);
			node->numKeys++;// get one node from left

				// left node job
			for (int i = 0; i < LeftNode->numKeys - 1; i++)
			{
				SetLeafKey(LeftNode, i, LeftNode->keys [ i + 1], LeftNode->nodevalues[i + 1].value);
			}

			LeftNode->numKeys--;
			//update parent
		// we need to find the index of the node in parant
		// keyIndexinParent won't be parent->numKey
			ParentNode->keys[keyIndexinParent] = node->keys[0];
			UpdateNodeinDisk(tree, node);
			DeConstructor_TreeNode(node);
			UpdateNodeinDisk(tree, LeftNode);
			DeConstructor_TreeNode(LeftNode);
			UpdateNodeinDisk(tree, ParentNode);
			DeConstructor_TreeNode(ParentNode);
			return;
		}
		else if (RightNode != NULL && (RightNode->numKeys - 1 >= threshold)) {
			// 2.  try borrow from right
			//        [2]                                [3]
		   //        |     \                            |   \
			// node:[0]   left:[2,3,4]  ,n=5 --> node:[0,2],right:[3,4] still need update parent node

			//nodejob
			for (int i = pos; i < node->numKeys - 1; i++)
			{
				//SetLeafKeyValue(node, i, node->keys + i, node->nodevalues[i].value);
				SetNonLeafKey(node, i, node->keys [ i + 1],
					node->nodevalues[i + 1].child_index, node->nodevalues[i + 2].child_index);
			}
			node->numKeys--;
			//SetLeafKeyValue(node, node->numKeys, &(RightNode->keys[0]), RightNode->nodevalues[0].value);
			SetNonLeafKey(node, 0, LeftNode->keys[0],
				LeftNode->nodevalues[0].child_index, LeftNode->nodevalues[0 + 1].child_index);
			node->numKeys++;

			//Rightnode job shift all keys left for one position
			for (int i = 0; i < RightNode->numKeys - 1; i++)
			{
				SetLeafKey(RightNode, i, RightNode->keys [ i + 1], RightNode->nodevalues[i + 1].value);
			}
			RightNode->numKeys--;

			//Update parentindex
			ParentNode->keys[keyIndexinParent] = RightNode->keys[0];

			UpdateNodeinDisk(tree, node);
			DeConstructor_TreeNode(node);
			UpdateNodeinDisk(tree, RightNode);
			DeConstructor_TreeNode(RightNode);
			UpdateNodeinDisk(tree, ParentNode);
			DeConstructor_TreeNode(ParentNode);

			return;
		}

		if (LeftNode != NULL) {
			// 3.  try merge with left
			//     [3,5]          delete 3   --->   [5]
			 //   |   \     \                       |   \
		      //[1,2] [,4] [6,7]                  [1,2,4] [6, 7]

			//Left side Job
			for (int i = 0, j = 0; i < node->numKeys; i++)
			{
				if (j != pos) {// skip deletekey
					//SetLeafKeyValue(LeftNode, LeftNode->numKeys + j, node->keys + i, node->nodevalues[i].value);
					SetNonLeafKey(LeftNode, LeftNode->numKeys + j, node->keys [i],
						node->nodevalues[i].child_index, node->nodevalues[i + 1].child_index);
				}
				
			}
			LeftNode->numKeys += node->numKeys - 1;

			LeftNode->right = node->right;
			if (RightNode != NULL)
			{
				RightNode->left = LeftNode->self;
			}

			DestoryNodeinDisk(bplustree, node->self);
			UpdateNodeinDisk(tree, node);
			DeConstructor_TreeNode(node);
			RemoveNonLeafNodeFromNonLeafNode(tree, ParentNode, keyIndexinParent);


		}
		else//(is RightNode is NULL too, this must be root node)
		{// 4.  try merge with right
			//    [3,5]        delete 4      ----> [3]
			//   |   \     \                      |    \
			//[0,1,2] [3,] [6,7]               [0,1,2] [3,6,7]

			//rightside job
			// delete the key in  node first
			for (int i = pos; i < node->numKeys - 1; i++)
			{
				//SetLeafKeyValue(node, i, node->keys + i, node->nodevalues[i].value);
				SetNonLeafKey(node, i, node->keys [i + 1],
					node->nodevalues[i + 1].child_index, node->nodevalues[i + 2].child_index);
			}
			node->numKeys--;
			// copy RightNode
			for (int i = 0; i < RightNode->numKeys - 1; i++)
			{
				//SetLeafKeyValue(node, node->numKeys + i, RightNode->keys + i, RightNode->nodevalues[i].value);
				SetNonLeafKey(node, node->numKeys + i, RightNode->keys [ i],
					RightNode->nodevalues[i].child_index, RightNode->nodevalues[i + 1].child_index);
			}
			node->numKeys += RightNode->numKeys;

			node->right = RightNode->right;
			if (node->right != -1)
			{
				TreeNode* RightRightNode = GetNodeFromDisk(tree, node->right);
				RightRightNode->left = node->self;
				UpdateNodeinDisk(tree, RightRightNode);
				DeConstructor_TreeNode(RightRightNode);
			}

			DestoryNodeinDisk(bplustree, RightNode->self);
			UpdateNodeinDisk(tree, RightNode);
			DeConstructor_TreeNode(RightNode);
			RemoveNonLeafNodeFromNonLeafNode(tree, ParentNode, keyIndexinParent);
		}

	}
}

void RemoveKeyinLeafNode(BTreeHandle* tree, TreeNode* node, int atEnd, int pos, Value* key) {

	BplusTree* bplustree = tree->mgmtData;
	// check if the node will underflow

	// if we only have one root node, then just delete,  can't borrow or merge
	if (bplustree->numNodes==1) {
		assert(node->parent != -1);
		for (int i = pos; i < node->numKeys - 1; i++)
		{
			SetLeafKey(node, i, node->keys [ i + 1], node->nodevalues[i + 1].value);
		}
		node->numKeys--;
		if (node->numKeys == 0) {
			bplustree->rootnode = -1;
			DestoryNodeinDisk(bplustree, node->self);
			
		}
		UpdateNodeinDisk(tree, node);
		DeConstructor_TreeNode(node);
		return;
	}
	int threshold;
	if (bplustree->MAXNUMKEY % 2 == 0) {
		// n=4, when below 2, underflow
		threshold = bplustree->MAXNUMKEY / 2;
	}
	else {
		//n=5,when below 2, underflow
		threshold= bplustree->MAXNUMKEY / 2;
	}

	if (node->numKeys - 1 >= threshold)
	{//no underflow,simple case
		//[0,1,2,3] delete 2 get[0,1,3],pos=2

		for (int i = pos; i < node->numKeys - 1; i++)
		{
			SetLeafKey(node, i, node->keys [ i+1], node->nodevalues[i+1].value);
		}
		
		node->numKeys--;

		UpdateNodeinDisk(tree, node);
		DeConstructor_TreeNode(node);
	}
	else {
	// underflow, very complex 
		// 1. try borrow from left
		// 2.  try borrow from right
		// 3.  try merge with left
		// 4.  try merge with right
		TreeNode* LeftNode = GetNodeFromDisk(tree, node->left);
		TreeNode* RightNode= GetNodeFromDisk(tree, node->right);
		TreeNode* ParentNode= GetNodeFromDisk(tree, node->parent);

		// we need to find the index of the node in parant, note: the key in parent node, is just big OR equal than right, less than left
		//     [3,5]  <------ note the five here
		//   |   \     \
		//[1,2] [3,4] [6,7]
		int keyIndexinParent
		 = FindChildinNonLeafNode(node->keys[0], ParentNode);

		if (LeftNode != NULL && (LeftNode->numKeys - 1>=threshold)) {
			// 1. try borrow from left
			//              [3]                        [2]
			//            |     \                     |   \
			// left:[0,1,2] node:[3] ,n=5 -->left:[0,1], node:[2,3], still need update parent node
			// delete value first
			for (int i = pos; i < node->numKeys - 1; i++)
			{
				SetLeafKey(node, i, node->keys [ i+1], node->nodevalues[i+1].value);
			}
			node->numKeys--;

			// borrow from left
				
				// node job
			for (int i = node->numKeys; i >0; i--)//copy from end to start
			{
				SetLeafKey(node, i, node->keys [ i-1], node->nodevalues[i-1].value);
			}
			SetLeafKey(node, 0, LeftNode->keys[LeftNode->numKeys-1], LeftNode->nodevalues[LeftNode->numKeys - 1].value);
            node->numKeys++;// get one node from left

				// left node job
			/*for (int i = 0; i < LeftNode->numKeys-1; i++)
			{
				SetLeafKey(LeftNode, i, LeftNode->keys [ i+1], LeftNode->nodevalues[i+1].value);
			}*/

			LeftNode->numKeys--;
				//update parent
			// we need to find the index of the node in parant
			// keyIndexinParent won't be parent->numKey
			ParentNode->keys[keyIndexinParent-1] = node->keys[0];
			UpdateNodeinDisk(tree, node);
			DeConstructor_TreeNode(node);
			UpdateNodeinDisk(tree, LeftNode);
			DeConstructor_TreeNode(LeftNode);
			UpdateNodeinDisk(tree, ParentNode);
			DeConstructor_TreeNode(ParentNode);
			return;
		}
		else if(RightNode!=NULL && (RightNode->numKeys - 1 >= threshold)){
			 // 2.  try borrow from right
			 //        [2]                                [3]
			//        |     \                            |   \
			// node:[0]   left:[2,3,4]  ,n=5 --> node:[0,2],right:[3,4] still need update parent node

			//nodejob
			for (int i = pos; i < node->numKeys - 1; i++)
			{
				SetLeafKey(node, i, node->keys [ i+1], node->nodevalues[i+1].value);
			}
			node->numKeys--;
			SetLeafKey(node, node->numKeys, RightNode->keys[0], RightNode->nodevalues[0].value);
			node->numKeys++;

			//Rightnode job shift all keys left for one position
			for (int i = 0; i < RightNode->numKeys - 1; i++)
			{
				SetLeafKey(RightNode, i, RightNode->keys [ i+1], RightNode->nodevalues[i+1].value);
			}
			RightNode->numKeys--;

			//Update parentindex
			ParentNode->keys[keyIndexinParent] = RightNode->keys[0];

			UpdateNodeinDisk(tree, node);
			DeConstructor_TreeNode(node);
			UpdateNodeinDisk(tree, RightNode);
			DeConstructor_TreeNode(RightNode);
			UpdateNodeinDisk(tree, ParentNode);
			DeConstructor_TreeNode(ParentNode);

			return;
		}

		if(LeftNode != NULL ){
			// 3.  try merge with left
			//     [3,5]          delete 3   --->   [5]
		     //   |   \     \                       |   \
		      //[1,2] [,4] [6,7]                  [1,2,4] [6, 7]

			//Left side Job
			for (int i = 0,j=0; i < node->numKeys; i++)
			{
				if (j != pos) {// skip deletekey
					SetLeafKey(LeftNode, LeftNode->numKeys + j, node->keys [ i], node->nodevalues[i].value);
				}
			}
			LeftNode->numKeys += node->numKeys - 1;

			LeftNode->right = node->right;
			if (RightNode != NULL)
			{
				RightNode->left = LeftNode->self;
			}
			
			DestoryNodeinDisk(bplustree, node->self);
			UpdateNodeinDisk(tree, node);
			DeConstructor_TreeNode(node);
			RemoveNonLeafNodeFromNonLeafNode(tree, ParentNode, keyIndexinParent);
			

		}
		else//(is RightNode is NULL too, this must be root node)
		{// 4.  try merge with right
			//    [3,5]        delete 4      ----> [3]
			//   |   \     \                      |    \
			//[0,1,2] [3,] [6,7]               [0,1,2] [3,6,7]

			//rightside job
			// delete the key in  node first
			for (int i = pos; i < node->numKeys - 1; i++)
			{
				SetLeafKey(node, i, node->keys[i+1], node->nodevalues[i+1].value);
			}
			node->numKeys--;
			// copy RightNode
			for (int i = 0; i < RightNode->numKeys; i++)
			{
				SetLeafKey(node, node->numKeys+i, RightNode->keys [ i], RightNode->nodevalues[i].value);
			}
			node->numKeys += RightNode->numKeys;

			node->right = RightNode->right;
			if (node->right != -1)
			{
				TreeNode* RightRightNode = GetNodeFromDisk(tree, node->right);
				RightRightNode->left = node->self;
			}
			
			DestoryNodeinDisk(bplustree, RightNode->self);
			UpdateNodeinDisk(tree, RightNode);
			DeConstructor_TreeNode(RightNode);
			RemoveNonLeafNodeFromNonLeafNode(tree, ParentNode, keyIndexinParent);
		}

	}

}



RC deleteKey(BTreeHandle* tree, Value* key)
{
	//same like find key, we need to go deep down to leaf node to insert
	//Step 1:
	// if tree is empty, just creat a root node and return
	BplusTree* bplustree = tree->mgmtData;
	RC ret=RC_OK;
	if (bplustree->numNodes == 0) {
		return RC_IM_KEY_NOT_FOUND;
	}

	// get back root node from disk
	TreeNode* root = GetNodeFromDisk(tree, bplustree->rootnode);

	TreeNode* node = root;// start from node
	while (node != NULL)
	{
		if (node->nodetype == NONLEAF)
		{
			// search and find which subnode we should search into
			int index = FindChildinNonLeafNode(key->v.intV, node);
			TreeNode* newnode = GetNodeFromDisk(tree, node->nodevalues[index].child_index);
			DeConstructor_TreeNode(node);//release the old one
			node = newnode;//set the new one and keep searching
		}
		else {
			//search in this leaf node,
			// find the first subnode that bigger or equal to the key
			// if key < 
			int index;
			int rtcode = SearchKeyinLeafNode(key->v.intV, node, &index);
			if (rtcode == 1)
			{//found


				RemoveKeyinLeafNode(tree, node, 1, index, key);
				DecriOneEntriesinTree(tree);
				ret = RC_OK;// set return value
				
			}
			else {
				ret = RC_IM_KEY_NOT_FOUND;
                DeConstructor_TreeNode(node);
			}
			
			break;
		}
	}
	return ret;

}


typedef struct BT_ScanHandle_extra_struct
{
	int numScanned;
	int currleafnodeBlock;
	int currKeyIndexinBlock;
	//bool endofLeafnode;
}BTreeScan;


RC openTreeScan(BTreeHandle* tree, BT_ScanHandle** handle)
{
	*handle = malloc(sizeof(BT_ScanHandle));
	BT_ScanHandle* scanhandle = *handle;

	BTreeScan* btreescan = malloc(sizeof(BTreeScan));
	btreescan->numScanned = 0;
	btreescan->currleafnodeBlock = -1;
	btreescan->currKeyIndexinBlock = -1;

	scanhandle->tree = tree;
	scanhandle->mgmtData = btreescan;

	return RC_OK;
}


//assume result is allocated
// leaf nodes are connected one by one, all we need to do is find the first leaf node
// Then scan every leaf node
RC nextEntry(BT_ScanHandle* handle, RID* result)
{
	BTreeScan* btreescan = handle->mgmtData;
	BTreeHandle* tree = handle->tree;
	BplusTree* bptree = tree->mgmtData;

	if (bptree->numEntries == 0)// empty tree
	{
		printf(" nextEntry() :empty tree\n");
		return RC_IM_NO_MORE_ENTRIES;

	}
	else if (btreescan->numScanned >= bptree->numEntries) {
		// all scanned
		return RC_IM_NO_MORE_ENTRIES;
	}
	// test if this is the first calling
	if (btreescan->numScanned == 0)
	{
		TreeNode* node = GetNodeFromDisk(tree, bptree->rootnode);
		while (node != NULL)
		{
			if (node->nodetype == NONLEAF)
			{
				TreeNode* newnode = GetNodeFromDisk(tree, node->nodevalues[0].child_index);
				DeConstructor_TreeNode(node);//release the old one
				node = newnode;//set the new one and keep searching
			}
			else {
				// now we are in first leafnode
				result->page = node->nodevalues[0].value.page;
				result->slot = node->nodevalues[0].value.slot;

				btreescan->currKeyIndexinBlock = 0;//first record
				btreescan->numScanned++;
				btreescan->currleafnodeBlock = node->self;

				DeConstructor_TreeNode(node);
				return RC_OK;
			}
		}

	}
	else {
		// not first calling, just go last leaf node
		TreeNode* node = GetNodeFromDisk(tree, btreescan->currleafnodeBlock);
		// go find next value in this block, if this leaf node still have next 
		btreescan->currKeyIndexinBlock++;
		if (btreescan->currKeyIndexinBlock != node->numKeys)
		{//still have key in this block
			result->page = node->nodevalues[btreescan->currKeyIndexinBlock].value.page;
			result->slot = node->nodevalues[btreescan->currKeyIndexinBlock].value.slot;
			DeConstructor_TreeNode(node);
			return RC_OK;
		}
		else {// we need to go next leaf node

			btreescan->currleafnodeBlock = node->right;
			if (node->right == -1)
			{
				//no more records
				DeConstructor_TreeNode(node);
				return RC_IM_NO_MORE_ENTRIES;
			}
			DeConstructor_TreeNode(node);
			// enter new block
			node = GetNodeFromDisk(tree, btreescan->currleafnodeBlock);
			result->page = node->nodevalues[0].value.page;
			result->slot = node->nodevalues[0].value.slot;

			btreescan->currKeyIndexinBlock = 0;//first record
			btreescan->numScanned++;
			DeConstructor_TreeNode(node);
			return RC_OK;
		}
	}
	return RC_OK;
}



RC closeTreeScan(BT_ScanHandle* handle)
{
	BTreeScan* btreescan = handle->mgmtData;
	free(btreescan);
	free(handle);
	handle = NULL;
	return RC_OK;
}



char* printTree(BTreeHandle* tree)
{
	if (tree == NULL)return NULL;
	BplusTree* bptree = tree->mgmtData;


	BplusTree* bplustree = tree->mgmtData;
	if (bplustree->numEntries == 0) {
		//empty tree
		return RC_IM_KEY_NOT_FOUND;
	}

	// get back root node from disk
	TreeNode* root = GetNodeFromDisk(tree, bplustree->rootnode);

	TreeNode* node = root;// start from node
	RC ret = RC_OK;
	while (node != NULL)
	{
		if (node->nodetype == NONLEAF)
		{
			TreeNode* nextlevelfirstnode = GetNodeFromDisk(tree, node->nodevalues[0].child_index);
			
			int endoflevel = false;
			do {
				for (size_t i = 0; i < node->numKeys; i++)
				{
					printf("[%d L%d,R%d]",node->keys[i], node->nodevalues[i].child_index, node->nodevalues[i + 1].child_index);
				}
				if (node->right == -1)
				{
					endoflevel = true;
				}
				else {
					int nextright = node->right;
					DeConstructor_TreeNode(node);//release the old one
					node= GetNodeFromDisk(tree, nextright);
				}
			} while (endoflevel);
			
			DeConstructor_TreeNode(node);//release the old one
			node = nextlevelfirstnode;//set the new one and keep searching
		}
		else {
			//search in this leaf node,
			// find the first subnode that bigger or equal to the key
			// if key < 
			int endoflevel = false;
			do {
				for (size_t i = 0; i < node->numKeys; i++)
				{
					printf("[%d L%d,R%d]", node->keys[i], node->nodevalues[i].child_index, node->nodevalues[i + 1].child_index);
				}
				if (node->right == -1)
				{
					endoflevel = true;
				}
				else {
					int nextright = node->right;
					DeConstructor_TreeNode(node);//release the old one
					node = GetNodeFromDisk(tree, nextright);
				}
			} while (endoflevel);
			DeConstructor_TreeNode(node);
			break;
		}
	}

	return NULL;
}
