#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

//Bit map in the beginning of a page block, if the slot is free the bit mapped to it is 0, else is 1
typedef struct bitvector_s {
    uint32_t numEntries; // length of how many record we have, each record is one bit in char.one char has 8 bits
	uint32_t lenBytes;  // length of databits
	uint8_t* databytes; // a char array where we put bits in here, something like 010000110
}BitVector;


//creat a bitvector which can contain number of numEntries in total
BitVector*  ConstructorBitVector(uint32_t numEntries)
{

	BitVector* this = malloc(sizeof(BitVector));
	this->numEntries = numEntries;
	this->lenBytes = numEntries / 8;
	if (numEntries % 8 > 0) numEntries += 1;// need one more byte 
	this->databytes = malloc(this->lenBytes * sizeof(uint8_t));
	memset(this->databytes, 0, this->lenBytes);// set defalut value 0
	return this;
}
// delete and free memory
void DeletorBitVector(BitVector* this)
{
	if (this->lenBytes > 0)
	{
		free(this->databytes);
	}
	free(this);
	this = NULL;
}
//// get new SerializedBV from BitVector, BitVector this, won't be deleted,
// return the length of SerializedBV
uint32_t SerializeBitVector(BitVector* this, uint8_t** serializedBV)
{
	if (!this||!serializedBV)
	{
		printf("Null pointer in SerializeBitVector\n");
		return -1;
    }

	*serializedBV = (uint8_t*)malloc(8 +  (this->lenBytes));
	//memcpy(*serializedBV, this->numEntries, sizeof(uint32_t));
	//memcpy(*serializedBV+4, this->lenBytes, sizeof(uint32_t));
	*((uint32_t*)*serializedBV) = this->numEntries;
	*((uint32_t*)(*serializedBV + 4)) = this->lenBytes;
	memcpy(*serializedBV + 8, this->databytes, this->lenBytes);
	return 4 + 4 + (this->lenBytes);
}

// get new BitVector from SerializedBV, SerializedBV won't be deleted,
BitVector* DeSerializeBitVector(uint8_t* SerializedBV) {
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

}
/// ///////////////////////
//get a bit value, 0 for false, 1 for true. other value for error
int GetBit(BitVector* this, uint32_t index)
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
	databyte = this->databytes[index/8];
	// find the bit then
	uint32_t indexInByte = index % 8;
	// a mask code to get the bit value;
	// say if we want the 4th(index is 3) of 0010 1010
	// put mask code as 0000 0001, left shift 4 , become 0000 1000
	//0000 1000 & 0010 1010 , And opration to get th  4 th byte, then right shift it to
	//first bit. Coincert is back to int value, if it is 1, the it is true, 0 is false
	uint8_t mask = 1 << (7 - indexInByte);

	uint8_t value = (databyte & mask);
	value=value >> (7 - indexInByte);
	return value;
}

// set the bit, 0 for success, other for error 
int SetBit(BitVector* this, uint32_t index, bool value)
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
}

//find the first false bit in bit vector
//we use this fucntion to find a free block and free slot
int FindFirst0BitVector(BitVector* this)
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
			freeindes=i;
				break;
		}
	}
	return freeindes;
}
////////////////////end of class BitVector


Block_header* block_header;
// table and manager
/*
typedef struct RM_TableData
{
	char* name;
	Schema* schema;
	void* mgmtData;
} RM_TableData;*/
//
typedef struct TableExtraContent_s
{
	size_t lenRecord;// length of bytes of one record
	size_t maxRecords1Block;// max records number in one block
	unsigned int numRecordsinTotal;// total number of records
	unsigned int* numRecordinBlock;// num of records in each bloack
	BitVector* pagefullflag; //Bit vector indicate if the page block is full
	/// ////two struct help us do fetch and store data to disk/////////////////////
	BM_BufferPool* bufferPool;
	BM_PageHandle* pageHandle;
}TableExtraInfo;

// this struct will be written to disk, it has all the info that we need to get the table back
// read this struct from disk, and construct RM_TableData in memory
typedef struct RM_TableDataIndisk_s
{
	char* name;
	Schema* schema;
	size_t lenRecord;// length of bytes of one record
	size_t maxRecords1Block;// max records number in one block
	unsigned int numRecordsinTotal;// total number of records
	unsigned int* numRecordinBlock;// num of records in each bloack
	BitVector* pagefullflag; //Bit vector indicate if the page block is full
}RM_TableDataIndisk;


//covert  RM_TableData 2 RM_TableDataIndisk, pre allocate memory for tableindisk
void Serialize_TableData(RM_TableData* rel,RM_TableDataIndisk* tableindisk) {

	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;

	tableindisk->name = rel->name;//no need to write this to disk
	tableindisk->schema = rel->schema;
	tableindisk->lenRecord = tableExtraInfo->lenRecord;
	tableindisk->maxRecords1Block = tableExtraInfo->maxRecords1Block;
	tableindisk->numRecordsinTotal = tableExtraInfo->numRecordsinTotal;
	tableindisk->numRecordinBlock = tableExtraInfo->numRecordinBlock;
	tableindisk->pagefullflag = tableExtraInfo->pagefullflag;

}
// remember to preallocate and free binary_data
void ConvertTableinDisk2binary(RM_TableDataIndisk* tableindisk,char* binary_data)
{
	//prepare data for page data
	size_t offset = 0;
	//1. write schema
	Schema* schema = tableindisk->schema;
	// 1.1 schema->numAttr
	memcpy(binary_data + offset, &(schema->numAttr), sizeof(int));
	offset += sizeof(int);
	// 1.2 Typelength
	memcpy(binary_data + offset, schema->typeLength, sizeof(int) * schema->numAttr);
	offset += sizeof(int) * schema->numAttr;
	//1.3 write DataTypes then
	memcpy(binary_data + offset, schema->dataTypes, sizeof(DataType) * schema->numAttr);
	offset += sizeof(DataType) * schema->numAttr;
	//1.4 now write attrNames;
	for (int i = 0; i < schema->numAttr; ++i) {
		switch (schema->dataTypes[i])
		{
		case DT_STRING:
			memcpy(binary_data + offset, schema->attrNames[i], schema->typeLength[i] * sizeof(char));
			offset += schema->typeLength[i] * sizeof(char);// string attr length is the number of chars
			break;
		case DT_INT:
			memcpy(binary_data + offset, schema->attrNames[i], sizeof(int));
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			memcpy(binary_data + offset, schema->attrNames[i], sizeof(float));
			offset += sizeof(float);
			break;
		case DT_BOOL:
			memcpy(binary_data + offset, schema->attrNames[i], sizeof(bool));
			offset += sizeof(bool);
			break;
		default:
			printf("Undefined dataType in Serialize_TableData\n");
			return ;
		}

	}
	//1.5 Write keySize of schema
	memcpy(binary_data + offset, &(schema->keySize), sizeof(int));
	offset += sizeof(int);
	//1.6 write keyAttrs of schema
	memcpy(binary_data + offset, schema->keyAttrs, sizeof(int) * schema->keySize);
	offset += sizeof(int) * schema->keySize;

	// 2. tableindisk->lenRecord
	memcpy(binary_data + offset, &(tableindisk->lenRecord), sizeof(size_t));
	offset += sizeof(size_t);

	// 3.tableindisk->maxRecords1Block
	memcpy(binary_data + offset, &(tableindisk->maxRecords1Block), sizeof(size_t));
	offset += sizeof(size_t);

	//4. tableindisk->numRecordsinTotal
	memcpy(binary_data + offset, &(tableindisk->numRecordsinTotal), sizeof(unsigned int));
	offset += sizeof(unsigned int);

	//5. tableindisk->numRecordinBlock
	size_t blockNum = tableindisk->pagefullflag->numEntries;
	memcpy(binary_data + offset, tableindisk->numRecordinBlock, sizeof(unsigned int) * blockNum);
	offset += sizeof(unsigned int) * blockNum;

	//6. tableindisk->pagefullflag
	uint8_t* serializedBV;
	size_t len = SerializeBitVector(tableindisk->pagefullflag, &serializedBV);
	memcpy(binary_data + offset, serializedBV, sizeof(uint8_t) * len);

	offset += sizeof(uint8_t) * len;
	free(serializedBV);
	////////////////////////////////////////////////////////
}

//Convert TableDataIndisk 2 RM_TableData, need to pre acclocate memory for RM_TableData
// won't release tableindisk
void deSerialize_TableData(RM_TableDataIndisk* tableindisk, RM_TableData* rel) {
	/*
	typedef struct TableExtraContent_s
	{
		size_t lenRecord;// length of bytes of one record
		size_t maxRecords1Block;// max records number in one block
		unsigned int numRecordsinTotal;// total number of records
		unsigned int* numRecordinBlock;// num of records in each bloack
		BitVector* pagefullflag; //Bit vector indicate if the page block is full
		/// ////two struct help us do fetch and store data to disk/////////////////////
		BM_BufferPool* bufferPool;
		BM_PageHandle* pageHandle;
	}TableExtraInfo;*/
	TableExtraInfo* tableExtraInfo = malloc(sizeof(TableExtraInfo));
	

	rel->name = tableindisk->name;//no need to write this to disk
	rel->schema=tableindisk->schema ;

	tableExtraInfo->lenRecord=tableindisk->lenRecord;
	 tableExtraInfo->maxRecords1Block=tableindisk->maxRecords1Block;
	tableExtraInfo->numRecordsinTotal=tableindisk->numRecordsinTotal;
	tableExtraInfo->numRecordinBlock=tableindisk->numRecordinBlock;
	tableExtraInfo->pagefullflag=tableindisk->pagefullflag;
	tableExtraInfo->bufferPool = NULL;
	tableExtraInfo->pageHandle = NULL;
	rel->mgmtData = tableExtraInfo;

}

// remember to preallocate and free tableindisk
void Convertbinary2TableinDisk( char* binary_data,RM_TableDataIndisk* tableindisk)
{
	/*
	* typedef struct RM_TableDataIndisk_s
{
	char* name;
	Schema* schema;
	size_t lenRecord;// length of bytes of one record
	size_t maxRecords1Block;// max records number in one block
	unsigned int numRecordsinTotal;// total number of records
	unsigned int* numRecordinBlock;// num of records in each bloack
	BitVector* pagefullflag; //Bit vector indicate if the page block is full
}RM_TableDataIndisk;
	*/
	size_t offset = 0;
	//name is the filename, must be assigned out side
	//1. write schema
	tableindisk->schema = malloc(sizeof(Schema));
	Schema* schema = tableindisk->schema;
	// 1.1 schema->numAttr
	memcpy(  &(schema->numAttr),binary_data + offset,sizeof(int));
	offset += sizeof(int);
	// 1.2 Typelength
	schema->typeLength = malloc(sizeof(int) * schema->numAttr);//allocate memory
	memcpy( schema->typeLength, binary_data + offset,sizeof(int) * schema->numAttr);
	offset += sizeof(int) * schema->numAttr;
	//1.3 write DataTypes then
	schema->dataTypes = malloc(sizeof(DataType) * schema->numAttr);
	memcpy( schema->dataTypes,binary_data + offset, sizeof(DataType) * schema->numAttr);
	offset += sizeof(DataType) * schema->numAttr;
	//1.4 now write attrNames;
	//fix: allocate memory for schema->attrNames;
	schema->attrNames = malloc(sizeof(char*) * schema->numAttr);//fix+++++++++
	for (int i = 0; i < schema->numAttr; ++i) {
		switch (schema->dataTypes[i])
		{
		case DT_STRING:
			schema->attrNames[i] = malloc(schema->typeLength[i] * sizeof(char));
			memcpy( schema->attrNames[i],binary_data + offset, schema->typeLength[i] * sizeof(char));
			offset += schema->typeLength[i] * sizeof(char);// string attr length is the number of chars
			break;
		case DT_INT:
			schema->attrNames[i] = malloc(sizeof(int));
			memcpy( schema->attrNames[i],binary_data + offset, sizeof(int));
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			schema->attrNames[i] = malloc(sizeof(float));
			memcpy( schema->attrNames[i],binary_data + offset, sizeof(float));
			offset += sizeof(float);
			break;
		case DT_BOOL:
			schema->attrNames[i] = malloc(sizeof(bool));
			memcpy( schema->attrNames[i],binary_data + offset, sizeof(bool));
			offset += sizeof(bool);
			break;
		default:
			printf("Undefined dataType in Serialize_TableData\n");
			return ;
		}

	}
	//1.5 Write keySize of schema
	memcpy( &(schema->keySize),binary_data + offset, sizeof(int));
	offset += sizeof(int);
	//1.6 write keyAttrs of schema
	schema->keyAttrs = malloc(sizeof(int) * schema->keySize);//fix  allocate memory
	memcpy( schema->keyAttrs, binary_data + offset,sizeof(int) * schema->keySize);
	offset += sizeof(int) * schema->keySize;

	// 2. tableindisk->lenRecord
	memcpy( &(tableindisk->lenRecord),binary_data + offset, sizeof(size_t));
	offset += sizeof(size_t);

	// 3.tableindisk->maxRecords1Block
	memcpy( &(tableindisk->maxRecords1Block),binary_data + offset, sizeof(size_t));
	offset += sizeof(size_t);

	//4. tableindisk->numRecordsinTotal
	memcpy( &(tableindisk->numRecordsinTotal),binary_data + offset, sizeof(unsigned int));
	offset += sizeof(unsigned int);

	//5. tableindisk->numRecordinBlock
	size_t blockNum = tableindisk->pagefullflag->numEntries;
	memcpy(tableindisk->numRecordinBlock,binary_data + offset,  sizeof(unsigned int) * blockNum);
	offset += sizeof(unsigned int) * blockNum;

	//6. tableindisk->pagefullflag
	tableindisk->pagefullflag= DeSerializeBitVector(binary_data + offset);

	////////////////////////////////////////////////////////
}

typedef BitVector BlockInfo;
RC initRecordManager(void* mgmtData)
{
	return RC_OK;
}

RC shutdownRecordManager()
{
	free(block_header);
	return RC_OK;
}

//Creating a table should create the underlying page file 
//and store information about the schema, free-space, … 
//and so on in the Table Information pages. 

//Each table should be stored in a separate page file
RC createTable(char* name, Schema* schema)
{
	//create a page by storage_mgc
	initStorageManager();
	if (createPageFile(name) != RC_OK) {
		printError(RC_WRITE_FAILED);
		return RC_WRITE_FAILED;
	}
	BM_BufferPool* bufferpool = MAKE_POOL();
	RC rc = initBufferPool(bufferpool, name, 3, RS_FIFO, NULL);// 3 page slot
	if (rc != RC_OK) {
		printf("initBufferPool Failed in creatTable\n");
		return RC_OK;
	}
	BM_PageHandle* pagehandle = MAKE_PAGE_HANDLE();
	RM_TableDataIndisk* tableindisk = malloc(sizeof(RM_TableDataIndisk));
	tableindisk->name = name;
	
	/*
	in the first 4+4+(x/8)+1 bytes is the block header
	then is the records of number x
	*/
	int recordsize = getRecordSize(schema);
	tableindisk->lenRecord = recordsize;
	tableindisk->maxRecords1Block = (8 * PAGE_SIZE - 72) / (8 * getRecordSize(schema) + 1);// max number of records
	int numBlocks = 256;//i.e. 2^8 and 2^5=32 bytes in bitvector, could be more, just don't use up all page block 0
	tableindisk->numRecordinBlock = malloc(256*sizeof(unsigned int));
	memset(tableindisk->numRecordinBlock, 0, 256 * sizeof(unsigned int));
	tableindisk->numRecordsinTotal = 0;
	tableindisk->schema = schema;
	tableindisk->pagefullflag = ConstructorBitVector(256);
	SetBit(tableindisk->pagefullflag, 0, true);//page block 1 is reserved for meta info
	pinPage(bufferpool, pagehandle,0);
	ConvertTableinDisk2binary(tableindisk, pagehandle->data);
	//delete tmp varable
	DeletorBitVector(tableindisk->pagefullflag);
	free(tableindisk->numRecordinBlock);
	free(tableindisk);

	//delete buffer pool
	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);
	forceFlushPool(bufferpool);
	shutdownBufferPool(bufferpool);

	free(bufferpool);
	free(pagehandle);

	return RC_OK;
}

RC openTable(RM_TableData* rel, char* name)
{
	//create a page by storage_mgc
	initStorageManager();
	
	BM_BufferPool* bufferpool = MAKE_POOL();
	RC rc = initBufferPool(bufferpool, name, 10, RS_FIFO, NULL);// 10 page slot
	if (rc != RC_OK) {
		printf("initBufferPool Failed in creatTable\n");
		return RC_OK;
	}
	BM_PageHandle* pagehandle = MAKE_PAGE_HANDLE();

	TableExtraInfo* tableExtraInfo = malloc(sizeof(TableExtraInfo));
	/*typedef struct TableExtraContent_s
{
	size_t lenRecord;// length of bytes of one record
	size_t maxRecords1Block;// max records number in one block
	unsigned int numRecordsinTotal;// total number of records
	unsigned int* numRecordinBlock;// num of records in each bloack
	BitVector* pagefullflag; //Bit vector indicate if the page block is full
	/// ////two struct help us do fetch and store data to disk/////////////////////
	BM_BufferPool* bufferPool;
	BM_PageHandle* pageHandle;
}TableExtraInfo;*/
	
	// get table data from disk as a RM_TableDataIndisk;
	pinPage(bufferpool, pagehandle, 0);
    RM_TableDataIndisk* tableindisk = malloc(sizeof(RM_TableDataIndisk));
	tableindisk->numRecordinBlock= malloc(256 * sizeof(unsigned int));// same like creat table
	tableindisk->schema = malloc(sizeof(Schema));
	tableindisk->pagefullflag = ConstructorBitVector(256);
	Convertbinary2TableinDisk(pagehandle->data, tableindisk);
	unpinPage(bufferpool, pagehandle);

	tableExtraInfo->lenRecord = tableindisk->lenRecord;
	tableExtraInfo->maxRecords1Block = tableindisk->maxRecords1Block;
	tableExtraInfo->numRecordsinTotal = tableindisk->numRecordsinTotal;
	tableExtraInfo->numRecordinBlock = tableindisk->numRecordinBlock;
	tableExtraInfo->pagefullflag = tableindisk->pagefullflag;
	tableExtraInfo->bufferPool = bufferpool;
	tableExtraInfo->pageHandle = pagehandle;
	rel->name = name;
	rel->schema = tableindisk->schema;
	rel->mgmtData = tableExtraInfo;
	free(tableindisk);
	
	return RC_OK;
}

//Closing a table should cause all outstanding changes to the table to be written to the page file.
//-------------------??????????????????---------------------
RC closeTable(RM_TableData* rel)
{
	
	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;
	//1. write data to disk
    RM_TableDataIndisk* tableindisk = malloc(sizeof(RM_TableDataIndisk));
	Serialize_TableData(rel, tableindisk);
	pinPage(bufferpool, pagehandle, 0);
	ConvertTableinDisk2binary(tableindisk, pagehandle->data);
	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);
	free(tableindisk);
	//2. free rel
	forceFlushPool(bufferpool);
	shutdownBufferPool(bufferpool);
	/*
	* typedef struct TableExtraContent_s
{
	size_t lenRecord;// length of bytes of one record
	size_t maxRecords1Block;// max records number in one block
	unsigned int numRecordsinTotal;// total number of records
	unsigned int* numRecordinBlock;// num of records in each bloack
	BitVector* pagefullflag; //Bit vector indicate if the page block is full
	/// ////two struct help us do fetch and store data to disk/////////////////////
	BM_BufferPool* bufferPool;
	BM_PageHandle* pageHandle;
}TableExtraInfo;
	*/
	free(tableExtraInfo->numRecordinBlock);
	DeletorBitVector(tableExtraInfo->pagefullflag);
	free(tableExtraInfo->bufferPool);
	free(tableExtraInfo->pageHandle);
	free(tableExtraInfo);
	rel->mgmtData = NULL;
	return RC_OK;
}

RC deleteTable(char* name)
{
	destroyPageFile(name);
	return RC_OK;
}

//The getNumTuples function returns the number of tuples in the table.
int getNumTuples(RM_TableData* rel)
{
	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	
	return tableExtraInfo->numRecordsinTotal;
}

//---------------------------------------------------------------------
// handling records in a table
RC insertRecord(RM_TableData* rel, Record* record)
{
	if (!rel || !record) {
		printf(" NULL pointer in insertRecord\n ");
		return RC_READ_NON_EXISTING_PAGE;

	}
	TableExtraInfo* tableExtraInfo = (TableExtraInfo*) rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;
	
	//Step 1: FInd a free page
	unsigned int freepageIndex = FindFirst0BitVector(tableExtraInfo->pagefullflag);
	if (freepageIndex <0) {
		
		printf(" all pages are full\n");
		return RC_IM_KEY_NOT_FOUND;
	}
	//Step2: go to free page and find a free slot there
	// 2.1 get block header from page file
	pinPage(bufferpool, pagehandle, freepageIndex);// +1 because we store records in 1st page and below, the 0 page is reserved for table info
	char* outputdataptr = pagehandle->data;
	//BlockInfo* blockinfo = (BlockInfo*)outputdataptr;// need a fix

	BlockInfo* blockinfo = NULL;
	// if the free page has never be inislized, we need to creat a new bitvector on the start of that page
	if (tableExtraInfo->numRecordinBlock[freepageIndex] == 0) {

		 blockinfo = ConstructorBitVector(tableExtraInfo->maxRecords1Block);
	}else{
	      blockinfo = DeSerializeBitVector(outputdataptr);// remeber to free
	}
	 // 2.2 find a free slot in this block
	unsigned int freeslotIndex = FindFirst0BitVector(blockinfo);
	record->id.page = freepageIndex;
	record->id.slot = freeslotIndex;
	// 3. find the data start position, we store the first record at the last of the page block
	size_t start_pos = PAGE_SIZE - (freeslotIndex+1) * tableExtraInfo->lenRecord;
	//4. put the record data to buffer
	memcpy(outputdataptr+start_pos, record->data, tableExtraInfo->lenRecord);
	// 5. update index flag, block header and table header
	SetBit(blockinfo, freeslotIndex, true);// updaate block
	tableExtraInfo->numRecordinBlock[freepageIndex]++;
	tableExtraInfo->numRecordsinTotal++;
	// if the page block is full now, mark the full flag in table
	if (tableExtraInfo->numRecordinBlock[freepageIndex] == tableExtraInfo->maxRecords1Block-1)
	{
		SetBit(tableExtraInfo->pagefullflag, freepageIndex, true);// page block is marked as full now
	}
	uint8_t* binayBlockinfo;
	size_t len=SerializeBitVector(blockinfo, &binayBlockinfo);
	DeletorBitVector(blockinfo);// delete block header in memory
	memcpy(outputdataptr, binayBlockinfo, len);
	free(binayBlockinfo);
	/********Write page back to disk*/
	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);
	return RC_OK;
}

RC deleteRecord(RM_TableData* rel, RID id)
{
	if (!rel ) {
		printf(" NULL pointer in insertRecord\n ");
		return RC_READ_NON_EXISTING_PAGE;

	}
	
	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;
	if (id.page > tableExtraInfo->pagefullflag->numEntries)
	{
		printf("RID invalid in deleteRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	//get block header from page file
	pinPage(bufferpool, pagehandle, id.page);
	char* outputdataptr = pagehandle->data;
	BlockInfo* blockinfo = DeSerializeBitVector(outputdataptr);// remeber to free
	if (id.slot > blockinfo->numEntries)
	{
		printf("RID invalid in deleteRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	// no need to delete data, just set flags
	// 1.set block header
	SetBit(blockinfo, id.slot, false);
	//2. update table header
	tableExtraInfo->numRecordinBlock[id.page]--;
	tableExtraInfo->numRecordsinTotal--;
	// if the page block is full now, mark the full flag in table
	if (tableExtraInfo->numRecordinBlock[id.page]+1 == tableExtraInfo->maxRecords1Block)
	{
		SetBit(tableExtraInfo->pagefullflag, id.page, false);// page block is marked as full now
	}
	uint8_t* binayBlockinfo;
	size_t len = SerializeBitVector(blockinfo, &binayBlockinfo);
	DeletorBitVector(blockinfo);// delete block header in memory
	memcpy(outputdataptr, binayBlockinfo, len);
	free(binayBlockinfo);
	/********Write page back to disk*/
	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);
	return RC_OK;
}

RC updateRecord(RM_TableData* rel, Record* record)
{
	if (!rel||!record) {
		printf(" NULL pointer in updateRecord\n ");
		return RC_READ_NON_EXISTING_PAGE;

	}

	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;
	if (record->id.page > tableExtraInfo->pagefullflag->numEntries)
	{
		printf("RID invalid in updateRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	//get block header from page file
	pinPage(bufferpool, pagehandle, record->id.page);
	char* outputdataptr = pagehandle->data;
	BlockInfo* blockinfo = DeSerializeBitVector(outputdataptr);// remeber to free
	if (record->id.slot > blockinfo->numEntries)
	{
		printf("RID invalid in updateRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	// 1.check block header

	int set=GetBit(blockinfo, record->id.slot);
	if (set!=1)// no such record in disk file
	{
		printf("record position is not allocated in updateRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	
	// 3. find the data start position, we store the first record at the last of the page block
	size_t start_pos = PAGE_SIZE - (record->id.slot+1)* tableExtraInfo->lenRecord;
	//4. put the record data to buffer
	memcpy(outputdataptr+start_pos,record->data,  tableExtraInfo->lenRecord);

	uint8_t* binayBlockinfo;
	size_t len = SerializeBitVector(blockinfo, &binayBlockinfo);
	DeletorBitVector(blockinfo);// delete block header in memory
	memcpy(outputdataptr, binayBlockinfo, len);
	free(binayBlockinfo);
	/********Write page back to disk*/
	markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);

	return RC_OK;
}

RC getRecord(RM_TableData* rel, RID id, Record* record)
{
	if (!rel || !record) {
		printf(" NULL pointer in updateRecord\n ");
		return RC_READ_NON_EXISTING_PAGE;

	}

	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;
	if (id.page > tableExtraInfo->pagefullflag->numEntries)
	{
		printf("RID invalid in getRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	record->id = id;
	//get block header from page file
	pinPage(bufferpool, pagehandle, record->id.page);
	char* outputdataptr = pagehandle->data;
	BlockInfo* blockinfo = DeSerializeBitVector(outputdataptr);// remeber to free
	if (id.slot > blockinfo->numEntries)
	{
		printf("RID invalid in getRecord \n ");
		return RC_FILE_NOT_FOUND;
	}
	// 1.check block header
	int set = GetBit(blockinfo, record->id.slot);
	if (set != 1)// no such record in disk file
	{
		printf("record position is not allocated in getRecord \n ");
		return RC_FILE_NOT_FOUND;
	}

	// 3. find the data start position, we store the first record at the last of the page block
	size_t start_pos = PAGE_SIZE - (record->id.slot+1) * tableExtraInfo->lenRecord;
	//4. put the record data to buffer
	memcpy( record->data,outputdataptr+ start_pos, tableExtraInfo->lenRecord);
	record->id = id;
	uint8_t* binayBlockinfo;
	size_t len = SerializeBitVector(blockinfo, &binayBlockinfo);
	DeletorBitVector(blockinfo);// delete block header in memory
	memcpy(outputdataptr, binayBlockinfo, len);
	free(binayBlockinfo);
	/********Write page back to disk*/
	//markDirty(bufferpool, pagehandle);
	unpinPage(bufferpool, pagehandle);

	return RC_OK;
}

typedef struct ScanHandle_extra_t
{
	size_t numScannedRecord;// how many record that we have scanned
	Expr* cond;// the Expr condition used to test a recxord
	RID lastHitPosition;// the last position where we find a hitted record, used in Next function
}ScanHandleExtraInfo;

//---------------------------------------------------------------------
// scans
RC startScan(RM_TableData* rel, RM_ScanHandle* scan, Expr* cond)
{
	if (!rel || !scan || !cond)
	{
		printf("null poniter in startScan Function \n");
		return RC_FILE_NOT_FOUND;
	}
	//extra info 
	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;

	// open table first
	openTable(rel, rel->name);

	// construct a RM_ScanHandle
	scan->rel = rel;//scan is allocated according to test code
	// put ScanHandleExtraInfo
	ScanHandleExtraInfo* scaninfo = (ScanHandleExtraInfo*)malloc(sizeof(ScanHandleExtraInfo));// remember to deallocate memory
	scaninfo->numScannedRecord = 0;
	scaninfo->cond = cond;
	scaninfo->lastHitPosition.page = 1;// first page block 0 is reserved to table meta info, put record started in page block 1
	scaninfo->lastHitPosition.slot = -1;// slot is started in 0th slot
	scan->mgmtData = scaninfo;// store extra info


	return RC_OK;
}
/*
* typedef struct RM_ScanHandle
{
	RM_TableData *rel;
	void *mgmtData;
} RM_ScanHandle;
*/

RC next(RM_ScanHandle* scan, Record* record)
{
	if ( !scan || !record)// record need to be allocated before enter into this function
	{
		printf("null poniter in startScan Function \n");
		return RC_FILE_NOT_FOUND;
	}

	// extract structs needed from input
	RM_TableData* rel = scan->rel;
	ScanHandleExtraInfo* scanhandleExtraInfo = scan->mgmtData;
	TableExtraInfo* tableExtraInfo = (TableExtraInfo*)rel->mgmtData;
	BM_BufferPool* bufferpool = tableExtraInfo->bufferPool;
	BM_PageHandle* pagehandle = tableExtraInfo->pageHandle;

	if (scanhandleExtraInfo->numScannedRecord==tableExtraInfo->numRecordsinTotal)
	{
		//no  tuples in table
		return RC_RM_NO_MORE_TUPLES;
	}
	
	RID currRid = scanhandleExtraInfo->lastHitPosition;
	BlockInfo* blockinfo;
	Expr* cond = scanhandleExtraInfo->cond;
	Value* evalExprResult;// store th etem value when we try to call evalExpr
	bool found = false;// a flag indicate that we have hit a record
	bool scan2End = false;// a flag indicate that we have reach the end of the table
	while (!scan2End)
	{


		//retrive last page block from disk
		pinPage(bufferpool, pagehandle, currRid.page);
		//get block info
		char* outputdataptr = pagehandle->data;
		blockinfo = DeSerializeBitVector(outputdataptr);// remeber to free

		for (currRid.slot++; currRid.slot < blockinfo->numEntries; currRid.slot++) { // for loop inside a page block
			 // scan until no more records in this page block, go check next

			 // check if next slot has a valid record
			int validFlag = GetBit(blockinfo, currRid.slot);
			if (validFlag != 1) {// this slot is not valid, the record here may be delted before
				continue;// checkt next slot
			}

			getRecord(rel, currRid, record);
			// note call evalExpr while allocate memory for evalExprResult
			// remember to free it 
			RC rc = evalExpr(record, rel->schema, cond, &evalExprResult);
			scanhandleExtraInfo->numScannedRecord++;

			// if the slot is not valid it is not a record, we wouldn't increase the numScannedRecord;
			// this is important we need to compare scanhandleExtraInfo->numScannedRecord == tableExtraInfo->numRecordsinTotal
			// to check if all records has been scanned
			if (rc == RC_OK && evalExprResult->v.boolV)
			{
				// found the value we need
				found = true;
				scanhandleExtraInfo->lastHitPosition = currRid;
				free(evalExprResult);
				break;
			}
			free(evalExprResult);
            if (scanhandleExtraInfo->numScannedRecord == tableExtraInfo->numRecordsinTotal)
            {
                scan2End = true;
                break;
            }
			// if not hit, the for loop go for next slot in THIS block, until it get the end of this block, 
			//then we use while loop go for next page block
		}
		//clean up for this page block
		unpinPage(bufferpool, pagehandle);
		DeletorBitVector(blockinfo);// delete block header in memory


		if (found == true)
		{// break while loop we have hit a record
			break;
		}
		if (scanhandleExtraInfo->numScannedRecord == tableExtraInfo->numRecordsinTotal)
		{
			scan2End = true;
		}
		currRid.page++;// go next page
	}// end while
	if (scan2End)
	{
		return RC_RM_NO_MORE_TUPLES;
	}

	return RC_OK;
}



RC closeScan(RM_ScanHandle* scan)
{// won't delete scan itself, according to test case, it is deleted outside this function
	if (!scan) {
		printf("Null pointer in closeScan\n");
		return RC_FILE_NOT_FOUND;
	}

	ScanHandleExtraInfo* scaninfo = scan->mgmtData;
	free(scaninfo);
	scan->mgmtData = NULL;
	scan->rel = NULL;
	return RC_OK;
}

int getRecordSize(Schema* schema)
{
	int offset = 0;
	for (int i = 0; i < schema->numAttr; ++i) {
		switch (schema->dataTypes[i])
		{
		case DT_STRING:
			//schema->attrNames[i] = malloc(schema->typeLength[i] * sizeof(char));
			offset += schema->typeLength[i] * sizeof(char);// string attr length is the number of chars
			break;
		case DT_INT:
			//schema->attrNames[i] = malloc(sizeof(int));
			//memcpy(schema->attrNames[i], binary_data + offset, sizeof(int));
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			//schema->attrNames[i] = malloc(sizeof(float));
			//memcpy(schema->attrNames[i], binary_data + offset, sizeof(float));
			offset += sizeof(float);
			break;
		case DT_BOOL:
			//schema->attrNames[i] = malloc(sizeof(bool));
			//memcpy(schema->attrNames[i], binary_data + offset, sizeof(bool));
			offset += sizeof(bool);
			break;
		default:
			printf("Undefined dataType in Serialize_TableData\n");
			return -1;
		}

	}
	return offset;
}

Schema* createSchema(int numAttr, char** attrNames, DataType* dataTypes, int* typeLength, int keySize, int* keys)
{
	if (numAttr < 0) {
		printError(RC_STORE_FAILED);
		return NULL;
	}
	if (attrNames == NULL) {
		printError(RC_STORE_FAILED);
		return NULL;
	}
	if (dataTypes == NULL) {
		printError(RC_STORE_FAILED);
		return NULL;
	}
	if (typeLength == NULL) {
		printError(RC_STORE_FAILED);
		return NULL;
	}
		/**
		
		// information of a table schema: its attributes, datatypes, 
typedef struct Schema
{
	//number of attributes
	int numAttr;
	// fields or attributes
	char **attrNames;
	// type of each fields/attributes
	DataType *dataTypes;
	//For attributes of type DT_STRING we record the size of the strings in typeLength
	int *typeLength;
	//The key is represented as an array of integers 
	//that are the positions of the attributes of the key ( keyAttrs). 
	int *keyAttrs;
	int keySize;
} Schema;
		
		
		*/

	Schema* schema = (Schema*)malloc(sizeof(Schema));
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;
	return schema;
}

RC freeSchema(Schema* schema)
{
	free(schema);
	return RC_OK;
}

//---------------------------------------------------------------------
// dealing with schemas
//Creating a new record should allocate enough memory to the data field 
//to hold the binary representations for all attributes of this record as determined by the schema.
RC createRecord(Record** record, Schema* schema)
{
	Record* empRecord = (Record*)malloc(sizeof(Record));
	int sizeRecord = getRecordSize(schema);
	empRecord->data = (char*)malloc(sizeof(sizeRecord));
	empRecord->id.page = -1;
	empRecord->id.slot = -1;
	*record = empRecord;
	return RC_OK;
}

RC freeRecord(Record* record)
{
	free(record->data);
	free(record);
	return RC_OK;
}

// aux function to return the begin postion of a attribute
size_t GetPosofAttr(Schema* schema, int attrNum) {
	size_t offset = 0;
	for (size_t i = 0; i < attrNum; i++)
	{
		switch (schema->dataTypes[i]) {
		case DT_STRING:
			offset += schema->typeLength[i] * sizeof(char);
			break;
		case DT_INT:
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			offset += sizeof(float);
			break;
		case DT_BOOL:
			offset += sizeof(bool);
			break;
		default:
			printf("Undefined dataType\n");
			return RC_RM_UNKOWN_DATATYPE;
		}
	}
	return offset;
}
//These functions are used to get or set the attribute values of a record 
//and create a new record for a given schema
RC getAttr(Record* record, Schema* schema, int attrNum, Value** value)
{
	//get the pos of the attr
	size_t offset = GetPosofAttr(schema,attrNum);
	
	
	char* posAttr = record->data + offset;

	//allocate the memory for value
	Value* attr = (Value*)malloc(sizeof(Value));
	attr->dt = schema->dataTypes[attrNum];

	switch (attr->dt)
	{
	case DT_STRING:
		//Recall that in C a string is an array of characters ended by a 0 byte. 
		//In a record, strings are stored without the additional 0 byte in the end
		attr->v.stringV = malloc(schema->typeLength[attrNum] + 1);
		memcpy(attr->v.stringV, posAttr, schema->typeLength[attrNum]);
		attr->v.stringV[schema->typeLength[attrNum]] = '\0';
		break;

	case DT_INT:
		memcpy(&(attr->v.intV), posAttr, sizeof(int));
		break;

	case DT_FLOAT:
		memcpy(&(attr->v.floatV), posAttr, sizeof(float));
		break;
	case DT_BOOL:
		memcpy(&(attr->v.boolV), posAttr, sizeof(bool));
		break;
	default:
		printf("Undefined dataType\n");
		return RC_RM_UNKOWN_DATATYPE;
	}
	*value = attr;

	return RC_OK;
}

RC setAttr(Record* record, Schema* schema, int attrNum, Value* value)
{
	//get the pos of the attr

	size_t offset = GetPosofAttr(schema, attrNum);
	//
	char* posAttr = record->data + offset;

	switch (schema->dataTypes[attrNum])
	{
	case DT_STRING:
		memcpy(posAttr, value->v.stringV, schema->typeLength[attrNum]);
		break;
	case DT_INT:
		memcpy(posAttr, &(value->v.intV), sizeof(int));
		break;
	case DT_FLOAT:
		memcpy(posAttr, &(value->v.floatV), sizeof(float));
		break;
	case DT_BOOL:
		memcpy(posAttr, &(value->v.boolV), sizeof(bool));
		break;
	default:
		printf("Undefined dataType\n");
		return RC_RM_UNKOWN_DATATYPE;
	}
	return RC_OK;
}
