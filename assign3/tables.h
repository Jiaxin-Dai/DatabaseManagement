#ifndef TABLES_H
#define TABLES_H

#include "dt.h"

// Data Types, Records, and Schemas
typedef enum DataType {
	DT_INT = 0,
	DT_STRING = 1,
	DT_FLOAT = 2,
	DT_BOOL = 3
} DataType;

//The value struct represents the values of a data type using standard C data types. 
//For example, a string is a char * and an integer using a C int. 
//Note that values are only used for expressions 
//and for returning data to the client of the record manager.
typedef struct Value {
	DataType dt;
	union v {
		int intV;
		char *stringV;
		float floatV;
		bool boolV;
	} v;
} Value;

typedef struct RID {
	//tuple identifier -- tid
	//page identifier
	int page;
	//slot number
	int slot;
} RID;

typedef struct Record
{
	RID id;
	char *data;
} Record;

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

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
	char *name;
	Schema *schema;
	void *mgmtData;
} RM_TableData;

#define MAKE_STRING_VALUE(result, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = DT_STRING;						\
			(result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
			strcpy((result)->v.stringV, value);					\
		} while(0)


#define MAKE_VALUE(result, datatype, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = datatype;						\
			switch(datatype)							\
			{									\
			case DT_INT:							\
			(result)->v.intV = value;					\
			break;								\
			case DT_FLOAT:							\
			(result)->v.floatV = value;					\
			break;								\
			case DT_BOOL:							\
			(result)->v.boolV = value;					\
			break;								\
			}									\
		} while(0)


// debug and read methods
extern Value *stringToValue (char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

#endif
