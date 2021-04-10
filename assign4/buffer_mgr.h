﻿#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"
#include "storage_mgr.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
typedef int FrameNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile; // the name of the page file associated with the buffer pool ( pageFile)
	int numPages;//the size of the buffer pool, i.e., the number of page frames ( numPages),
	ReplacementStrategy strategy;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
} BM_BufferPool;
typedef struct pageFrame {
    PageNumber pageNum; ///the position of the page map with this frame
    SM_PageHandle memPage; //A page handle is an pointer to an area in memory storing the data of a page.
    bool dirty; //indicate whether the content in this frame has been modified
    int fixCount; // the number of client pin this page
    int virtual_t_now; //for LRU
}frame;

typedef struct ExtraInfo_BufferPools {
	SM_FileHandle* fh; //one bufferpool match one fileHandle
	frame* frame; //for frame info
	int firstIndex; //for FIFO
	bool* dirtyflags;// for debug function getDirtyFlags only, flag only update in get DirtyFlags
	int* fixCounts;// for debug function getFixCounts only, flag only update in getFixCounts
	PageNumber * pagenumbers;// for debug function getFixCounts only, flag only update in getFixCounts
	int totalRead  ;
	int totalWrite ;
	int queue_head;// queue head for FIFO
}EI_BP;



//typedef struct LRU_time {
//	time_t lastUse_time; //the last time when this frame has been used
//	FrameNumber frameindex; //this frame index
//}LRU_t;

//The page number (position of the page in the page file) is stored in pageNum.
//The data field points to the area in memory storing the content of the page.
typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
