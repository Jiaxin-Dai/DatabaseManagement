#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "stdlib.h"


//creates a new buffer pool with numPages page frames using the page replacement strategy strategy.
//The pool is used to cache pages from the page file with name pageFileName.
//Initially, all page frames should be empty.The page file should already exist, 
//i.e., this method should not generate a new page file.
//stratData can be used to pass parameters for the page replacement strategy.
//For example, for LRU - k this could be the parameter k.
RC initBufferPool(BM_BufferPool* const bm, const char* const pageFileName, const int numPages, ReplacementStrategy strategy, void* stratData)
{
	//open file
	SM_FileHandle* filehandle = malloc(sizeof(SM_FileHandle));
	RC open = openPageFile(pageFileName, filehandle);
	if (open != RC_OK)
	{   //open failed
		free(filehandle);
		return open;
	}
	else { //open file
		//initBufferManage
		bm->pageFile = pageFileName; //the file -- match the bufferPool
		bm->numPages = numPages; //the number of frame
		bm->strategy = strategy;

		//init the space of the extra info for bufferpool
		EI_BP* bufferPool = malloc(sizeof(EI_BP));
		bufferPool->fh = filehandle;

		bufferPool->firstIndex = 0;
		//init the space of the frames
		bufferPool->frame = malloc(sizeof(frame) * bm->numPages);
		bufferPool->dirtyflags = malloc(sizeof(bool) * bm->numPages);//debug info only!!!!!do not use
		bufferPool->fixCounts= malloc(sizeof(int) * bm->numPages);//debug info only!!!!!do not use, use this in getFIxcounts function, to prevent memory leak
		bufferPool->pagenumbers= malloc(sizeof(PageNumber) * bm->numPages);//debug info only!!!!!
		bufferPool->totalRead = 0;
		bufferPool->totalWrite = 0;
		bufferPool->queue_head = 0;
		//time_t now = time(NULL);
		for (int i=0; i < numPages; i++){
			bufferPool->frame[i].memPage = malloc(sizeof(char)*PAGE_SIZE);
			bufferPool->frame[i].pageNum = -1;
			bufferPool->frame[i].dirty = false;
			bufferPool->frame[i].virtual_t_now = -1;
			//bufferPool->frame[i].firstIndex = 0;
			bufferPool->frame[i].fixCount = 0;
		}

		bm->mgmtData = bufferPool;

		return RC_OK;
	}
	
}

typedef struct TimeSlot {// this struct map page pageframe number to time slot,used for sort
	int time;// the time of this pagenumber
	int pageframeNumber;// the page number
}TimeSlot;

int FramesCompare(const void* fa, const void* fb) {
	if (((TimeSlot*)fa)->time == ((TimeSlot*)fb)->time)
		return ((TimeSlot*)fa)->pageframeNumber - ((TimeSlot*)fb)->pageframeNumber;
	return ((TimeSlot*)fa)->time - ((TimeSlot*)fb)->time;
}

PageNumber FIFO(BM_BufferPool* const bm) {
	EI_BP* bufferPool = (EI_BP*)bm->mgmtData;
	//find the min of the creat time,i.e. the oldest bufferslot, if it it pinned, we cann't flush it, we need go 2nd smallest value,
	// if all slots is busy, we can't pin any more pages, for ther's no free slot
	PageNumber freeslot = -1;
	TimeSlot* timeSlots = malloc(sizeof(TimeSlot) * bm->numPages);
	for (int i = 0; i < bm->numPages; i++) {
		timeSlots[i].pageframeNumber =i;
		timeSlots[i].time = bufferPool->frame[i].virtual_t_now;
	}
	qsort(timeSlots, bm->numPages, sizeof(TimeSlot), FramesCompare);
	int i = 0;
	for (i = 0; i < bm->numPages; i++)
	{
		freeslot = timeSlots[i].pageframeNumber;
		if (bufferPool->frame[freeslot].fixCount == 0) {
			//nobody pin this slot,we will choose this
			break;
		}
	}
	free(timeSlots);
	if (i == bm->numPages)
	{// all slot are busy
		freeslot = -1;
	}
	return freeslot;
}
PageNumber LRU(BM_BufferPool* const bm) {
	EI_BP* bufferPool = (EI_BP*)bm->mgmtData;
	//find the min of the creat time,i.e. the oldest bufferslot, if it it pinned, we cann't flush it, we need go 2nd smallest value,
	// if all slots is busy, we can't pin any more pages, for ther's no free slot
	PageNumber freeslot = -1;
	TimeSlot* timeSlots = malloc(sizeof(TimeSlot) * bm->numPages);
	for (int i = 0; i < bm->numPages; i++) {
		timeSlots[i].pageframeNumber = i;
		timeSlots[i].time = bufferPool->frame[i].virtual_t_now;
	}
	qsort(timeSlots, bm->numPages, sizeof(TimeSlot), FramesCompare);
	int i = 0;
	for (i = 0; i < bm->numPages; i++)
	{
		freeslot = timeSlots[i].pageframeNumber;
		if (bufferPool->frame[freeslot].fixCount == 0) {
			//nobody pin this slot,we will choose this
			break;
		}
	}
	free(timeSlots);
	if (i == bm->numPages)
	{// all slot are busy
		freeslot = -1;
	}
	return freeslot;
}

RC shutdownBufferPool(BM_BufferPool* const bm)
{
	forceFlushPool(bm);// force all the page in bufferpool back to disk
	/*typedef struct ExtraInfo_BufferPools {
		SM_FileHandle* fh; //one bufferpool match one fileHandle
		frame* frame; //for frame info
		int firstIndex; //for FIFO

	}EI_BP;*/
	//Close the file then
	EI_BP* bufferPoolconten = (EI_BP*)bm->mgmtData;
	SM_FileHandle* fh = bufferPoolconten->fh;
	RC closeRC = closePageFile(fh);
	if (closeRC!=RC_OK)
	{
		return RC_WRITE_FAILED;
	}
	// free dynamic memory  in EI_BP;
	for (int i = 0; i < bm->numPages; i++) {
		free(bufferPoolconten->frame[i].memPage);
	}
	free(bufferPoolconten->fh);
	free(bufferPoolconten->frame);
	free(bufferPoolconten->dirtyflags);
	free(bufferPoolconten->fixCounts);
	free(bufferPoolconten->pagenumbers);
	//free EI_BP itself in BM_BufferPool;
	free(bm->mgmtData);
	return RC_OK;
}

// flush all dirty page to disk
	RC forceFlushPool(BM_BufferPool* const bm)
{
	EI_BP* bufferPoolconten = (EI_BP*)bm->mgmtData;
	frame* frames = bufferPoolconten->frame;
	for (int i = 0; i < bm->numPages; i++)
	{
		
		if (frames[i].dirty) {// write dirty page to disk
			int pagenum = frames[i].pageNum;
			SM_FileHandle* fh = bufferPoolconten->fh;
			writeBlock(pagenum, fh, frames[i].memPage);
			frames[i].dirty = false;
			bufferPoolconten->totalWrite++;//update writeIO
		}
	}

	return RC_OK;
}
// mark the page as dirty
RC markDirty(BM_BufferPool* const bm, BM_PageHandle* const page)
{
	int pagenum = page->pageNum;
	EI_BP* bufferPoolconten = (EI_BP*)bm->mgmtData;
	frame* frames = bufferPoolconten->frame;
	for (size_t i = 0; i < bm->numPages; i++)
	{
		// search for the page number to be marked
		if (frames[i].pageNum == pagenum) {
			frames[i].memPage = page->data;
			frames[i].dirty = true;
			return RC_OK;// return OK, exit for loop
		}
	}
	return RC_READ_NON_EXISTING_PAGE;
}

//unpinPage, but won't flush it to disk if it is dirty
RC unpinPage(BM_BufferPool* const bm, BM_PageHandle* const page)
{
	int pagenum = page->pageNum;
	EI_BP* bufferPoolconten = (EI_BP*)bm->mgmtData;
	frame* frames = bufferPoolconten->frame;
	for (size_t i = 0; i < bm->numPages; i++)
	{
		// search for the page number to be unpined
		if (frames[i].pageNum == pagenum) {
			frames[i].fixCount--;
			return RC_OK;// return OK, exit for loop
		}
	}
	return RC_READ_NON_EXISTING_PAGE;
}
//fore the page to disk if its dirty
RC forcePage(BM_BufferPool* const bm, BM_PageHandle* const page)
{
	EI_BP* bufferPoolconten = (EI_BP*)bm->mgmtData;
	frame* frames = bufferPoolconten->frame;
	int pagenum = page->pageNum;
	bool pageInPool = false;
	for (int i = 0; i < bm->numPages; i++)
	{
		if (pagenum==frames[i].pageNum) {// write dirty page to disk
			pageInPool = true;// page found in buffer pool
			int pagenum = frames[i].pageNum;
			if (frames[i].dirty) {
				SM_FileHandle* fh = bufferPoolconten->fh;
				writeBlock(pagenum, fh, frames[i].memPage);
				bufferPoolconten->totalWrite++;//update writeIO
				frames[i].dirty = false;
			}
			return RC_OK;// exit loop
		}
	}
	
	return RC_READ_NON_EXISTING_PAGE;
}

int hasCached(BM_BufferPool* const bm, const PageNumber pageNum) {
	const EI_BP* bufferPool = (const EI_BP*)bm->mgmtData;
	//iterate through the bufferframe
	for (int i = 0; i < bm->numPages; ++i) {
		if (bufferPool->frame[i].pageNum == pageNum) {
			//the page has been cached
			//return this frame index
			return i;
		}
	}
	//the page didn't in the memory
	return -1;
}
//Clients of the buffer manager can request pages identified by their position in the page file(page number) to be loaded in a page frame.
//This is called pinning a page.
//Internally, the buffer manager has to check whether the page requested by the client is already cached in a page frame.
//If this is the case, then the buffer simply returns a pointer to this page frame to the client.
//Otherwise, the buffer manager has to read this page from disk and decide in which page frame it should be stored
//(this is what the replacement strategy is for).
//Once an appropriate frame is foundand the page has been loaded, the buffer manager returns a pointer to this frame to the client.
RC pinPage(BM_BufferPool* const bm, BM_PageHandle* const page, const PageNumber pageNum)
{
	//init a virtual start time
	//every pin-operation will add 1 in it
	static int virtual_t = 0;
	EI_BP* bufferPool = (EI_BP*	)bm->mgmtData;
	//check bm
	if (bm == NULL)
		return RC_BUFFERPOOL_NOT_INIT;
	
	//check whether the page is already cached
	int frameIndex = hasCached(bm, pageNum);
	if (frameIndex != -1) {
		//has cached
		bufferPool->frame[frameIndex].fixCount++; //the count add 1: a new client pin this page
		page->pageNum = bufferPool->frame[frameIndex].pageNum; //put the info into the pageHandle
		page->data = bufferPool->frame[frameIndex].memPage;
		virtual_t++;// virtual time update as a new pin just happens
		if(bm->strategy== RS_LRU)// if it is FIFO we won't update the frame time, in FIFO, the time is when it is fetched from disk
		bufferPool->frame[frameIndex].virtual_t_now = virtual_t;// update the last access time
		return RC_OK;
	}
	else {
		//to find the free frame to store
		//frameIndex = -1
		//two situation : there is a empty frame or it's full and need to do the replacement
		PageNumber freeFrame = -1;
		bool isFull = true;
		/*FrameNumber lastFrameIndex = bm->numPages - 1;
		for (int i = 0; i <= lastFrameIndex; i++) {
			if (bufferPool->frame[i].fixCount == 0) {//change = to ==
				//bufferPool isn't full
				freeFrame = i;
				isFull = false;
				bufferPool->firstIndex = 0;
				break;
			}

		}*/
		if (isFull) {
			//the bufferPool is full
			switch (bm->strategy)
			{
			case RS_FIFO:
				freeFrame = FIFO(bm);
				break;
			case RS_LRU:
				freeFrame = LRU(bm);
				break;
			case RS_CLOCK:
				freeFrame = -2;
				break;
			case RS_LFU:
				freeFrame = -2;
				break;
			case RS_LRU_K:
				freeFrame = -2;
				break;
			}

			if (freeFrame == -1) {
				printf("can't replace");
				return RC_STORE_FAILED;
			}
			else if (freeFrame == -2) {
				printf("this stratege didn't implement");
				return RC_STORE_FAILED;
			}
		}
		//ensure the file capacity
		RC ensure = ensureCapacity(pageNum+1, bufferPool->fh);
		if ( ensure != RC_OK) 
			return ensure;
		BM_PageHandle bp;
		bp.data = bufferPool->frame[freeFrame].memPage;
		bp.pageNum = bufferPool->frame[freeFrame].pageNum;
		forcePage(bm,&bp);//force the old page to disk

		//read the page from disk into the freeFrame
		RC read = readBlock(pageNum, bufferPool->fh, bufferPool->frame[freeFrame].memPage);
		bufferPool->totalRead++;//debug info, read a bloack here
		if (read != RC_OK)
			return read;
			
		bufferPool->frame[freeFrame].fixCount++;
		bufferPool->frame[freeFrame].pageNum = pageNum;
		//bufferPool->frame[freeFrame].nowTime = time(NULL);

		virtual_t++;// new pin happens, update gloable time
		bufferPool->frame[freeFrame].virtual_t_now = virtual_t;// update last access time

		page->pageNum = bufferPool->frame[freeFrame].pageNum; //put the info into the pageHandle
		page->data = bufferPool->frame[freeFrame].memPage;
		return RC_OK;
	}
}


PageNumber* getFrameContents(BM_BufferPool* const bm)
{
	const EI_BP* bufferPool = (const EI_BP*)bm->mgmtData;
	PageNumber* a = bufferPool->pagenumbers;// use th preallocat memory with bm object to prevent memory leak, this memory will be free when bm is delete
	for (int i = 0; i < bm->numPages; i++)
		a[i] = bufferPool->frame[i].pageNum;
	return a;
}

bool* getDirtyFlags(BM_BufferPool* const bm)
{
	const EI_BP* bufferPool = (const EI_BP*)bm->mgmtData;
	bool* a = bufferPool->dirtyflags;// use th preallocat memory with bm object to prevent memory leak, this memory will be free when bm is delete
	for (int i = 0; i < bm->numPages; i++)
		a[i] = bufferPool->frame[i].dirty;
	return a;
}

//The getFixCounts function returns an array of ints(of size numPages) 
//where the ith element is the fix count of the page stored in the ith page frame.
//Return 0 for empty page frames.
int* getFixCounts(BM_BufferPool* const bm)
{
	const EI_BP* bufferPool = (const EI_BP*)bm->mgmtData;
	int* a = bufferPool->fixCounts;// same like before
	for (int i = 0; i < bm->numPages; i++) {
		if (bufferPool->frame[i].fixCount == -1)
			a[i] = 0;
		else
			a[i] = bufferPool->frame[i].fixCount;
	}
	return a;
}

int getNumReadIO(BM_BufferPool* const bm)
{
	const EI_BP* bufferPool = (const EI_BP*)bm->mgmtData;
	return bufferPool->totalRead ;
}

int getNumWriteIO(BM_BufferPool* const bm)
{
	const EI_BP* bufferPool = (const EI_BP*)bm->mgmtData;
	return bufferPool->totalWrite ;
}
