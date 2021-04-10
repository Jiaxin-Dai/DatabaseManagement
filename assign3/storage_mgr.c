#include "storage_mgr.h"
#include "malloc.h" //for malloc
#include "string.h" //for memset
#include <sys/stat.h> //for fstat
#include <errno.h> //for check error

extern int errno;

/*
The storage manager has to maintain several types of information for an open file:
The number of total pages in the file, 
the current page position (for reading and writing), 
the file name, 
and a POSIX file descriptor or FILE pointer.
*/
void initStorageManager(void)
{
	
}

/********************File Related Methods*******************/


//Create a new page file fileName. 
RC createPageFile(char* fileName)
{
	FILE *f = fopen(fileName, "wb+"); //"wb" means open a file and allow to write as a binary file
	if (f == NULL) {
		//write failed: maybe not permmission
		printError(RC_WRITE_FAILED);
		printf("fopen file failed in CreatPageFile()");
		return RC_WRITE_FAILED;
	}
	else {
		//create success and write into the new file
		char *pageFile = malloc(PAGE_SIZE);
		if (pageFile != 0) {
			memset(pageFile, 0, PAGE_SIZE); //memset copies 0 to the first PAGE_SIZE characters of the block of memory, which the 'pageFile' pointed to.

			if (fwrite(pageFile, 1, PAGE_SIZE, f) != PAGE_SIZE) {	 //every time write 1 bytes, write PAGE_SIZE times
				//fwrite will return the count number 1, or it write failed.
				printError(RC_WRITE_FAILED);
				printf("write new file failed in CreatPageFile()");
				return RC_WRITE_FAILED;
			}
		}
		
        free(pageFile); //free he allocated memory
		fclose(f); 
	}
	return RC_OK;
}


/*
Opens an existing page file. 
Should return RC_FILE_NOT_FOUND if the file does not exist. 
The second parameter is an existing file handle. 
If opening the file is successful, 
then the fields of this file handle should be 
initialized with the information about the opened file. 
For instance, you would have to read the total number of pages that are stored in the file from disk.
*/
RC openPageFile(char* fileName, SM_FileHandle* fHandle)
{
	FILE* f = fopen(fileName, "rb+");//"rb" means open a exsist file and allow to read as a binary file
	if (f == NULL) {
		//open failed
		printError(RC_FILE_NOT_FOUND);
		printf("fopen file failed in OpenPageFile()");
		return RC_FILE_NOT_FOUND;
	}
	else {
		//open success
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;
		fHandle->mgmtInfo = f;
		

		struct stat fileStat;
		int fd = fileno(f);	//fileno - map a stream pointer to a file descriptor
		//fstat(int const _FileHandle, struct stat* const _Stat)
		//the first passing argument is 'int'
		if (fstat(fd, &fileStat) < 0) {
			printf("fstat() failed in OpenPageFile()");
			return RC_FILE_HANDLE_NOT_INIT;
		}
			
		fHandle->totalNumPages = fileStat.st_size / PAGE_SIZE;

		/*
		This method to determine size of file has a vulnerability.
		Setting the file position indicator to end-of-file, 
		as with fseek(file, 0, SEEK_END), has undefined behavior for a binary stream
		*/
		//fseek(f, 0, SEEK_END);
		//int f_size = ftell(f);
		//fHandle->totalNumPages = f_size / PAGE_SIZE;

		return RC_OK;
	}
	
}

/*
Close an open page file or destroy (delete) a page file.
*/
RC closePageFile(SM_FileHandle* fHandle)
{
	if (fHandle == NULL) {
		// no fHandle
		printError(RC_FILE_HANDLE_NOT_INIT);
		printf("fHandle is NULL in ClosePageFile()");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->mgmtInfo == NULL) {
		//no FILE
		printError(RC_FILE_NOT_FOUND);
		printf("file is NULL in ClosePageFile()");
		return RC_FILE_NOT_FOUND;
	}
	else {
		if (fclose(fHandle->mgmtInfo) == 0) {
			fHandle->fileName = "";
			fHandle->totalNumPages = 0;
			fHandle->curPagePos = 0;
			fHandle->mgmtInfo = NULL;
			return RC_OK;
		}
		else {
			printf("close failed in closePageFile()");
			return RC_WRITE_FAILED;
		}

		
	}
}

RC destroyPageFile(char* fileName)
{	
	
	int rm = remove(fileName);
	if ( rm == 0) {
		//delete success
		return RC_OK;
	}
	else {
		//delete failed
		printf("%d\n",errno);
		printf("destory file failed in DestoryPageFile()");
		return RC_FILE_NOT_FOUND;
	}
}

/*****************************************************************/

/*******************Read and Write Methods************************/

/*
If the file has less than pageNum pages, 
the method should return RC_READ_NON_EXISTING_PAGE.
*/
RC readBlock(int pageNum, SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	if (fHandle == NULL) {
		// no fHandle
		printError(RC_FILE_HANDLE_NOT_INIT);
		printf("fHandle is Null in readBlock()");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->mgmtInfo == NULL) {
		//no FILE
		printError(RC_FILE_NOT_FOUND);
		printf("file is Null in readBlock()");
		return RC_FILE_NOT_FOUND;
	}
	else {
		if (pageNum < 0 && pageNum > fHandle->totalNumPages - 1) {
			//pageNum less than 0 or more than the totalNumPages
			printError(RC_READ_NON_EXISTING_PAGE);
			printf("pageNum doesn't exist in file in readBlock()");
			return RC_READ_NON_EXISTING_PAGE;
		}
		

		//pageNum belongs to totalNumPages
		FILE* f = fHandle->mgmtInfo;
		fseek(f, pageNum * PAGE_SIZE, SEEK_SET);
		if (fread(memPage, 1, PAGE_SIZE, f) != 0) {
			fHandle->curPagePos = pageNum ;
			//???????
			//fclose(f);
			return RC_OK;
		}
				
		else {
			//read failed
			printError(RC_READ_NON_EXISTING_PAGE);
			printf("read failed in readBlock()");
			return RC_READ_NON_EXISTING_PAGE;
		}
		

	}
	
}



//Return the current page position in a file
int getBlockPos(SM_FileHandle* fHandle)
{
	if (fHandle == NULL) {
		// no fHandle
		printError(RC_FILE_HANDLE_NOT_INIT);
		printf("fHandle is Null in getBlockPos()");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->mgmtInfo == NULL) {
		//no FILE
		printError(RC_FILE_NOT_FOUND);
		printf("file is Null in getBlockPos()");
		return RC_FILE_NOT_FOUND;
	}
	else
		return fHandle->curPagePos;
}





/*
The curPagePos should be moved to the page that was read. 
If the user tries to read a block before the first page or after the last page of the file, 
the method should return RC_READ_NON_EXISTING_PAGE.
*/

RC readFirstBlock(SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	readBlock(0, fHandle, memPage);
	return RC_OK;
}

RC readPreviousBlock(SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	if (getBlockPos(fHandle) > 0) {
		//read the perious block
		readBlock(getBlockPos(fHandle) - 1, fHandle, memPage);
		return RC_OK;
	}
	else {
		//current page is the first page 
		printError(RC_READ_NON_EXISTING_PAGE);
		printf("current page is the first page  -- in readPreviousBlock()");
		return RC_READ_NON_EXISTING_PAGE;
	}

}

RC readCurrentBlock(SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	//read current block
	readBlock(getBlockPos(fHandle), fHandle, memPage);
	return RC_OK;
}

RC readNextBlock(SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	if (getBlockPos(fHandle) !=	fHandle->totalNumPages - 1 ) {
		//read the next block
		readBlock(getBlockPos(fHandle) + 1, fHandle, memPage);
		return RC_OK;
	}
	else {
		//current page is the last page
		printError(RC_READ_NON_EXISTING_PAGE);
		printf("current page is the last page  -- in readLastBlock()");
		return RC_READ_NON_EXISTING_PAGE;
	}
}

RC readLastBlock(SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	//read the last block
	readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
	return RC_OK;
}


//Write a page to disk using either the current position or an absolute position.
RC writeBlock(int pageNum, SM_FileHandle* fHandle, SM_PageHandle memPage)
{
	
	if (fHandle == NULL) {
		// no fHandle
		printError(RC_FILE_HANDLE_NOT_INIT);
		printf("fHandle is Null in writeBlock()");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->mgmtInfo == NULL) {
		//no FILE
		printError(RC_FILE_NOT_FOUND);
		printf("file is Null in writeBlock()");
		return RC_FILE_NOT_FOUND;
	}
	
	else {
		if (pageNum < 0) {
			//pageNum less than 0 
			printError(RC_READ_NON_EXISTING_PAGE);
			printf("current page number is less than 0 -- in writeBlock()");
			return RC_READ_NON_EXISTING_PAGE; 

		}
		//////////////////////
		else if (pageNum > fHandle->totalNumPages - 1) {
			//pageNum more than the totalNumPages
			//extend the file
			//the first passing argument is the new Total pages Number, so it is "pageNum +1"
			//the index starts from 0, the total number is equal to pageNum + 1
			ensureCapacity(pageNum + 1, fHandle);
			
		}
		//pageNum belongs to totalNumPages
		fHandle->curPagePos = pageNum;
		if (memPage == NULL)
			printf("memPage is NULL");
		else
			writeCurrentBlock(fHandle, memPage);

		//////////////////////
		
		return RC_OK;
		
		
	}
}


RC writeCurrentBlock(SM_FileHandle* fHandle, SM_PageHandle memPage)

{
	//////////////////////
	FILE* f = fHandle->mgmtInfo;
	fseek(f, PAGE_SIZE * fHandle->curPagePos, SEEK_SET); //SEEK_SET means the beginning of file
	int writebytes = fwrite(memPage, 1, PAGE_SIZE, f);
	if ( writebytes< 0) {
		//fwrite will return the count number 1, or it write failed.
		
		printError(RC_READ_NON_EXISTING_PAGE);
		printf("write failed in WriteCurrentBlock()");
		return RC_READ_NON_EXISTING_PAGE;

	}
	fHandle->curPagePos++; //update the current page position
	//////////////////////
	//fclose(f);
	return RC_OK;
}

/*
The new last page should be filled with zero bytes.
*/
RC appendEmptyBlock(SM_FileHandle* fHandle)
{

	if (fHandle == NULL) {
		// no fHandle
		printError(RC_FILE_HANDLE_NOT_INIT);
		printf("fHandle is null in appendEmptyBlock()");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->mgmtInfo == NULL) {
		//no FILE
		printError(RC_FILE_NOT_FOUND);
		printf("file is null in appendEmptyBlock()");
		return RC_FILE_NOT_FOUND;
	}
	else {
		FILE *f = fHandle->mgmtInfo;
		
		fseek(f, fHandle->totalNumPages * PAGE_SIZE, SEEK_SET); 
		/*
		from the file beginning and offsite is the total size, 
		avoid the SEEK_END may have undefined behavior for a binary stream.
		*/
		char* pageFile = malloc(PAGE_SIZE);
		if (pageFile != 0) {
			memset(pageFile, 0, PAGE_SIZE);	//filled with 0
			if (fwrite(pageFile, PAGE_SIZE, 1, f) != 1) {
				//fwrite will return the count number 1, or it write failed.
				printError(RC_WRITE_FAILED);
				printf("write failed in appendFileBlock()");
				return RC_WRITE_FAILED;
			}
		}
		//fclose(f);  
		//update the info of the fileHandle
		fHandle->totalNumPages++;
		fHandle->curPagePos++;
		return RC_OK;
		
	}
}

/*
If the file has less than numberOfPages pages then increase the size to numberOfPages. 
*/
RC ensureCapacity(int numberOfPages, SM_FileHandle* fHandle)
{
	if (fHandle == NULL) {
		// no fHandle
		printError(RC_FILE_HANDLE_NOT_INIT);
		printf("fHandle is null in ensureCapacity()");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->mgmtInfo == NULL) {
		//no FILE
		printError(RC_FILE_NOT_FOUND);
		printf("file is null in ensureCapacity()");
		return RC_FILE_NOT_FOUND;
	}
	else {
		//cause in writeBlock(), we have checked the numberOfPages is more than 0
		//Increase the number of pages in the file by one. 
		for (int i = 0; i < numberOfPages - fHandle->totalNumPages; i++) {
			if (appendEmptyBlock(fHandle) != RC_OK) {
				printError(RC_WRITE_FAILED);
				printf("write failed in ensureCapacity()");
				return RC_WRITE_FAILED;
			}

		}
		return RC_OK;
	}
	
}
