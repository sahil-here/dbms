#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include <stdio.h>
#include <math.h>
#include <iostream>
#include <cstring>
#include <fstream>
#include <cstdlib>
#include <sys/stat.h>
using namespace std;

class FileHandle;

class PagedFileManager
{
public:
	static PagedFileManager* instance();                                  // Access to the _pf_manager instance

	RC createFile    (const string &fileName);                            // Create a new file
	RC destroyFile   (const string &fileName);                            // Destroy a file
	RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
	RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
	PagedFileManager();                                                   // Constructor
	~PagedFileManager();                                                  // Destructor

private:
	static PagedFileManager *_pf_manager;
};


class FileHandle
{
protected:
	FILE *file;

public:
	// variables to keep the counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	string fileNameDebug=""; //TODO: DEBUG
	fstream filef;
	unsigned totalPages;

	FileHandle();                                                         // Default constructor
	~FileHandle();                                                        // Destructor

	RC readPage(PageNum pageNum, void *data);                             // Get a specific page
	RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
	RC appendPage(const void *data);                                      // Append a specific page
	unsigned getNumberOfPages();                                          // Get the number of pages in the file
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
	RC setFile(FILE *pfile);                                         // Sets the file this file handler is handling
	FILE* getFile();                                                      // Returns the file that this file handler is handling
	RC findFreePage(const unsigned short &recordSize, unsigned short &slotNumber, PageNum &pageNumber);
	RC insertRecordInPage(const PageNum &pageNumber, const void *data, const unsigned short &size, const unsigned short &slotNumber);
	RC readRecordFromPage(const PageNum &pageNumber, const unsigned short &slotNum, void *data, unsigned short recordSize);
	RC deleteRecordFromPage(const PageNum &pageNumber, const unsigned short &slotNum);
	RC updateRecord(const PageNum &pageNumber, const unsigned short &slotNum, unsigned short length, const void *newRecordData);
	RC moveRecordDataLeft(const unsigned short &slotNum, const int length, const void *pageData);
	RC moveRecordDataRight(const unsigned short &slotNum, const int length, const void *pageData);
	unsigned short calculateSlotNumber(void *page);
	long int findSize(const char *file_name);
	int readMetaData(const string &fileName,FileHandle &fileHandle);
	void writeMetaData();
}; 

#endif
