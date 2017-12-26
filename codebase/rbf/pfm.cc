#include "../rbf/pfm.h"
#include <errno.h>
#include <sys/stat.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

bool doesFileExists(const string &fileName){
	//Opens the file to check if it is present or not
	FILE * file = fopen(fileName.c_str(),"r");
	if(file!=NULL){

		fclose(file);
		return true;
	}
	return false;
}

RC PagedFileManager::createFile(const string &fileName)
{
	  fstream fileCreate; //object of fstream class

	  if(std::fstream(fileName.c_str())) {
		  return -1;
	  }

	   //opening file "sample.txt" in out(write) mode
	  fileCreate.open(fileName.c_str(),ios::out|ios::binary);

	//   cout<< "Creating file " << fileName.c_str() <<endl;

	   if(!fileCreate.is_open()) return -1;

	   //closing the file
	   fileCreate.close();

	   return 0;
}



RC PagedFileManager::destroyFile(const string &fileName)
{
	if( remove( fileName.c_str()) != 0 ) {
		return -1;
	} else {
		return 0;
	}
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(fileHandle.filef.is_open()){
		return -1;
	}
	fileHandle.filef.open(fileName.c_str(),ios::in|ios::binary |ios::out);

	if(!fileHandle.filef.is_open()) {

		return -1;
	} else {
		fileHandle.readMetaData(fileName,fileHandle);
		fileHandle.fileNameDebug = fileName;
	//	cout<<"Total Pages in " << fileHandle.fileNameDebug <<" while opening" << fileHandle.totalPages<<endl;


		return 0;
	}

}
int FileHandle::readMetaData(const string &fileName,FileHandle &fileHandle) {

		struct stat st;
	    if(stat(fileName.c_str(), &st) != 0) {
	    	// File does not exist
	    	return -1;

	    }
	    if (st.st_size == 0) {
	    	//write 4 zero to beginning of the file
	    	//typecasting of character into int http://courses.cs.vt.edu/~cs2604/fall00/binio.html#nonchar
	    	unsigned a[PAGE_SIZE/sizeof(unsigned)] = {0};
	    	a[3]=0;
	    //	a[0]= 0; a[1]=0;a[2]=0;a[3]=0;

	    	fileHandle.filef.seekp(0); //SEEKP for WRITE
	      	fileHandle.filef.write((char *)&a,PAGE_SIZE);
	    	flush(fileHandle.filef);
	    	fileHandle.filef.seekp(0); //HERE

	    	return 0;
	    } else
	    	if (st.st_size > 0) {

	    		unsigned *y=new unsigned[4];
	    		fileHandle.filef.seekg(0); //SEEKG for READ
	    		//myFile.read ((char*)y, sizeof (Data) * 10);
	    		fileHandle.filef.read((char*)y,sizeof(unsigned)*4);
	    	//	cout<<"READING DATA" << y[0]<<y[1]<<y[2]<<y[3];
	    		readPageCounter = y[0];
	    		writePageCounter = y[1];
	    		appendPageCounter = y[2];
	    		totalPages = y[3];
	    		fileHandle.filef.seekg(0); //HERE
	    		delete(y);
	    		return 0;

	    	}


}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
       //closing the file
	   if (fileHandle.filef.is_open()) {
		   fileHandle.writeMetaData();
//		   cout<<"Total Pages in " << fileHandle.fileNameDebug <<" while closing" << fileHandle.totalPages<<endl;
		   fileHandle.filef.close();
		   return 0;
	   } else {
		   return 0;
	   }

}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    totalPages = 0;
}


FileHandle::~FileHandle()
{
	flush(filef);
	filef.close();
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
 //   cout<< "Reading from page " << (pageNum) <<endl;

	pageNum++;

    filef.seekg(pageNum*PAGE_SIZE); //SEEKG for READ

    	//cout << "Error while seeking to Page Number : " << pageNum << " with error code : " << strerror(errno) << endl;

	//myFile.read ((char*)y, sizeof (Data) * 10);
	filef.read((char*)data,PAGE_SIZE);

	//cout << "Error during read Page: " << strerror(errno) << endl;

	if(!filef.good()) {
		readPageCounter--;
		return -1;
	}

	readPageCounter++;
	writeMetaData();

	return 0;
}
void FileHandle::writeMetaData() {

	filef.seekp(0);
	unsigned a[4]={readPageCounter, writePageCounter,appendPageCounter,totalPages};
	filef.write((char *)a,sizeof(unsigned)*4);
	flush(filef);
	filef.seekg(0);

}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	//Increment pageNum

	//cout<< "Writing to page " << pageNum<<endl;

	if(pageNum >=totalPages) {
		return -1;
	}
	pageNum++;
	//Navigate to the beginning of pageNum * 4096

	filef.seekp(pageNum*PAGE_SIZE,ios_base::beg);

	//Write the data
	filef.write((char *)data,PAGE_SIZE);
	flush(filef);

	//Increment counter
	writePageCounter++;

	//Write meta data
	writeMetaData();

	return 0;
	//return -1;
}


RC FileHandle::appendPage(const void *data)
{
	// Navigate to the end of the file
	    filef.seekp(0,ios_base::end);
	    // Write the contents to data to the end
	    filef.write((char*)data, PAGE_SIZE);
	    if (!filef.good()) {
	        // Something went wrong while writing the contents of the file
	        // This resulted in good bit not being set.
	        // Return unsuccessful
	        return -1;
	    }
	    // Increment total number of pages and append page counter
	    totalPages++;
	    appendPageCounter++;
	    // Write the metadata to disk
	       writeMetaData();
	    // Return successful
	    return 0;
}



unsigned FileHandle::getNumberOfPages()
{
//	cout<< "Total Number of Pages in"<< fileNameDebug << totalPages <<endl;
    return totalPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::setFile(FILE *pfile) {
	if(pfile!=NULL && file==NULL){
		file = pfile;
		return 0;
	}
	return -1;
}

FILE* FileHandle::getFile()
{
	return file;
}

RC FileHandle::findFreePage(const unsigned short &recordSize, unsigned short &slotNumber, PageNum &pageNumber) {

	//Iterate through all existing pages to check if they have the required space
	void *currentPage = malloc(PAGE_SIZE);
	int numberOfPages = getNumberOfPages();
	for (int i = numberOfPages-1; i >= 0; i--) {
		//Read the current page in memory
		this->readPage(i, currentPage);
		//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
		unsigned short sizeOfEachSlot = sizeof(short) * 2;
		//Finding the value of Free Space Offset
		unsigned short freeSpaceOffset;
		memcpy(&freeSpaceOffset, (char *)currentPage + PAGE_SIZE - sizeof(short), sizeof(short));
		//Finding the value of number of slots in this page
		unsigned short numberOfSlots;
		memcpy(&numberOfSlots, (char *)currentPage + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
		//Finding where the slot directory starts
		unsigned short endOfSlotSpace = PAGE_SIZE - ((numberOfSlots * sizeOfEachSlot)+(sizeof(short)*2));
		//Calculating the free space by sub where slot directory starts and free space offset
		unsigned short totalFreeSpace = endOfSlotSpace - freeSpaceOffset;
		//If there is available space is greater or equal to required space
		if (totalFreeSpace >= (recordSize + sizeOfEachSlot)) {
			//Slot Number will be current number of slots + 1
			slotNumber = calculateSlotNumber(currentPage);
			//Pagenumber will be where we found the free space
			pageNumber = i;
			free(currentPage);
			return 0;
		}
	}

	//If we didn't find free space in any of the above pages, then we will create a new page
	//Setting the Number of Slots to 0 in new page
	unsigned short totalSlots = 0;
	memcpy((char *)currentPage + (PAGE_SIZE - (sizeof(short) * 2)), &totalSlots, sizeof(short));
	//Setting the Free Space Offset to 0 in new page
	unsigned short freeSpaceOffset = 0;
	memcpy((char *)currentPage + (PAGE_SIZE - sizeof(short)), &freeSpaceOffset, sizeof(short));

	RC status = this->appendPage(currentPage);
	if (status == 0) {
			slotNumber = 1;
			//Since pageNumber starts from 0
			pageNumber = getNumberOfPages() -1;
			free(currentPage);
			return 0;
	}
	free(currentPage);
	return -1;
}

unsigned short FileHandle::calculateSlotNumber(void *page){
	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;
	//Finding the value of number of slots in this page
	unsigned short numberOfSlots;
	memcpy(&numberOfSlots, (char *)page + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
	for(int i=1;i<=numberOfSlots;i++){
		unsigned short recordLength;
		memcpy(&recordLength, (char *)page + (PAGE_SIZE - ((i * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
		if(recordLength==0){
			return i;
		}
	}
	return numberOfSlots + 1;
}


RC FileHandle::insertRecordInPage(const PageNum &pageNumber, const void *data, const unsigned short &size,const unsigned short &slotNumber) {

	void *pageData = malloc(PAGE_SIZE);

	//Reads the page in memory
	this->readPage(pageNumber, pageData);

	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;

	//Finding the value of Free Space Offset
	unsigned short freeSpaceOffset;
	memcpy(&freeSpaceOffset, (char *)pageData + PAGE_SIZE - sizeof(short), sizeof(short));

	//Finding the value of number of slots in this page
	unsigned short numberOfSlots;
	memcpy(&numberOfSlots, (char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));

	//Finding where the slot directory starts
	unsigned short endOfSlotSpace = PAGE_SIZE - ((numberOfSlots * sizeOfEachSlot)+(sizeof(short)*2));

	unsigned short newSlotLocation;
	if(slotNumber<=numberOfSlots){
		newSlotLocation = PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2));

		moveRecordDataRight(slotNumber, size, pageData);

		unsigned short recordOffset;
		memcpy(&recordOffset, (char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));

		//Inserting the new record length in slot info
		memcpy((char *)pageData + newSlotLocation + sizeof(short), &size, sizeof(short));

		//Inserting the record
		memcpy((char *)pageData + recordOffset, data, size);

	}else{
		//New Slot will be added in front of last slot directory entry
		newSlotLocation = endOfSlotSpace - sizeOfEachSlot;

		//Updating the number of slots
		memcpy((char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), &slotNumber, sizeof(short));

		//Inserting the new record offset in slot info
		memcpy((char *)pageData + newSlotLocation, &freeSpaceOffset, sizeof(short));

		//Inserting the new record length in slot info
		memcpy((char *)pageData + newSlotLocation + sizeof(short), &size, sizeof(short));

		//Inserting the record
		memcpy((char *)pageData + freeSpaceOffset, data, size);
	}
	//Updating the free space offset
	unsigned short newfreeSpaceOffset = freeSpaceOffset + size;
	memcpy((char *)pageData + (PAGE_SIZE - sizeof(short)), &newfreeSpaceOffset, sizeof(short));

	//Writing to the disk
	this->writePage(pageNumber, pageData);
	free(pageData);
	return 0;
}

RC FileHandle::readRecordFromPage(const PageNum &pageNumber, const unsigned short &slotNum, void *data, unsigned short recordSize) {
	void *pageData = malloc(PAGE_SIZE);
	//Reads the given page in memory
	this->readPage(pageNumber, pageData);
	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;
	//Finding the value of number of slots in this page
	unsigned short numberOfSlots;
	memcpy(&numberOfSlots, (char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
	if(slotNum<=numberOfSlots){
		//Finding the offset value where this record is saved using slot number
		unsigned short recordStartOffset;
		memcpy(&recordStartOffset, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));
		unsigned short recordLength;
		memcpy(&recordLength, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
		if(recordLength==0){
			free(pageData);
			return -1;
		}
		if(recordLength>0){
			int schemaVersion;
			memcpy(&schemaVersion, (char *)pageData + recordStartOffset, sizeof(int));
			if(schemaVersion>=0){
				memcpy((char *)data, (char *)pageData + recordStartOffset, recordLength);
				recordSize = recordLength;
				free(pageData);
				return 0;
			}else{
				unsigned newPageNumber;
				memcpy(&newPageNumber, (char *)pageData+recordStartOffset+sizeof(int), sizeof(unsigned));
				unsigned newSlotNumber;
				memcpy(&newSlotNumber, (char *)pageData+recordStartOffset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
				free(pageData);
				return readRecordFromPage(newPageNumber, newSlotNumber, data, recordSize);
			}
		}else{
			free(pageData);
			return -1;
		}
	}
	free(pageData);
	return -1;
}

RC FileHandle::deleteRecordFromPage(const PageNum &pageNumber, const unsigned short &slotNum) {
	void *pageData = malloc(PAGE_SIZE);
	//Reads the given page in memory
	this->readPage(pageNumber, pageData);
	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;
	//Finding the value of number of slots in this page
	unsigned short numberOfSlots;
	memcpy(&numberOfSlots, (char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
	if(slotNum<=numberOfSlots){
		//Finding the offset value where this record is saved using slot number
		unsigned short recordStartOffset;
		memcpy(&recordStartOffset, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));
		unsigned short recordLength;
		memcpy(&recordLength, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
		if(recordLength==0){
			free(pageData);
			return -1;
		}
		if(recordLength>0){
			int schemaVersion;
			memcpy(&schemaVersion, (char *)pageData + recordStartOffset, sizeof(int));
			if(schemaVersion>=0){
				unsigned short recordLength;
				memcpy(&recordLength, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
				moveRecordDataLeft(slotNum, recordLength, pageData);
				unsigned short newRecordLength = 0;
				//unsigned short newRecordOffset = PAGE_SIZE+1;
				memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), &newRecordLength, sizeof(short));
				//memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))), &newRecordOffset, sizeof(short));
				this->writePage(pageNumber,pageData);
				free(pageData);
				return 0;
			}else{
				unsigned pageNumberNew;
				memcpy(&pageNumberNew, (char *)pageData+recordStartOffset+sizeof(int), sizeof(unsigned));
				unsigned slotNumberNew;
				memcpy(&slotNumberNew, (char *)pageData+recordStartOffset+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
				moveRecordDataLeft(slotNum, 12, pageData);
				unsigned short newRecordLength = 0;
				//unsigned short newRecordOffset = PAGE_SIZE+1;
				memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), &newRecordLength , sizeof(short));
				//memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))), &newRecordOffset, sizeof(short));
				this->writePage(pageNumber,pageData);
				free(pageData);
				return deleteRecordFromPage(pageNumberNew, slotNumberNew);
			}
		}else{
			free(pageData);
			return -1;
		}

	}else{
		free(pageData);
		return -1;
	}
	free(pageData);
	return 0;
}

RC FileHandle::updateRecord(const PageNum &pageNumber, const unsigned short &slotNum, unsigned short newRecordLength, const void *newRecordData){
	void *pageData = malloc(PAGE_SIZE);
	//Reads the given page in memory
	this->readPage(pageNumber, pageData);
	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;
	unsigned short recordStartOffset;
	memcpy(&recordStartOffset, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));
	int schemaVersion;
	memcpy(&schemaVersion, (char *)pageData + recordStartOffset, sizeof(int));
	if(schemaVersion>=0){
		unsigned short prevRecordLength;
			unsigned short recordStartOffset;
			//Finding the offset value where this record is saved using slot number
			memcpy(&recordStartOffset, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));
			memcpy(&prevRecordLength, (char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
			if(prevRecordLength==newRecordLength){
				memcpy((char *)pageData + recordStartOffset, newRecordData, newRecordLength);
			}else if(newRecordLength<prevRecordLength){
				int difference = prevRecordLength - newRecordLength;
				memcpy((char *)pageData + recordStartOffset, newRecordData, newRecordLength);
				memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), &newRecordLength , sizeof(short));
				moveRecordDataLeft(slotNum, difference, pageData);
			}else{
				unsigned short freeSpaceOffset;
				memcpy(&freeSpaceOffset, (char *)pageData + PAGE_SIZE - sizeof(short), sizeof(short));
				//Finding the value of number of slots in this page
				unsigned short numberOfSlots;
				memcpy(&numberOfSlots, (char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
				//Finding where the slot directory starts
				unsigned short endOfSlotSpace = PAGE_SIZE - ((numberOfSlots * sizeOfEachSlot)+(sizeof(short)*2));
				//Calculating the free space by sub where slot directory starts and free space offset
				unsigned short freeSpaceOnCurrentPage = endOfSlotSpace - freeSpaceOffset;
				if(freeSpaceOnCurrentPage>(newRecordLength - prevRecordLength)){
					moveRecordDataRight(slotNum, (newRecordLength - prevRecordLength), pageData);
					memcpy((char *)pageData + recordStartOffset, newRecordData, newRecordLength);
					memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), &newRecordLength , sizeof(short));
				}else{
					unsigned newPageNumber;
					unsigned short newSlotNumber;
					findFreePage(newRecordLength, newSlotNumber, newPageNumber);
					unsigned short pointerLength = 12;
					void *pointerToRecord = malloc(pointerLength);
					int schemaVersion = -1;
					memcpy((char *)pointerToRecord, &schemaVersion, sizeof(int));
					memcpy((char *)pointerToRecord+sizeof(int), &newPageNumber, sizeof(int));
					memcpy((char *)pointerToRecord+(sizeof(int)*2), &newSlotNumber, sizeof(int));
					if(prevRecordLength==pointerLength){
						memcpy((char *)pageData + recordStartOffset, pointerToRecord, pointerLength);
					}else if(prevRecordLength > pointerLength){
						memcpy((char *)pageData + recordStartOffset, pointerToRecord, pointerLength);
						memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), &pointerLength , sizeof(short));
						moveRecordDataLeft(slotNum, (prevRecordLength - pointerLength), pageData);
					}else if(prevRecordLength < pointerLength && freeSpaceOnCurrentPage>(pointerLength-prevRecordLength)){
						moveRecordDataRight(slotNum, (pointerLength-prevRecordLength), pageData);
						memcpy((char *)pageData + recordStartOffset, pointerToRecord, pointerLength);
						memcpy((char *)pageData + (PAGE_SIZE - ((slotNum * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), &pointerLength , sizeof(short));
					}else{
						return -1;
					}
					free(pointerToRecord);
					insertRecordInPage(newPageNumber, newRecordData, newRecordLength, newSlotNumber);
				}
			}
			this->writePage(pageNumber,pageData);
			free(pageData);
	}else{
		unsigned pageNumberNew;
		memcpy(&pageNumberNew, (char *)pageData+sizeof(int), sizeof(unsigned));
		unsigned slotNumberNew;
		memcpy(&slotNumberNew, (char *)pageData+sizeof(int)+sizeof(unsigned), sizeof(unsigned));
		free(pageData);
		updateRecord(pageNumberNew, slotNumberNew, newRecordLength, newRecordData);
	}
	return 0;
}

//Takes Page where data needs to be moved, slotNumber - all slots after this will be updated, length - amount of shift
RC FileHandle::moveRecordDataLeft(const unsigned short &slotNum, const int length, const void *pageData){
	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;
	//Finding the value of number of slots in this page
	unsigned short numberOfSlots;
	memcpy(&numberOfSlots, (char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
	//Find the slots which needs to be updated -> currentSlot + 1 to numberOfSlots
	unsigned short slotNumber = slotNum + 1;
	while(slotNumber<=numberOfSlots){
		unsigned short recordLength;
		memcpy(&recordLength, (char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
		unsigned short recordStartOffset;
		memcpy(&recordStartOffset, (char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));
		unsigned short newOffset = recordStartOffset - length;
		memcpy((char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))), &newOffset , sizeof(short));
		if(recordLength>0){
			void *tempRecord = malloc(recordLength);
			memcpy((char *)tempRecord, (char *)pageData + recordStartOffset, recordLength);
			memcpy((char *)pageData + recordStartOffset - length, (char *)tempRecord, recordLength);
			free(tempRecord);
		}
		slotNumber++;
	}
	unsigned short prevFreeSpaceOffset;
	memcpy(&prevFreeSpaceOffset, (char *)pageData + (PAGE_SIZE - sizeof(short)), sizeof(short));
	unsigned short newFreeSpaceOffset = prevFreeSpaceOffset - length;
	memcpy((char *)pageData + (PAGE_SIZE - sizeof(short)), &newFreeSpaceOffset, sizeof(short));
    return 0;
}

//Takes Page where data needs to be moved, slotNumber - all slots after this will be updated, length - amount of shift
RC FileHandle::moveRecordDataRight(const unsigned short &slotNum, const int length, const void *pageData){
	//Each Slot is Record Offset of 2 Bytes and Record Length of 2 Bytes
	unsigned short sizeOfEachSlot = sizeof(short) * 2;
	//Finding the value of number of slots in this page
	unsigned short numberOfSlots;
	memcpy(&numberOfSlots, (char *)pageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));
	//slots which needs to be updated -> numberOfSlots to currentSlot + 1
	unsigned short slotNumber = numberOfSlots;
	while(slotNumber>slotNum){
		unsigned short recordLength;
		memcpy(&recordLength, (char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))) + sizeof(short), sizeof(short));
		unsigned short recordStartOffset;
		memcpy(&recordStartOffset, (char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))), sizeof(short));
		//update record offset
		unsigned short newOffset = recordStartOffset + length;
		memcpy((char *)pageData + (PAGE_SIZE - ((slotNumber * sizeOfEachSlot)+(sizeof(short)*2))), &newOffset , sizeof(short));
		if(recordLength>0){
			void *tempRecord = malloc(recordLength);
			memcpy((char *)tempRecord, (char *)pageData + recordStartOffset, recordLength);
			memcpy((char *)pageData + recordStartOffset + length, (char *)tempRecord, recordLength);
			free(tempRecord);
		}
		slotNumber--;
	}
	unsigned short prevFreeSpaceOffset;
	memcpy(&prevFreeSpaceOffset, (char *)pageData + (PAGE_SIZE - sizeof(short)), sizeof(short));
	unsigned short newFreeSpaceOffset = prevFreeSpaceOffset + length;
	memcpy((char *)pageData + (PAGE_SIZE - sizeof(short)), &newFreeSpaceOffset, sizeof(short));
    return 0;
}
