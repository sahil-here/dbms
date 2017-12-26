
#include "ix.h"
#include <map>
#include <string>
#include <iomanip>
#include <locale>
#include <sstream>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	RC a = PagedFileManager::instance()->openFile(fileName,ixfileHandle.fileHandle);
	if(a==-1){
		return -1;
	}
	unsigned readPage;
	unsigned writePage;
	unsigned appendPage;
	return ixfileHandle.collectCounterValues(readPage,writePage,appendPage);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	return PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
}

int IndexManager::findLeafPage(IXFileHandle &ixfileHandle, const Attribute attribute, const void* key, int &pageNumber){
	if(pageNumber==-1){
		return -1;
	}

	void *pageData = malloc(PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(pageNumber,pageData);
	if(rc==-1){
		free(pageData);
		return -1;
	}

	int nodeType;
	memcpy(&nodeType, (char *)pageData + PAGE_SIZE - sizeof(short) -sizeof(int), sizeof(int));

	unsigned short freeSpaceOffset;
	memcpy(&freeSpaceOffset, (char *)pageData + PAGE_SIZE - sizeof(short), sizeof(short));
	int childPageNumber = -1;

	//nodeType=0 means non-leaf node page
	if(nodeType==0){
		unsigned short offset = sizeof(unsigned);
		while(offset<freeSpaceOffset){
			int lengthOfLeftKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
			void *leftKey = malloc(lengthOfLeftKey);
			getKeyValue(attribute, pageData, leftKey, offset);
			int inputKeyLength = (attribute.type==TypeVarChar)?getKeyLength(attribute,key,0)-3:4;
			void *inputKeyValue = malloc(inputKeyLength);
			getKeyValue(attribute,key,inputKeyValue,0);
			if(compareKey(attribute, inputKeyValue, leftKey, GE_OP)==1){
				offset = offset + getKeyLength(attribute, pageData, offset) + sizeof(unsigned);
				if(freeSpaceOffset==offset){
					//It means it is the last pointer in the node
					memcpy(&childPageNumber, (char *)pageData + offset - sizeof(int), sizeof(int));
					free(inputKeyValue);
					free(leftKey);
					break;
				}else if(freeSpaceOffset>offset){
					//checking the right key if it is greater than the given key
					int lengthOfRightKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
					void *rightKey = malloc(lengthOfRightKey);
					getKeyValue(attribute, pageData, rightKey, offset);
					if(compareKey(attribute, inputKeyValue, rightKey, LT_OP)==1){
						memcpy(&childPageNumber, (char *)pageData + offset - sizeof(int), sizeof(int));
						free(inputKeyValue);
						free(leftKey);
						free(rightKey);
						break;
					}else{
						free(inputKeyValue);
						free(leftKey);
						free(rightKey);
						continue;
					}
				}else{
					free(leftKey);
					return -1;
				}
			}else if(compareKey(attribute, inputKeyValue, leftKey, LT_OP)==1){
				//If the key is less than the first key
				memcpy(&childPageNumber, (char *)pageData+offset-sizeof(unsigned), sizeof(int));
				free(inputKeyValue);
				free(leftKey);
				break;
			}
		}
		free(pageData);
		return findLeafPage(ixfileHandle, attribute, key, childPageNumber);
	}else if(nodeType==1){
		free(pageData);
		return pageNumber;
	}
	free(pageData);
	return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	unsigned lengthOfGivenKey = getKeyLength(attribute, key, 0);

	int requiredSpace = sizeof(int) +  lengthOfGivenKey + sizeof(unsigned) + sizeof(unsigned);

	if(ixfileHandle.fileHandle.getNumberOfPages()==0){
		//if number of pages is 0, then create a new page which will act as root index page and create 2 pages for leaf nodes
		void *rootPage = malloc(PAGE_SIZE);
		unsigned leftPageNumber = 1;
		memcpy(rootPage,&leftPageNumber,sizeof(unsigned));
		memcpy((char*)rootPage+sizeof(unsigned),key,lengthOfGivenKey);
		unsigned rightPageNumber = 2;
		memcpy((char*)rootPage+sizeof(unsigned)+lengthOfGivenKey,&rightPageNumber,sizeof(unsigned));
		int nodeTypeForNonLeafNodes = 0;
		memcpy((char*)rootPage+PAGE_SIZE-sizeof(int)-sizeof(short),&nodeTypeForNonLeafNodes,sizeof(int));
		unsigned short freeSpaceOffsetOfRoot = 8 + lengthOfGivenKey;
		memcpy((char*)rootPage+PAGE_SIZE-sizeof(short),&freeSpaceOffsetOfRoot,sizeof(short));
		ixfileHandle.fileHandle.appendPage(rootPage);
		free(rootPage);


		void *leftPage = malloc(PAGE_SIZE);
		//Set Next Pointer
		int nextPageNumber = 2;
		memcpy((char *)leftPage+PAGE_SIZE-10, &nextPageNumber, sizeof(int));
		//Set NodeType
		int nodeTypeForLeafNode = 1;
		memcpy((char *)leftPage+PAGE_SIZE-6,&nodeTypeForLeafNode, sizeof(int));
		//Set FreeSpaceOffset
		unsigned short freeSpaceOffsetForLeftNode = 0;
		memcpy((char *)leftPage+PAGE_SIZE-2,&freeSpaceOffsetForLeftNode, sizeof(short));
		ixfileHandle.fileHandle.appendPage(leftPage);
		free(leftPage);


		void *rightPage = malloc(PAGE_SIZE);
		//Set Next Pointer
		int nextPageNumberForRightNode = -1;
		memcpy((char *)rightPage+PAGE_SIZE-10, &nextPageNumberForRightNode, sizeof(int));
		//Set NodeType
		int nodeTypeForLeafNodeForRightNode = 1;
		memcpy((char *)rightPage+PAGE_SIZE-6,&nodeTypeForLeafNodeForRightNode, sizeof(int));
		//Add the entry
		int deletedFlag = 0;
		memcpy((char *)rightPage,&deletedFlag,sizeof(int));
		memcpy((char*)rightPage+sizeof(int),key,lengthOfGivenKey);
		memcpy((char*)rightPage+sizeof(int)+lengthOfGivenKey,&rid.pageNum,sizeof(unsigned));
		memcpy((char*)rightPage+sizeof(int)+lengthOfGivenKey+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
		unsigned short freeSpaceOffsetForRightNode = requiredSpace;
		memcpy((char*)rightPage+PAGE_SIZE-sizeof(short),&freeSpaceOffsetForRightNode,sizeof(short));
		ixfileHandle.fileHandle.appendPage(rightPage);
		//printBtree(ixfileHandle,attribute);
		free(rightPage);

	}else{
		//newKey and newPageNumber will be used during split
		void *newKey = malloc(PAGE_SIZE);
		int newPageNumber = -1;
		//Assuming page 0 will always be root page
		insert(ixfileHandle,attribute,key,0,rid,newKey,newPageNumber);
		//printBtree(ixfileHandle,attribute);
	/*	IX_ScanIterator ix_ScanIterator;
		scan(ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
		ix_ScanIterator.close(); */
		free(newKey);
	}
    return 0;
}

unsigned short IndexManager::findOffsetForNewKey(Attribute attribute, void *pageData, const void *key, unsigned short endOffset){

	unsigned short offset = sizeof(unsigned);

	while(offset<endOffset){
		int lengthOfKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
		void *nextKey = malloc(lengthOfKey);
		getKeyValue(attribute, pageData, nextKey, offset);
		int lengthOfInputKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,key,0)-3:4;
		void *valueOfInputKey = malloc(lengthOfInputKey);
		getKeyValue(attribute,key,valueOfInputKey,0);
		if(compareKey(attribute, valueOfInputKey, nextKey, GT_OP)==1){
			free(valueOfInputKey);
			free(nextKey);
			offset += getKeyLength(attribute,pageData,offset) + sizeof(unsigned);
		}else{
			free(valueOfInputKey);
			free(nextKey);
			return offset;
		}
	}

	//It means we have reached end of current keys and key should be inserted at last
	return endOffset;
}

unsigned short IndexManager::findOffsetForNewKeyInLeafNode(Attribute attribute, void *pageData, const void *key, unsigned short endOffset){

	unsigned short offset = 0;

	while(offset<endOffset){
		int lengthOfKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+4)-3:4;
		void *nextKey = malloc(lengthOfKey);
		getKeyValue(attribute, pageData, nextKey, offset+4);
		int lengthOfInputKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,key,0)-3:4;
		void *valueOfInputKey = malloc(lengthOfInputKey);
		getKeyValue(attribute,key,valueOfInputKey,0);
		if(compareKey(attribute, valueOfInputKey, nextKey, GE_OP)==1){
			free(valueOfInputKey);
			free(nextKey);
			offset += getKeyLength(attribute,pageData,offset+4) + 8;
		}else{
			free(valueOfInputKey);
			free(nextKey);
			return offset;
		}
	}

	//It means we have reached end of current keys and key should be inserted at last
	return endOffset;
}

RC IndexManager::insert(IXFileHandle &ixfileHandle, Attribute attribute, const void *key, const unsigned &pageNumber, const RID &rid, void *newKey, int &newPageNumber){
	void *pageData = malloc(PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(pageNumber,pageData);
	if(rc==-1){
		free(pageData);
		return -1;
	}

	unsigned lengthOfGivenKey = (attribute.type==TypeVarChar)?getKeyLength(attribute, key, 0)-3:4;
	void *keyValue = malloc(lengthOfGivenKey);
	getKeyValue(attribute,key,keyValue,0);

	//cout<< "The given key is " << string((char*)(keyValue)) << endl;

	int nodeType;
	memcpy(&nodeType, (char *)pageData + PAGE_SIZE - sizeof(short) -sizeof(int), sizeof(int));

	unsigned short freeSpaceOffset;
	memcpy(&freeSpaceOffset, (char *)pageData + PAGE_SIZE - sizeof(short), sizeof(short));
	unsigned childPageNumber;

	//nodeType=0 means non-leaf node page
	if(nodeType==0){
		unsigned short offset = sizeof(unsigned);
		while(offset<freeSpaceOffset){
			int lengthOfKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
			void *leftKey = malloc(lengthOfKey);
			getKeyValue(attribute, pageData, leftKey, offset);
			if(compareKey(attribute, keyValue, leftKey, GE_OP)==1){
				offset = offset + getKeyLength(attribute, pageData, offset) + sizeof(unsigned);
				if(freeSpaceOffset==offset){
					//It means it is the last pointer in the node
					memcpy(&childPageNumber, (char *)pageData + offset - sizeof(unsigned), sizeof(unsigned));
					free(leftKey); //check if this works here
					break;
				}else if(freeSpaceOffset>offset){
					//checking the right key if it is greater than the given key
					int lengthOfKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
					void *rightKey = malloc(lengthOfKey);
					getKeyValue(attribute, pageData, rightKey, offset);
					if(compareKey(attribute, keyValue, rightKey, LT_OP)==1){
						memcpy(&childPageNumber, (char *)pageData + offset - sizeof(unsigned), sizeof(unsigned));
						free(leftKey);
						free(rightKey);
						break;
					}else{
						//set mem of leftKey and rightKey as 0 to be safe
						free(leftKey);
						free(rightKey);
						continue;
					}
				}else{
					free(leftKey);
					free(keyValue);
					free(pageData);
					return -1;
				}
			}else if(compareKey(attribute, keyValue, leftKey, LT_OP)==1){
				//If the key is less than the first key
				memcpy(&childPageNumber, (char *)pageData, sizeof(unsigned));
				free(leftKey);
				break;
			}
		}

		insert(ixfileHandle, attribute, key, childPageNumber, rid, newKey, newPageNumber);

		if(newPageNumber==-1){
			free(keyValue);
			free(pageData);
			return 0;
		}else{
			//Write the code to handle split
			int spaceOnCurrentPage =  PAGE_SIZE - freeSpaceOffset - 4 -2;

			int lengthOfNewKey = getKeyLength(attribute, newKey, 0);

			int requiredSpace = lengthOfNewKey + sizeof(unsigned);

			if(requiredSpace<spaceOnCurrentPage){
				//find a place for inserting this key

				unsigned short offsetForNewKey = findOffsetForNewKey(attribute,pageData,newKey,freeSpaceOffset);

				unsigned short lengthOfDataToMoveRight = freeSpaceOffset - offsetForNewKey;

				void *dataToMove = malloc(lengthOfDataToMoveRight);
				memcpy(dataToMove,(char *)pageData+offsetForNewKey,lengthOfDataToMoveRight);

				memcpy((char *)pageData+offsetForNewKey,newKey,lengthOfNewKey);
				memcpy((char *)pageData+offsetForNewKey+lengthOfNewKey,&newPageNumber,sizeof(unsigned));

				memcpy((char *)pageData+offsetForNewKey+requiredSpace,dataToMove,lengthOfDataToMoveRight);

				unsigned short newFreeSpaceOffset = freeSpaceOffset + requiredSpace;
				memcpy((char *)pageData+PAGE_SIZE-sizeof(short),&newFreeSpaceOffset, sizeof(short));
				ixfileHandle.fileHandle.writePage(pageNumber,pageData);
				newPageNumber = -1;
				newKey = NULL;
				free(dataToMove);
				free(keyValue);
				free(pageData);
				return 0;
			}else{
				//Split the current non leaf node

				void* newBigPage = malloc(requiredSpace+freeSpaceOffset);
				memcpy(newBigPage,pageData,freeSpaceOffset);

				unsigned short offsetForNewKey = findOffsetForNewKey(attribute,newBigPage,newKey,freeSpaceOffset);
				unsigned short lengthOfDataToMoveRight = freeSpaceOffset - offsetForNewKey;

				void *dataToMove = malloc(lengthOfDataToMoveRight);
				memcpy(dataToMove,(char *)newBigPage+offsetForNewKey,lengthOfDataToMoveRight);
				memcpy((char *)newBigPage+offsetForNewKey,newKey,lengthOfNewKey);
				memcpy((char *)newBigPage+offsetForNewKey+lengthOfNewKey,&newPageNumber,sizeof(unsigned));
				memcpy((char *)newBigPage+offsetForNewKey+requiredSpace,dataToMove,lengthOfDataToMoveRight);
				free(dataToMove);

				int keyCount=0;
				unsigned short keyCountOffset = 4;
				while(keyCountOffset<freeSpaceOffset){
					keyCount++;
					keyCountOffset += getKeyLength(attribute,pageData,keyCountOffset) + 4;
				}

				int numberOfKeysToSkip = keyCount/2;
				unsigned short splitOffset = 4;
				for(int i=1;i<=numberOfKeysToSkip;i++){
					splitOffset += getKeyLength(attribute,pageData,splitOffset) + 4;
				}

				memcpy(pageData,(char *)newBigPage,splitOffset);

				//Setting the newKey to propagate up
				int lengthOfThisNewKey = (attribute.type==TypeVarChar)?getKeyLength(attribute,newBigPage,splitOffset)-4:4;
				if(attribute.type==TypeVarChar){
					memcpy(newKey,&lengthOfThisNewKey,sizeof(int));
					memcpy((char *)newKey+4,(char *)newBigPage+splitOffset+4,lengthOfThisNewKey);
				//	int newKeyLength = getKeyLength(attribute,newKey,0)-3;
				//	void * valueOfNewKey = malloc(newKeyLength);
				//	getKeyValue(attribute,newKey,valueOfNewKey,0);
				//	string abc = string((char *)valueOfNewKey);
				//	cout << "The value set inside new key is " << abc << endl;

				}else{
					memcpy(newKey,(char *)newBigPage+splitOffset,4);
				}
				int lengthOfThisKey = getKeyLength(attribute,newBigPage,splitOffset);
				void *newNodePage = malloc(PAGE_SIZE);
				memcpy(newNodePage,(char *)newBigPage+splitOffset+lengthOfThisKey,freeSpaceOffset+requiredSpace-splitOffset-lengthOfThisKey);
				unsigned short freeSpaceOffsetForNewNode = freeSpaceOffset+requiredSpace-splitOffset-lengthOfThisKey;
				memcpy((char *)newNodePage+PAGE_SIZE-sizeof(short),&freeSpaceOffsetForNewNode,sizeof(short));
				int nonLeafNodeType = 0;
				memcpy((char *)newNodePage+PAGE_SIZE-6,&nonLeafNodeType,sizeof(int));
				ixfileHandle.fileHandle.appendPage(newNodePage);
				free(newBigPage);
				free(newNodePage);
				newPageNumber = ixfileHandle.fileHandle.getNumberOfPages() -1;


				if(pageNumber==0){
					void *newLeftPage = malloc(PAGE_SIZE);
					memcpy(newLeftPage,pageData,splitOffset);
					//set node type
					int nodeType = 0;
					memcpy((char *)newLeftPage+PAGE_SIZE-6,&nodeType,sizeof(int));
					//set free space offset and then persist
					memcpy((char *)newLeftPage+PAGE_SIZE-2,&splitOffset,sizeof(short));
					ixfileHandle.fileHandle.appendPage(newLeftPage);
					unsigned newLeftPageNumber = ixfileHandle.fileHandle.getNumberOfPages() -1;
				//	validateNonLeafNode(ixfileHandle,attribute,newLeftPageNumber);
					memcpy(pageData,&newLeftPageNumber,sizeof(unsigned));
					memcpy((char *)pageData+sizeof(unsigned),newKey,lengthOfThisKey);
					memcpy((char *)pageData+sizeof(unsigned)+lengthOfThisKey,&newPageNumber,sizeof(unsigned));
					unsigned short updatedFreeSpaceOffset = 4+lengthOfThisKey+4;
					memcpy((char *)pageData+PAGE_SIZE-2,&updatedFreeSpaceOffset,sizeof(short));
					int nodeTypeForRoot = 0;
					memcpy((char *)pageData+PAGE_SIZE-6,&nodeTypeForRoot,sizeof(int));
					ixfileHandle.fileHandle.writePage(0,pageData);
					unsigned int rootPage = 0;
				//	validateNonLeafNode(ixfileHandle,attribute,rootPage);
					free(newLeftPage);
					free(keyValue);
					free(pageData);
					return 0;
				}else{
					//find a place to add the newPage based on new key on current page
					ixfileHandle.fileHandle.writePage(pageNumber,pageData);
					//validateNonLeafNode(ixfileHandle,attribute,pageNumber);
					free(pageData);
					return 0;
				}



			}
		}
	}else if(nodeType==1){
		//If the node is a leaf node
		//Leaf Node Structure: Leaf node entries | Free Space | 4 Byte Next Page Number | 4 Byte Node Type | Short Type Free Space offset
		//Leaf Node Entry Structure: 4 byte int deleted flag | key | 8 byte RID - first 4 byte page Number, next 4 byte slot number
		int requiredSpace = sizeof(int) +  getKeyLength(attribute,key,0) + sizeof(unsigned) + sizeof(unsigned);
		int availableSpace = PAGE_SIZE - sizeof(short) - sizeof(int) - sizeof(unsigned) - freeSpaceOffset;
		if(availableSpace>=requiredSpace){
			//Add the entry in the required position
			//To add entry find the position by comparing the keys from starting such that k(i)<=key<k(i+1) or the last position
			unsigned short position = 0;
			if(freeSpaceOffset==0){
				//This means it is the first entry
				int deletedFlag = 0;
				memcpy((char *)pageData,&deletedFlag,sizeof(int));
				memcpy((char*)pageData+sizeof(int),key,lengthOfGivenKey);
				memcpy((char*)pageData+sizeof(int)+lengthOfGivenKey,&rid.pageNum,sizeof(unsigned));
				memcpy((char*)pageData+sizeof(int)+lengthOfGivenKey+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
				unsigned short newFreeSpaceOffset = freeSpaceOffset + requiredSpace;
				memcpy((char*)pageData+PAGE_SIZE-sizeof(short),&newFreeSpaceOffset,sizeof(short));
				RC rc = ixfileHandle.fileHandle.writePage(pageNumber,pageData);
				if(rc==-1){
					free(keyValue);
					free(pageData);
					return -1;
				}
				newPageNumber = -1;
				free(keyValue);
				free(pageData);
				return 0;
			}
			while(position<freeSpaceOffset){
						int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,position+sizeof(int))-3:4;
						void *leftLeafKey = malloc(keySize);
						getKeyValue(attribute, pageData, leftLeafKey, position+sizeof(int));
						if(compareKey(attribute, keyValue, leftLeafKey, GE_OP)==1){
							position = position + 4 + getKeyLength(attribute, pageData, position+sizeof(int)) + 8;
							if(freeSpaceOffset>position){
								//checking the right key if it is greater than the given key
								int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,position+sizeof(int))-3:4;
								void *rightLeafKey = malloc(keySize);
								getKeyValue(attribute, pageData, rightLeafKey, position+sizeof(int));
								if(compareKey(attribute, keyValue, rightLeafKey, LT_OP)==1){
									//We have found the required position
									//move data right by requiredSpace amount

									void *temp = malloc(freeSpaceOffset-position);
									memcpy(temp,(char *)pageData+position,freeSpaceOffset-position);
									int deletedFlag = 0;
									memcpy((char *)pageData+position,&deletedFlag,sizeof(int));
									memcpy((char*)pageData+position+sizeof(int),key,getKeyLength(attribute,key,0));
									memcpy((char*)pageData+position+sizeof(int)+getKeyLength(attribute,key,0),&rid.pageNum,sizeof(unsigned));
									memcpy((char*)pageData+position+sizeof(int)+getKeyLength(attribute,key,0)+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
									memcpy((char*)pageData+position+requiredSpace,temp,freeSpaceOffset-position);
									//Update the free Space Offset
									unsigned short newFreeSpaceOffset = freeSpaceOffset + requiredSpace;
									memcpy((char*)pageData+PAGE_SIZE-sizeof(short),&newFreeSpaceOffset,sizeof(short));
									free(temp);
									free(leftLeafKey);
									free(rightLeafKey); //check if this works here
									break;
								}else{
									//set mem of leftKey and rightKey as 0 to be safe
									free(rightLeafKey);
									free(leftLeafKey);
									continue;
								}

							}else{
								//Insert the entry at the end
								int deletedFlag = 0;
								memcpy((char *)pageData+freeSpaceOffset,&deletedFlag,sizeof(int));
								memcpy((char*)pageData+freeSpaceOffset+sizeof(int),key,getKeyLength(attribute,key,0));
								memcpy((char*)pageData+freeSpaceOffset+sizeof(int)+getKeyLength(attribute,key,0),&rid.pageNum,sizeof(unsigned));
								memcpy((char*)pageData+freeSpaceOffset+sizeof(int)+getKeyLength(attribute,key,0)+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
								unsigned short newFreeSpaceOffset = freeSpaceOffset + requiredSpace;
								memcpy((char*)pageData+PAGE_SIZE-sizeof(short),&newFreeSpaceOffset,sizeof(short));
								free(leftLeafKey);
								break;
							}
						}else if(compareKey(attribute, keyValue, leftLeafKey, LT_OP)==1){
							//If the key is less than the first key
							//Then insert the key at the front
							//move data by the requiredSpace Amount
							void *temp = malloc(freeSpaceOffset);
							memcpy(temp,(char *)pageData,freeSpaceOffset);
							//memcpy the new entry at offset 0
							int deletedFlag = 0;
							memcpy((char *)pageData,&deletedFlag,sizeof(int));
							memcpy((char*)pageData+sizeof(int),key,getKeyLength(attribute,key,0));
							memcpy((char*)pageData+sizeof(int)+getKeyLength(attribute,key,0),&rid.pageNum,sizeof(unsigned));
							memcpy((char*)pageData+sizeof(int)+getKeyLength(attribute,key,0)+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
							memcpy((char*)pageData+requiredSpace,temp,freeSpaceOffset);
							unsigned short newFreeSpaceOffset = freeSpaceOffset + requiredSpace;
							memcpy((char*)pageData+PAGE_SIZE-sizeof(short),&newFreeSpaceOffset,sizeof(short));
							free(temp);
							free(leftLeafKey);
							break;
						}
					}
			RC rc = ixfileHandle.fileHandle.writePage(pageNumber,pageData);
			if(rc==-1){
				free(keyValue);
				free(pageData);
				return -1;
			}
			newPageNumber = -1;
			free(keyValue);
			free(pageData);
			return 0;
		}else{

			//Split the node
			unsigned short offset = 0;

			int keyCount=0;
			unsigned short keyCountOffset = 0;
			while(keyCountOffset<freeSpaceOffset){
				keyCount++;
				keyCountOffset += 4 + getKeyLength(attribute,pageData,keyCountOffset+4) + 8;
			}

			int numberOfKeysToSkip = (keyCount%2==0)? (keyCount/2-1) : (keyCount/2);

			for(int i=1;i<=numberOfKeysToSkip;i++){
				offset += 4 + getKeyLength(attribute,pageData,offset+4) + 8;
			}

			while(offset<freeSpaceOffset){
				//Read the next 2 keys and compare them, if they are not equal return the 2nd key
				int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+sizeof(int))-3:4;
				void *firstKey = malloc(keySize);
				getKeyValue(attribute, pageData, firstKey, offset+sizeof(int));
				int firstRecordLength = 0;
				int lengthOfFirstKey = getKeyLength(attribute, pageData, offset+4);
				firstRecordLength = 4 + lengthOfFirstKey + 8;
				offset += firstRecordLength;
				keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+sizeof(int))-3:4;
				void *secondKey = malloc(keySize);
				getKeyValue(attribute, pageData, secondKey, offset+sizeof(int));
				int secondRecordLength = 0;
				int lengthOfSecondKey = getKeyLength(attribute, pageData, offset+4);
				secondRecordLength = 4 + lengthOfSecondKey + 8;
				int lengthOfSecondKeyValue = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+4)-4:4;

				if(compareKey(attribute, firstKey, secondKey, EQ_OP)!=1){
					if(attribute.type==TypeVarChar){
					//	string pqr = string((char*)(secondKey));
						//cout<< endl;
						//cout << "The new key should be set to second key with value " << pqr;

						memcpy(newKey,&lengthOfSecondKeyValue,sizeof(int));
						memcpy((char *)newKey+sizeof(int),secondKey,lengthOfSecondKeyValue);
						int length = getKeyLength(attribute,newKey,0);
						//cout << "The length set in new key is " << length;
						void *valueOfNewKey = malloc(length-3);
						getKeyValue(attribute,newKey,valueOfNewKey,0);
					//	string abc = string((char*)(valueOfNewKey));
					//	cout << "The value of the new key is " << abc;
						free(valueOfNewKey);
					}else{
						memcpy(newKey,secondKey,4);
					}

					unsigned short newOffsetForCurrentPage = offset;

					void *newPage = malloc(PAGE_SIZE);
					unsigned short newPageOffset=freeSpaceOffset - offset;

					memcpy(newPage,(char*)pageData+offset,freeSpaceOffset-offset);
					int insertionKeyLength = (attribute.type==TypeVarChar)?getKeyLength(attribute,key,0)-3:4;
					void *valueOfInsertionKey = malloc(insertionKeyLength);
					getKeyValue(attribute,key,valueOfInsertionKey,0);
					//Compare the value of key to the newKey to find where to insert
					if(compareKey(attribute, valueOfInsertionKey, secondKey, LT_OP)==1){
						//Insert in pageData at apt position
						unsigned short internalOffset = 0;
						while(internalOffset<newOffsetForCurrentPage){
							int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,internalOffset+4)-3:4;
							void *keyToCompare = malloc(keySize);
							getKeyValue(attribute,pageData,keyToCompare,internalOffset+4);
							if(compareKey(attribute,valueOfInsertionKey,keyToCompare,GE_OP)==1){
								internalOffset += 4 + getKeyLength(attribute,pageData,internalOffset+4) + 8;
							}else{
								//Found the place
								void *temp = malloc(PAGE_SIZE);
								memcpy(temp,(char *)pageData+internalOffset,newOffsetForCurrentPage-internalOffset);
								int lengthOfKey = getKeyLength(attribute,key,0);
								int isDeleted = 0;
								memcpy((char *)pageData+internalOffset,&isDeleted,4);
								memcpy((char *)pageData+internalOffset+4,key,getKeyLength(attribute,key,0));
								memcpy((char *)pageData+internalOffset+4+getKeyLength(attribute,key,0),&rid.pageNum,4);
								memcpy((char *)pageData+internalOffset+4+getKeyLength(attribute,key,0)+4,&rid.slotNum,4);
								memcpy((char *)pageData+internalOffset+4+getKeyLength(attribute,key,0)+8,temp,newOffsetForCurrentPage-internalOffset);
								free(temp);
								free(keyToCompare);
								newOffsetForCurrentPage += 4+getKeyLength(attribute,key,0) + 8;
								break;
							}
							free(keyToCompare);
						}
						if(internalOffset==newOffsetForCurrentPage){
							//It means record needs to be inserted at last
							void *temp = malloc(PAGE_SIZE);
							memcpy(temp,(char *)pageData+internalOffset,newOffsetForCurrentPage-internalOffset);
							int lengthOfKey = getKeyLength(attribute,key,0);
							int isDeleted = 0;
							memcpy((char *)pageData+internalOffset,&isDeleted,4);
							memcpy((char *)pageData+internalOffset+4,key,lengthOfKey);
							memcpy((char *)pageData+internalOffset+4+lengthOfKey,&rid.pageNum,4);
							memcpy((char *)pageData+internalOffset+4+lengthOfKey+4,&rid.slotNum,4);
							memcpy((char *)pageData+internalOffset+4+lengthOfKey+8,temp,newOffsetForCurrentPage-internalOffset);
							free(temp);
							newOffsetForCurrentPage += 4+lengthOfKey + 8;
						}

					}else{
						//Insert in new Page at apt position
						unsigned short internalOffset = 0;
						while(internalOffset<newPageOffset){
							int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,newPage,internalOffset+4)-3:4;
							void *keyToCompare = malloc(keySize);
							getKeyValue(attribute,newPage,keyToCompare,internalOffset+4);
							if(compareKey(attribute,valueOfInsertionKey,keyToCompare,GE_OP)==1){
								internalOffset += 4+getKeyLength(attribute,newPage,internalOffset+4) + 8;
							}else{
								//Found the place
								void *temp = malloc(PAGE_SIZE);
								memcpy(temp,(char *)newPage+internalOffset,newPageOffset-internalOffset);
								int lengthOfKey = getKeyLength(attribute,key,0);
								int isDeleted = 0;
								memcpy((char *)newPage+internalOffset,&isDeleted,4);
								memcpy((char *)newPage+internalOffset+4,key,lengthOfKey);
								memcpy((char *)newPage+internalOffset+4+lengthOfKey,&rid.pageNum,4);
								memcpy((char *)newPage+internalOffset+4+lengthOfKey+4,&rid.slotNum,4);
								memcpy((char *)newPage+internalOffset+4+lengthOfKey+8,temp,newPageOffset-internalOffset);
								free(temp);
								free(keyToCompare);
								newPageOffset += 4+lengthOfKey + 8;
								break;
							}
							free(keyToCompare);
						}

						if(internalOffset==newPageOffset){
							//It means record needs to be inserted at last
							void *temp = malloc(PAGE_SIZE);
							memcpy(temp,(char *)newPage+internalOffset,newPageOffset-internalOffset);
							int lengthOfKey = getKeyLength(attribute,key,0);
							int isDeleted = 0;
							memcpy((char *)newPage+internalOffset,&isDeleted,4);
							memcpy((char *)newPage+internalOffset+4,key,lengthOfKey);
							memcpy((char *)newPage+internalOffset+4+lengthOfKey,&rid.pageNum,4);
							memcpy((char *)newPage+internalOffset+4+lengthOfKey+4,&rid.slotNum,4);
							memcpy((char *)newPage+internalOffset+4+lengthOfKey+8,temp,newPageOffset-internalOffset);
							free(temp);
							newPageOffset += 4+lengthOfKey + 8;
						}
					}

					memcpy((char *)newPage+PAGE_SIZE-2,&newPageOffset,sizeof(short));
					int nodeTypeOfNewPage = 1;
					memcpy((char *)newPage+PAGE_SIZE-6,&nodeTypeOfNewPage, sizeof(int));
					//Copying the next page number entry
					memcpy((char *)newPage+PAGE_SIZE-10,(char *)pageData+PAGE_SIZE-10,sizeof(int));

					ixfileHandle.fileHandle.appendPage(newPage);
					newPageNumber = ixfileHandle.fileHandle.getNumberOfPages() - 1;

					//Setting the new offset for the current Page
					memcpy((char *)pageData+PAGE_SIZE-2,&newOffsetForCurrentPage, sizeof(short));
					//Setting the new next page for the current Page
					memcpy((char *)pageData+PAGE_SIZE-10,&newPageNumber,sizeof(int));
					ixfileHandle.fileHandle.writePage(pageNumber,pageData);
				//	validateAllLeafNodes(ixfileHandle,attribute);

					free(valueOfInsertionKey);
					free(keyValue);
					free(pageData);
					free(newPage);
					free(firstKey);
					free(secondKey);
					return 0;
				}else{
					free(secondKey);
					free(firstKey);
				}
			}
		}
	}
	free(keyValue);
	free(pageData);
	return -1;
}

RC IndexManager::compareKey(Attribute attribute, const void *key, void *keyToBeComparedWith, CompOp compOp){
	if(attribute.type==TypeVarChar){
		return varcharCompare(string((char*)(key)),string((char*)(keyToBeComparedWith)),compOp);
	}else if(attribute.type==TypeInt){
		return intCompare(*(int*)key,*(int*)keyToBeComparedWith,compOp);
	}else if(attribute.type==TypeReal){
		return floatCompare(*(float*)key,*(float*)keyToBeComparedWith,compOp);
	}else{
		return -1;
	}
}

RC IndexManager::floatCompare(float &conditionAttributeValue,float &valueToBeComparedWith,CompOp compOp) {
	switch (compOp) {
	case LT_OP:
		return (conditionAttributeValue < valueToBeComparedWith);
	case GT_OP:
		return (conditionAttributeValue > valueToBeComparedWith);
	case EQ_OP:
		return (conditionAttributeValue == valueToBeComparedWith);
	case NE_OP:
		return (conditionAttributeValue != valueToBeComparedWith);
	case LE_OP:
		return (conditionAttributeValue <= valueToBeComparedWith);
	case GE_OP:
		return (conditionAttributeValue >= valueToBeComparedWith);
	case NO_OP:
		return 1;
	default:
		return 0;
	}
}

RC IndexManager::intCompare(int &conditionAttributeValue, int &valueToBeComparedWith, CompOp compOp) {
	switch (compOp) {
	case LT_OP:
		return (conditionAttributeValue < valueToBeComparedWith);
	case GT_OP:
		return (conditionAttributeValue > valueToBeComparedWith);
	case EQ_OP:
		return (conditionAttributeValue == valueToBeComparedWith);
	case NE_OP:
		return (conditionAttributeValue != valueToBeComparedWith);
	case LE_OP:
		return (conditionAttributeValue <= valueToBeComparedWith);
	case GE_OP:
		return (conditionAttributeValue >= valueToBeComparedWith);
	case NO_OP:
		return 1;
	default:
		return 0;
	}
}

RC IndexManager::varcharCompare(string conditionAttributeValue, string valueToBeComparedWith, CompOp compOp) {
	int cmp = strcmp(conditionAttributeValue.c_str(), valueToBeComparedWith.c_str());
	switch (compOp) {
	case LT_OP:
		return (cmp < 0);
	case GT_OP:
		return (cmp > 0);
	case EQ_OP:
		return cmp == 0;
	case NE_OP:
		return cmp != 0;
	case LE_OP:
		return (cmp <= 0);
	case GE_OP:
		return (cmp >= 0);
	case NO_OP:
		return 1;
	default: return 0;
	}
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int rootNode = 0;
	int pageNumber = findLeafPage(ixfileHandle,attribute,key,rootNode);
	if(pageNumber==-1){
		return -1;
	}

	void *pageData = malloc(PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(pageNumber,pageData);
	if(rc==-1){
		free(pageData);
		return -1;
	}


	unsigned short freeSpaceOffset;
	memcpy(&freeSpaceOffset,(char *)pageData+PAGE_SIZE-sizeof(short),sizeof(short));

	unsigned short offset = 0;

	while(offset<freeSpaceOffset){
		int length = getKeyLength(attribute, pageData, offset+4);

		int isDeleted;
		unsigned lengthOfEntry = 0;
		memcpy(&isDeleted,(char *)pageData+offset+lengthOfEntry,sizeof(int));
		lengthOfEntry += 4;
		lengthOfEntry += length;

		unsigned pageNum;
		memcpy(&pageNum,(char *)pageData+offset+lengthOfEntry,sizeof(unsigned));
		lengthOfEntry += 4;
		unsigned slotNum;
		memcpy(&slotNum,(char *)pageData+offset+lengthOfEntry,sizeof(unsigned));
		lengthOfEntry += 4;
		if(pageNum==rid.pageNum && slotNum==rid.slotNum && isDeleted==0){
			isDeleted = 1;
			memcpy((char *)pageData+offset,&isDeleted,sizeof(int));
			RC rc = ixfileHandle.fileHandle.writePage(pageNumber,pageData);
			if(rc==-1){
				free(pageData);
				return -1;
			}
			free(pageData);
			return 0;
		}else if(pageNum==rid.pageNum && slotNum==rid.slotNum && isDeleted==1){
			free(pageData);
			return -1;
		}else{
			offset += lengthOfEntry;
		}
	}
	//It means the entry is not found in the page
	free(pageData);
	return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if(lowKey==NULL){
		CompOp compOp = GT_OP;
		if(highKeyInclusive){
			compOp = GE_OP;
		}
		if(highKey==NULL){
			compOp = NO_OP;
		}
		void *pageData = malloc(PAGE_SIZE);

		int readPageNumber = findLeftMostLeafPage(ixfileHandle,attribute);

		if(readPageNumber==-1){
			free(pageData);
			return -1;
		}

		while(readPageNumber!=-1){

			RC rc = ixfileHandle.fileHandle.readPage(readPageNumber,pageData);
			if(rc==-1){
				free(pageData);
				return -1;
			}

			unsigned short freeSpaceOffset;
			memcpy(&freeSpaceOffset,(char *)pageData+PAGE_SIZE-sizeof(short),sizeof(short));

			int nextPageNumber;
			memcpy(&nextPageNumber,(char *)pageData+PAGE_SIZE-10,sizeof(int));

			unsigned short offset = 0;

			while(offset<freeSpaceOffset){
				int isDeleted = 0;
				memcpy(&isDeleted,(char *)pageData+offset,sizeof(int));
				if(isDeleted==1){
					offset += 4 + getKeyLength(attribute,pageData,offset+4) + 8;
					continue;
				}else{
					int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+4)-3:4;
					void *keyValue = malloc(keySize);
					getKeyValue(attribute, pageData, keyValue, offset+4);
					void *highKeyValue;
					if(highKey!=NULL){
						int highKeyLength = (attribute.type==TypeVarChar)?getKeyLength(attribute,highKey,0)-3:4;
						highKeyValue = malloc(highKeyLength);
						getKeyValue(attribute,highKey,highKeyValue,0);
					}
					if(highKey==NULL || compareKey(attribute, highKeyValue, keyValue, compOp)==1){
						if(attribute.type==TypeVarChar){
							ix_ScanIterator.varcharKeys.push_back(string((char*)(keyValue)));
							offset += 4 + getKeyLength(attribute,pageData,offset+4);
							RID rid;
							memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
							offset += sizeof(unsigned);
							memcpy(&rid.slotNum,(char *)pageData+offset, sizeof(unsigned));
							ix_ScanIterator.rids.push_back(rid);
							offset += sizeof(unsigned);
							if(highKey!=NULL){free(highKeyValue);}
							free(keyValue);
							continue;
						}else if(attribute.type==TypeInt){
							ix_ScanIterator.intKeys.push_back((*(int*)keyValue));
							offset += 4 +  getKeyLength(attribute,pageData,offset+4);
							RID rid;
							memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
							offset += sizeof(unsigned);
							memcpy(&rid.slotNum,(char *)pageData+offset, sizeof(unsigned));
							ix_ScanIterator.rids.push_back(rid);
							offset += sizeof(unsigned);
							if(highKey!=NULL){free(highKeyValue);}
							free(keyValue);
							continue;
						}else{
							ix_ScanIterator.floatKeys.push_back((*(float*)keyValue));
							offset += 4 +  getKeyLength(attribute,pageData,offset+4);
							RID rid;
							memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
							offset += sizeof(unsigned);
							memcpy(&rid.slotNum,(char *)pageData+offset, sizeof(unsigned));
							ix_ScanIterator.rids.push_back(rid);
							offset += sizeof(unsigned);
							if(highKey!=NULL){free(highKeyValue);}
							free(keyValue);
							continue;
						}
					}else{
						readPageNumber = -1;
						if(highKey!=NULL){free(highKeyValue);}
						free(keyValue);
						break;
					}

				}
			}
			readPageNumber = nextPageNumber;
		}
		ix_ScanIterator.ixfileHandle = &ixfileHandle;
		ix_ScanIterator.attribute = attribute;
		free(pageData);
		return 0;
	}else{
		//Find the page number where this key is and then parse from that page onwards
		CompOp compOpForLowKey = LT_OP;
		if(lowKeyInclusive){
			compOpForLowKey = LE_OP;
		}
		CompOp compOpForHighKey = GT_OP;
		if(highKeyInclusive){
			compOpForHighKey = GE_OP;
		}

		void *pageData = malloc(PAGE_SIZE);

		int rootPage = 0;
		int readPageNumber = findLeafPage(ixfileHandle, attribute, lowKey, rootPage);

		while(readPageNumber!=-1){

			RC rc = ixfileHandle.fileHandle.readPage(readPageNumber,pageData);
			if(rc==-1){
				free(pageData);
				return -1;
			}

			unsigned short freeSpaceOffset;
			memcpy(&freeSpaceOffset,(char *)pageData+PAGE_SIZE-sizeof(short),sizeof(short));

			int nextPageNumber;
			memcpy(&nextPageNumber,(char *)pageData+PAGE_SIZE-10,sizeof(int));

			unsigned short offset = 0;

			while(offset<freeSpaceOffset){
				int isDeleted = 0;
				int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+4)-3:4;
				void *keyValue = malloc(keySize);
				getKeyValue(attribute, pageData, keyValue, offset+4);
				memcpy(&isDeleted,(char *)pageData+offset,sizeof(int));
				if(isDeleted==1){
					offset += 4 + getKeyLength(attribute,pageData,offset+4) + 8;
					free(keyValue);
					continue;
				}else{
					void *lowKeyValue;
					void *highKeyValue;
					if(lowKey!=NULL){
						int lowKeyLength = (attribute.type==TypeVarChar)?getKeyLength(attribute,lowKey,0)-3:4;
						lowKeyValue = malloc(lowKeyLength);
						getKeyValue(attribute,lowKey,lowKeyValue,0);
					}
					if(highKey!=NULL){
						int highKeyLength = (attribute.type==TypeVarChar)?getKeyLength(attribute,highKey,0)-3:4;
						highKeyValue = malloc(highKeyLength);
						getKeyValue(attribute,highKey,highKeyValue,0);
					}

					if(compareKey(attribute, lowKeyValue, keyValue, compOpForLowKey)==1 && (highKey==NULL || compareKey(attribute, highKeyValue, keyValue, compOpForHighKey)==1)){
						if(attribute.type==TypeVarChar){
							ix_ScanIterator.varcharKeys.push_back(string((char*)(keyValue)));
							offset += 4 +  getKeyLength(attribute,pageData,offset+4);
							RID rid;
							memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
							offset += sizeof(unsigned);
							memcpy(&rid.slotNum,(char *)pageData+offset, sizeof(unsigned));
							ix_ScanIterator.rids.push_back(rid);
							offset += sizeof(unsigned);
							if(highKey!=NULL){free(highKeyValue);}
							if(lowKey!=NULL){free(lowKeyValue);}
							free(keyValue);
							continue;
						}else if(attribute.type==TypeInt){
							ix_ScanIterator.intKeys.push_back((*(int*)keyValue));
							offset += 4 +  getKeyLength(attribute,pageData,offset+4);
							RID rid;
							memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
							offset += sizeof(unsigned);
							memcpy(&rid.slotNum,(char *)pageData+offset, sizeof(unsigned));
							ix_ScanIterator.rids.push_back(rid);
							offset += sizeof(unsigned);
							if(highKey!=NULL){free(highKeyValue);}
							if(lowKey!=NULL){free(lowKeyValue);}
							free(keyValue);
							continue;
						}else{
							ix_ScanIterator.floatKeys.push_back((*(float*)keyValue));
							offset += 4 +  getKeyLength(attribute,pageData,offset+4);
							RID rid;
							memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
							offset += sizeof(unsigned);
							memcpy(&rid.slotNum,(char *)pageData+offset, sizeof(unsigned));
							ix_ScanIterator.rids.push_back(rid);
							offset += sizeof(unsigned);
							if(highKey!=NULL){free(highKeyValue);}
							if(lowKey!=NULL){free(lowKeyValue);}
							free(keyValue);
							continue;
						}
					}else{
						if((highKey != NULL) && (compareKey(attribute,highKeyValue,keyValue,LT_OP))){
							readPageNumber = -1;
							if(highKey!=NULL){free(highKeyValue);}
							if(lowKey!=NULL){free(lowKeyValue);}
							free(keyValue);
							break;
						}
						offset += 4 + getKeyLength(attribute,pageData,offset+4) + 8;
						if(highKey!=NULL){free(highKeyValue);}
						if(lowKey!=NULL){free(lowKeyValue);}
						free(keyValue);
					}
				}
			}
			readPageNumber = nextPageNumber;
		}
		ix_ScanIterator.ixfileHandle = &ixfileHandle;
		ix_ScanIterator.attribute = attribute;
		free(pageData);
		return 0;
	}
    return -1;
}

void IndexManager::validateNonLeafNode(IXFileHandle &ixfileHandle, Attribute attribute, unsigned &pageNum){
	//It will always start from the root node and check all the non leaf pages if they are valid
	if(pageNum>=ixfileHandle.fileHandle.getNumberOfPages()){
		cout<< "The pageNumber " << pageNum << " is not valid for this file" << endl;
		return;
	}else{
		void *pageData = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(pageNum,pageData);
		int nodeType;
		memcpy(&nodeType,(char *)pageData+PAGE_SIZE-6,sizeof(int));
		if(nodeType==0){
			//It means it is a non leaf node
			unsigned short freeSpaceOffset;
			memcpy(&freeSpaceOffset,(char *)pageData+PAGE_SIZE-sizeof(short),sizeof(short));
			unsigned short offset = 0;

			while(offset<freeSpaceOffset){
				unsigned firstPagePointer;
				memcpy(&firstPagePointer,(char *)pageData+offset,sizeof(unsigned));
				if(firstPagePointer>=ixfileHandle.fileHandle.getNumberOfPages()){
					cout<< "The first pageNumber " << firstPagePointer << " is not right on non leaf page with pageNumber "<< pageNum << endl;
					return;
				}
				offset += 4;
				if(offset>=freeSpaceOffset){
					return;
				}
				int key1Length = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
				void *key1 = malloc(key1Length);
				getKeyValue(attribute,pageData,key1,offset);
				offset += getKeyLength(attribute,pageData,offset);
				if(offset>=freeSpaceOffset){
					return;
				}

				unsigned page;
				memcpy(&page,(char *)pageData+offset,sizeof(unsigned));
				if(page>=ixfileHandle.fileHandle.getNumberOfPages()){
					cout << "The page number " << page << " is not correct in the non leaf page with pageNumber " <<pageNum << endl;
				}
				offset += sizeof(unsigned);
				if(offset>=freeSpaceOffset){
					return;
				}

				int key2Length = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
				void *key2 = malloc(key2Length);
				getKeyValue(attribute,pageData,key2,offset);
				offset += getKeyLength(attribute,pageData,offset);
				if(compareKey(attribute,key1,key2,LT_OP)){
					free(key1);
					free(key2);
					continue;
				}else{
					free(key1);
					free(key2);
					cout << "The key are not in correct order in non leaf page with pageNumber " << pageNum << endl;
					break;
				}
			}
		}
		free(pageData);
		return;
	}
}

int IndexManager::findLeftMostLeafPage(IXFileHandle &ixfileHandle, const Attribute attribute){
	//find the left most page
		void *pageData = malloc(PAGE_SIZE);
		RC rc = ixfileHandle.fileHandle.readPage(0,pageData);
		if(rc == -1){
			free(pageData);
			return -1;
		}

		int nodeType;
		memcpy(&nodeType,(char *)pageData+PAGE_SIZE-6,sizeof(int));

		unsigned page;
		memcpy(&page,(char*)pageData,sizeof(unsigned));

		free(pageData);
		while(nodeType!=1){
			//read first pointer
			void *nextPageData = malloc(PAGE_SIZE);
			RC rc = ixfileHandle.fileHandle.readPage(page,nextPageData);
			if(rc == -1){
				free(nextPageData);
				return -1;
			}
			int node;
			memcpy(&node,(char *)nextPageData+PAGE_SIZE-6,sizeof(int));
			if(node==1){
				free(nextPageData);
				break;
			}else{
				unsigned nextPage;
				memcpy(&nextPage,nextPageData,sizeof(unsigned));
				page = nextPage;
				free(nextPageData);
				continue;
			}

		}

		return page;
}

void IndexManager::validateAllLeafNodes(IXFileHandle &ixfileHandle, const Attribute &attribute){
	int currentPage = findLeftMostLeafPage(ixfileHandle,attribute);
	while(currentPage!=-1){
		void *leftMostPage = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(currentPage,leftMostPage);

		unsigned short freeSpaceOffset;
		memcpy(&freeSpaceOffset,(char *)leftMostPage+PAGE_SIZE-2,2);

		int siblingPage;
		memcpy(&siblingPage,(char *)leftMostPage+PAGE_SIZE-10,4);

		unsigned short offset=0;
		while(offset<freeSpaceOffset){
			int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,leftMostPage,offset+sizeof(int))-3:4;
			void *firstKey = malloc(keySize);
			getKeyValue(attribute, leftMostPage, firstKey, offset+sizeof(int));
			int firstRecordLength = 0;
			int lengthOfFirstKey = getKeyLength(attribute, leftMostPage, offset+4);
			firstRecordLength = 4 + lengthOfFirstKey + 8;
			offset += firstRecordLength;
			if(offset>=freeSpaceOffset){
				break;
			}
			keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,leftMostPage,offset+sizeof(int))-3:4;
			void *secondKey = malloc(keySize);
			getKeyValue(attribute, leftMostPage, secondKey, offset+sizeof(int));

			if(compareKey(attribute, firstKey, secondKey, LE_OP)==1){
				free(firstKey);
				free(secondKey);
				continue;
			}else{
				cout<< "The leaf node keys are not in correct order" << endl;
				free(firstKey);
				free(secondKey);
				return;
			}
		}

		if(siblingPage!=-1){
			free(leftMostPage);
			currentPage = siblingPage;
			continue;
		}else{
			free(leftMostPage);
			return;
		}
	}
}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	unsigned startPage = 0;
	recursivePrint(startPage,ixfileHandle,attribute,false);
}

void IndexManager::printNonLeafNodesOfBtree(IXFileHandle &ixfileHandle, const Attribute attribute) const {
	unsigned startPage = 0;
	bool onlyNonLeafNodes = true;
	recursivePrint(startPage,ixfileHandle,attribute, onlyNonLeafNodes);
}

void IndexManager::recursivePrint(unsigned &pageNumber, IXFileHandle &ixfileHandle, const Attribute attribute, bool onlyNonLeafNodes) const{
	if(pageNumber<ixfileHandle.fileHandle.totalPages){
		vector<int> intKeys;
			vector<float> floatKeys;
			vector<string> stringKeys;
			vector<unsigned> children;
			void *pageData = malloc(PAGE_SIZE);
			RC rc = ixfileHandle.fileHandle.readPage(pageNumber,pageData);
			if(rc==-1){
				free(pageData);
				return;
			}

			int nodeType;
			memcpy(&nodeType,(char *)pageData+PAGE_SIZE-6,sizeof(int));

			unsigned short freeSpaceOffset;
			memcpy(&freeSpaceOffset, (char *)pageData + PAGE_SIZE - sizeof(short), sizeof(short));

			if(nodeType==0){
				unsigned short offset = 0;
				unsigned child;
				if(offset<freeSpaceOffset){
					//First Child
					memcpy(&child,(char*)pageData,sizeof(unsigned));
					children.push_back(child);
					offset += sizeof(unsigned);
				}
				while(offset<freeSpaceOffset){
					int keySize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset)-3:4;
					void *key = malloc(keySize);
					getKeyValue(attribute, pageData, key, offset);

					if(attribute.type==TypeVarChar){
						stringKeys.push_back(string((char*)(key)));
					}else if(attribute.type==TypeReal){
						floatKeys.push_back(*(float*)key);
					}else if(attribute.type==TypeInt){
						intKeys.push_back(*(int*)key);
					}else{
						free(pageData);
						return;
					}

					unsigned keyLength = getKeyLength(attribute, pageData, offset);
					offset += keyLength;

					memcpy(&child,(char *)pageData+offset,sizeof(unsigned));
					children.push_back(child);
					offset += sizeof(unsigned);
					free(key);
				}
				cout << "{" << endl;
				cout << "\"keys\" :[" ;
				if(attribute.type==TypeVarChar){
					for(int i=0;i<stringKeys.size();i++){
						cout << "\"";
						cout << stringKeys[i];
						cout << "\"" ;
						if(i!=stringKeys.size()-1){
							cout << " , ";
						}
					}
				}else if(attribute.type==TypeReal){
					for(int i=0;i<floatKeys.size();i++){
						cout << "\"";
						cout << floatKeys[i];
						cout << "\"" ;
						if(i!=floatKeys.size()-1){
							cout << " , ";
						}
					}
				}else if(attribute.type==TypeInt){
					for(int i=0;i<intKeys.size();i++){
						cout << "\"";
						cout << intKeys[i];
						cout << "\"" ;
						if(i!=intKeys.size()-1){
							cout << " , ";
						}
					}
				}
				cout << " ]," << endl;

				cout << "\"children\" :[";
				for(unsigned i=0;i<children.size();i++){
					recursivePrint(children[i], ixfileHandle, attribute, onlyNonLeafNodes);
					if(i!=children.size()-1){
						cout << ",";
					}
					cout << endl;
				}
				cout << "]}";
			}else if(nodeType==1 && !onlyNonLeafNodes){
				//If it is a leaf node
				map<string,vector<RID>> stringMap;
				map<int,vector<RID>> intMap;
				map<float,vector<RID>> floatMap;
				unsigned short offset = 0;

				while(offset<freeSpaceOffset){
					int keyValueSize = (attribute.type==TypeVarChar)?getKeyLength(attribute,pageData,offset+4)-3:4;
					void *leafKey = malloc(keyValueSize);
					offset += 4;
					getKeyValue(attribute, pageData, leafKey, offset);

					unsigned keyLength = getKeyLength(attribute, pageData, offset);
					offset += keyLength;

					RID rid;
					memcpy(&rid.pageNum,(char *)pageData+offset,sizeof(unsigned));
					offset += sizeof(unsigned);
					memcpy(&rid.slotNum,(char *)pageData+offset,sizeof(unsigned));
					offset += sizeof(unsigned);

					if(attribute.type==TypeVarChar){
						std::map<string,vector<RID>>::iterator it = stringMap.find(string((char*)(leafKey)));
						if(it!=stringMap.end()){
							it->second.push_back(rid);
						}else{
							vector<RID> rids;
							rids.push_back(rid);
							stringMap.insert(pair <string,vector<RID>> (string((char*)(leafKey)),rids));
						}
					}else if(attribute.type==TypeReal){
						std::map<float,vector<RID>>::iterator it = floatMap.find((*(float*)leafKey));
						if(it!=floatMap.end()){
							it->second.push_back(rid);
						}else{
							vector<RID> rids;
							rids.push_back(rid);
							floatMap.insert(pair <float,vector<RID>> ((*(float*)leafKey),rids));
						}
					}else if(attribute.type==TypeInt){
						std::map<int,vector<RID>>::iterator it = intMap.find((*(int*)leafKey));
						if(it!=intMap.end()){
							it->second.push_back(rid);
						}else{
							vector<RID> rids;
							rids.push_back(rid);
							intMap.insert(pair <int,vector<RID>> ((*(int*)leafKey),rids));
						}
					}else{
						return;
					}

					free(leafKey);
				}

				if(attribute.type==TypeVarChar){
					std::map<string,vector<RID>>::iterator mitVar = stringMap.begin();
					int j =0;
					cout << "{\"keys\" : [";
					for(;mitVar!=stringMap.end();mitVar++){
						string key = mitVar->first;
						vector<RID> rids = mitVar->second;
						cout << "\"" << key << ": [";
						for(int i=0;i<rids.size();i++){
							cout<<"(" << rids[i].pageNum << "," << rids[i].slotNum << ")" ;
							if(i!=rids.size()-1){
								cout << ",";
							}
						}
						cout << "]\"";
						if(j!=stringMap.size()-1){
							cout << ",";
						}
						j++;
					}
					cout << "]}";
				}else if(attribute.type==TypeReal){
					std::map<float,vector<RID>>::iterator mitFloat = floatMap.begin();
					int j =0;
					cout << "{\"keys\" : [";
					for(;mitFloat!=floatMap.end();mitFloat++){
						float key = mitFloat->first;
						vector<RID> rids = mitFloat->second;
						cout << "\"" << key << ": [";
						for(int i=0;i<rids.size();i++){
							cout<<"(" << rids[i].pageNum << "," << rids[i].slotNum << ")" ;
							if(i!=rids.size()-1){
								cout << ",";
							}
						}
						cout << "]\"";
						if(j!=floatMap.size()-1){
							cout << ",";
						}
						j++;
					}
					cout << "]}";
				}else if(attribute.type==TypeInt){
					std::map<int,vector<RID>>::iterator mitInt = intMap.begin();
					int j =0;
					cout << "{\"keys\" : [";
					for(;mitInt!=intMap.end();mitInt++){
						int key = mitInt->first;
						vector<RID> rids = mitInt->second;
						cout << "\"" << key << ": [";
						for(int i=0;i<rids.size();i++){
							cout<<"(" << rids[i].pageNum << "," << rids[i].slotNum << ")" ;
							if(i!=rids.size()-1){
								cout << ",";
							}
						}
						cout << "]\"";
						if(j!=intMap.size()-1){
							cout << ",";
						}
						j++;
					}
					cout << "]}";
				}
			}
			free(pageData);
			return;
	}
	return;
}


RC IndexManager::getKeyValue(Attribute attribute, const void *pageData, void *keyValue, unsigned short startOffset) const{
	if(attribute.type==TypeVarChar){
		int lengthOfVarChar;
		memcpy(&lengthOfVarChar, (char *)pageData + startOffset, sizeof(int));
		memcpy(keyValue, (char *)pageData + startOffset + sizeof(int), lengthOfVarChar);
		char endChar = '\0';
		memcpy((char *)keyValue+lengthOfVarChar, &endChar, 1);

	}else{
		memcpy(keyValue, (char *)pageData + startOffset, sizeof(int));
	}
	return 0;
}

unsigned IndexManager::getKeyLength(Attribute attribute, const void *pageData, unsigned short startOffset) const{
	unsigned length = 0;
	if(attribute.type==TypeVarChar){
		memcpy(&length, (char *)pageData + startOffset, sizeof(int));
		length += sizeof(int);
	}else{
		length  = sizeof(int);
	}
	return length;
}

IX_ScanIterator::IX_ScanIterator()
{
	position = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(rids.size()==0 || position>=rids.size()){
		position = 0;
		return -1;
	}
//	RID possibleRid = rids[position];
//	void *keyValue = malloc(PAGE_SIZE);
	if(attribute.type==TypeVarChar){
		int lengthOfString = varcharKeys[position].length();
		memcpy((char *)key, &lengthOfString, 4);
		memcpy((char *)key+sizeof(int),varcharKeys[position].c_str(),lengthOfString);
	}else if(attribute.type==TypeInt){
		memcpy(key,&intKeys[position],sizeof(int));
	}else{
		memcpy(key,&floatKeys[position],sizeof(float));
	}
	rid = rids[position];
	position++;
	return 0;
}

RC IX_ScanIterator::close()
{
	floatKeys.clear();
	intKeys.clear();
	varcharKeys.clear();
	rids.clear();
	position = 0;
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	RC rc = fileHandle.collectCounterValues(ixReadPageCounter,ixWritePageCounter,ixAppendPageCounter);
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
	return rc;
}

