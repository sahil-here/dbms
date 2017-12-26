#include "rbfm.h"


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if(!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return PagedFileManager::instance()->openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	//Calculating the length of the data to be stored as a record using record descriptor


	//cout<<"Total Pages BEFORE : " << fileHandle.getNumberOfPages() << endl;//TODO: DEBUG
	unsigned short size = calculateSizeOfRecord(recordDescriptor,data);
	unsigned short slotNumber = 0;
	PageNum pageNumber = 0;
	//Findina a page where this record can be stored
	fileHandle.findFreePage(size, slotNumber, pageNumber);
	void *record = malloc(size);
	prepareRecord(recordDescriptor, data, record);
	//Inserting the record in the page found above
	fileHandle.insertRecordInPage(pageNumber, record, size, slotNumber);
	rid.pageNum = pageNumber;
	rid.slotNum = slotNumber;
	free(record);

//	cout<<"Total Pages AFTER : " <<  fileHandle.getNumberOfPages() << endl;//TODO: DEBUG
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	unsigned short recordLength=0;
	//Reads the record from given page number and slot number into data
	void * record = malloc(calculateMaxSizeOfRecord(recordDescriptor));
	if(fileHandle.readRecordFromPage(rid.pageNum,rid.slotNum,record,recordLength)==-1){
		free(record);
		return -1;
	}
	transformRecord(recordDescriptor,record,data);
	free(record);
	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	unsigned short recordLength;
	void * record = malloc(calculateMaxSizeOfRecord(recordDescriptor));
	if(fileHandle.readRecordFromPage(rid.pageNum,rid.slotNum,record,recordLength)==-1){
		free(record);
		return -1;
	}
	int sizeOfNullBitArray = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator,(char *)record+sizeof(int)+sizeof(short), sizeOfNullBitArray);
	int i=0;
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
		if(it->name==attributeName){
			bitset<8> nullByte = 00000000;
			if(nullsIndicator[i/8] & (1 << (7-(i%8)))){
				nullByte = 10000000;
			}
			memcpy((char *) data, &nullByte, 1);
			unsigned short beginOffset;
			unsigned short endOffset;

			//i=0 What happens to endOffset?

			//if i=recordDescriptor.size()-1 - it is at the third offset - end offset : GOES TO THE DATA PART
			memcpy(&endOffset, (char *)record+sizeof(int)+sizeof(short)+sizeOfNullBitArray+(sizeof(short)*i), sizeof(short));
			if(i==0){
				beginOffset = sizeof(int) + sizeof(short) + sizeOfNullBitArray + sizeof(short)*recordDescriptor.size();
			}else{
				memcpy(&beginOffset, (char *)record+sizeof(int)+sizeof(short)+sizeOfNullBitArray+(sizeof(short)*(i-1)), sizeof(short));
			}
			if(it->type==TypeVarChar){
				unsigned length = endOffset-beginOffset;
				memcpy((char *) data + 1, &length , sizeof(int));
				memcpy((char *) data + 1 + sizeof(int), (char *)record + beginOffset , endOffset-beginOffset);
			}else{
				int var;
				memcpy(&var,(char *)record+beginOffset, endOffset-beginOffset);
				memcpy((char *) data + 1, (char *)record + beginOffset , sizeof(int));
			}
			break;
		}
		i++;
	}
	free(nullsIndicator);
	free(record);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	//Finding the size needed for null bit array
	int sizeOfNullBitArray = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator, data, sizeOfNullBitArray);
	bool nullBit = false;
	int i=0;
	int offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
		nullBit = nullsIndicator[i/8] & (1 << (7-(i%8)));
		std::cout << it->name << ": ";
		if(!nullBit){
			if(it->type == TypeVarChar){
				int varLength;
				memcpy(&varLength, (char *)data+offset, sizeof(int));
				offset += sizeof(int);
				for (int i = 0; i < varLength; i++){
					std::cout << *reinterpret_cast<const char *>((char *)data + i + offset);
				}
				std::cout << "\n";
				offset += varLength;
			}else if(it->type == TypeInt){
				int var;
				memcpy(&var,(char *)data + offset, sizeof(int));
				std::cout << var << endl;
				offset += sizeof(int);
			}else if(it->type == TypeReal){
				float var;
				memcpy(&var,(char *)data + offset, sizeof(float));
				std::cout << var << endl;
				offset += sizeof(float);
			}
		}else{
			std::cout << "NULL" << "\n";
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

unsigned short RecordBasedFileManager::calculateSizeOfRecord(const vector<Attribute> &recordDescriptor, const void *data) {

	unsigned size = sizeof(int) + 2; // For storing schema version and number of attributes in a record

	unsigned sizeOfNullBitArray = ceil((double) recordDescriptor.size() / CHAR_BIT); // Size for Null Bit Array

	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	memcpy(nullsIndicator, data, sizeOfNullBitArray);
	bool nullBit = false;
	int i=0;
	int offset = sizeOfNullBitArray;
	int numberOfVarCharAttributes = 0;
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
		nullBit = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!nullBit){
			if(it->type == TypeVarChar){
				int varLength = 0;
				memcpy(&varLength, (char *)data+offset, sizeof(int));
				offset += sizeof(int);
				offset += varLength;
				numberOfVarCharAttributes++;
			}else{
				offset += sizeof(int);
			}
		}
		i++;
	}
	free(nullsIndicator);
	size += offset; //Size of the record data
	size += recordDescriptor.size() * sizeof(short); //To store the offsets of attribute values
	size -= numberOfVarCharAttributes * sizeof(int); //We will not need the space to store length of varchar
	return size;
}

RC RecordBasedFileManager::prepareRecord(const vector<Attribute> &recordDescriptor, const void *data, void *preparedRecord){
	int schemaVersion = 0; //For pointers, it will be <0 and for records it will be >=0
	memcpy((char *)preparedRecord ,&schemaVersion,sizeof(int));
	//First we are storing the number of attributes in the record in first 2 bytes
	short numberOfAttributes = recordDescriptor.size();
	memcpy((char *)preparedRecord + sizeof(int),&numberOfAttributes,sizeof(short));
	int sizeOfNullBitArray = ceil((double) numberOfAttributes / CHAR_BIT); // Size for Null Bit Array
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	memcpy(nullsIndicator, data, sizeOfNullBitArray);
	memcpy((char *)preparedRecord + sizeof(int) + sizeof(short), data, sizeOfNullBitArray);
	bool nullBit = false;
	int i=0;
	int offsetToParseData = sizeOfNullBitArray;
	short offsetForPreparedRecord = sizeof(int) + sizeof(short) + sizeOfNullBitArray + (numberOfAttributes*sizeof(short));
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
		nullBit = nullsIndicator[i/8] & (1 << (7-(i%8)));
		unsigned locationWhereOffsetValueWillBeStored =  sizeof(int) + sizeof(short) + sizeOfNullBitArray + (i*sizeof(short));
		if(!nullBit){
			if(it->type == TypeVarChar){
				int varLength = 0;
				memcpy(&varLength, (char *)data+offsetToParseData, sizeof(int));
				offsetToParseData += sizeof(int);
				unsigned short valueTobeStoredInTheOffsetLocation = offsetForPreparedRecord + varLength;
				memcpy((char *)preparedRecord + locationWhereOffsetValueWillBeStored, &valueTobeStoredInTheOffsetLocation, sizeof(short));
				memcpy((char *)preparedRecord + offsetForPreparedRecord, (char *)data+offsetToParseData, varLength);
				offsetForPreparedRecord += varLength;
				offsetToParseData += varLength;
			}else if(it->type == TypeInt){
				unsigned short valueTobeStoredInTheOffsetLocation = offsetForPreparedRecord + sizeof(int);
				memcpy((char *)preparedRecord + locationWhereOffsetValueWillBeStored, &valueTobeStoredInTheOffsetLocation, sizeof(short));
				memcpy((char *)preparedRecord + offsetForPreparedRecord, (char *)data+offsetToParseData, sizeof(int));
				offsetForPreparedRecord += sizeof(int);
				offsetToParseData += sizeof(int);
			}else if(it->type == TypeReal){
				unsigned short valueTobeStoredInTheOffsetLocation = offsetForPreparedRecord + sizeof(float);
				memcpy((char *)preparedRecord + locationWhereOffsetValueWillBeStored, &valueTobeStoredInTheOffsetLocation, sizeof(short));
				memcpy((char *)preparedRecord + offsetForPreparedRecord, (char *)data+offsetToParseData, sizeof(float));
				offsetForPreparedRecord += sizeof(float);
				offsetToParseData += sizeof(float);
			}else{
				free(nullsIndicator);
				return -1;
			}
		}else{
			unsigned short valueTobeStoredInTheOffsetLocation = offsetForPreparedRecord;
			memcpy((char *)preparedRecord + locationWhereOffsetValueWillBeStored, &valueTobeStoredInTheOffsetLocation, sizeof(short));
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}



RC RecordBasedFileManager::transformRecord(const vector<Attribute> &recordDescriptor, const void *dataFromFile, void * tranformedRecord){
	int numberOfAttributes = recordDescriptor.size();
	int sizeOfNullBitArray = ceil((double) numberOfAttributes / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	memcpy(nullsIndicator,(char *) dataFromFile+sizeof(int)+sizeof(short), sizeOfNullBitArray);
	memcpy(tranformedRecord, (char *)dataFromFile+sizeof(int)+sizeof(short), sizeOfNullBitArray);
	int i=0;
	bool nullBit = false;
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
		nullBit = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!nullBit){
			unsigned short beginOffset;
			unsigned short endOffset;
			memcpy(&endOffset, (char *)dataFromFile+sizeof(int)+sizeof(short)+sizeOfNullBitArray+(sizeof(short)*i), sizeof(short));
			if(i==0){
				beginOffset = sizeof(int) + sizeof(short) + sizeOfNullBitArray + sizeof(short)*numberOfAttributes;
			}else{
				memcpy(&beginOffset, (char *)dataFromFile+sizeof(int)+sizeof(short)+sizeOfNullBitArray+(sizeof(short)*(i-1)), sizeof(short));
			}
			if(it->type == TypeVarChar){
				//allocate memory for length of varchar
				unsigned int length = endOffset-beginOffset;
				memcpy((char *) tranformedRecord + offset , &length, sizeof(int));
				offset += sizeof(int);
				//set the valve of varchar
				memcpy((char *) tranformedRecord + offset , (char *)dataFromFile + beginOffset , length);
				offset += length;
			}else if(it->type == TypeInt){
				memcpy((char *) tranformedRecord + offset , (char *)dataFromFile + beginOffset, sizeof(int));
				offset += sizeof(int);
			}else if(it->type == TypeReal){
				memcpy((char *) tranformedRecord + offset , (char *)dataFromFile + beginOffset, sizeof(float));
				offset += sizeof(float);
			}else{
				free(nullsIndicator);
				return -1;
			}
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

unsigned int RecordBasedFileManager::calculateMaxSizeOfRecord(const vector<Attribute> &recordDescriptor){
	unsigned int size = sizeof(int) + sizeof(short); //For Schema version and Number of Attributes
	size += ceil((double) recordDescriptor.size() / CHAR_BIT); //Size of null bit array
	size += sizeof(short)*recordDescriptor.size(); //Size needed to store offsets
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
		size += it->length;
	}
	return size;
}

// Delete a record identified by the given rid.
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	if(fileHandle.deleteRecordFromPage(rid.pageNum, rid.slotNum)!=0){
		return -1;
	}
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
	//Calculating the length of the data to be stored as a record using record descriptor
	unsigned short size = calculateSizeOfRecord(recordDescriptor,data);
	void *record = malloc(size);
	prepareRecord(recordDescriptor, data, record);
	fileHandle.updateRecord(rid.pageNum, rid.slotNum, size, record);
	free(record);
	return 0;
}

//void prepareRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
/*RC RBFM_ScanIterator:: getNextRecord(RID &rid, void *data){
	if(rids.size()==0 || position>=rids.size()){
			position = 0;
			return -1;
		}
		rid = rids[position];
		void *completeRecord = malloc(RecordBasedFileManager::instance()->calculateMaxSizeOfRecord(recordDescriptor));
		RecordBasedFileManager::instance()->readRecord(*fileHandle, recordDescriptor, rid, completeRecord);
		RecordBasedFileManager::instance()->printRecord(recordDescriptor,completeRecord);
		vector<Field> fields;
		int sizeOfNullBitArray = ceil((double) recordDescriptor.size() / CHAR_BIT);
		unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
		//Getting the null bit array
		memcpy(nullsIndicator, completeRecord, sizeOfNullBitArray);
		bool nullBit = false;
		int i=0;
		int offset = sizeOfNullBitArray;
		for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
			Field field;
			nullBit = nullsIndicator[i/8] & (1 << (7-(i%8)));
			field.isNull = nullBit;
			field.type = it->type;
			field.attributeName = it->name;
			if(!nullBit){
				if(it->type == TypeVarChar){
					int varLength;
					memcpy(&varLength, (char *)completeRecord+offset, sizeof(int));
					field.length = varLength;
					offset += sizeof(int);
					void *value = malloc(varLength);
					memcpy(value,(char *)completeRecord+offset,varLength);
					offset += varLength;
					field.varcharValue = string((char*)(value));
					fields.push_back(field);
					free(value);
				}else if(it->type == TypeInt){
					int var;
					memcpy(&var,(char *)completeRecord + offset, sizeof(int));
					field.intValue = var;
					fields.push_back(field);
					offset += sizeof(int);
				}else if(it->type == TypeReal){
					float var;
					memcpy(&var,(char *)completeRecord + offset, sizeof(float));
					field.floatValue = var;
					fields.push_back(field);
					offset += sizeof(float);
				}
			}else{
				fields.push_back(field);
			}
			i++;
		}
		free(nullsIndicator);
		free(completeRecord);
		unsigned char *nullsIndicator2 = (unsigned char *) malloc(sizeOfNullBitArray);
		memset(nullsIndicator2,0,sizeOfNullBitArray);
		unsigned resultOffset = sizeOfNullBitArray;
		for(int i=0;i<attributeNames.size();i++){
			for(int j=0;j<fields.size();j++){
				if(strcmp(attributeNames[i].c_str(),fields[j].attributeName.c_str())==0){
					if (!fields[j].isNull) {
						nullsIndicator2[i/8] &= ~(1 << (7 - i%8));
					} else {
						nullsIndicator2[i/8] |=  (1 << (7 - i%8));
					}
					if(fields[j].type==TypeVarChar){
						memcpy((char *)data+resultOffset,&fields[j].length,sizeof(int));
						resultOffset += sizeof(int);
						memcpy((char *)data+resultOffset,&fields[j].varcharValue, fields[j].length);
						resultOffset += fields[j].length;
					}else if(fields[j].type==TypeInt){
						memcpy((char *)data+resultOffset,&fields[j].intValue,sizeof(int));
						resultOffset += sizeof(int);
					}else{
						memcpy((char *)data+resultOffset,&fields[j].floatValue,sizeof(float));
						resultOffset += sizeof(float);
					}
				}
			}
		}
		memcpy(data,nullsIndicator2,sizeOfNullBitArray);
		free(nullsIndicator2);
		fields.clear();
		position++;
		return 0;
}*/

/*RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute,		//
		const CompOp compOp,                  // comparison type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {

	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.fileHandle = &fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	AttrType attrTypeOfConditionAttribute;

	for(int k=0;k<recordDescriptor.size();k++){
		if(strcmp(recordDescriptor[k].name.c_str(),conditionAttribute.c_str())==0){
			attrTypeOfConditionAttribute = recordDescriptor[k].type;
		}
	}

	for(int j = 0;j<fileHandle.getNumberOfPages();j++) {
		void *readPageData = malloc(PAGE_SIZE);

		if(fileHandle.readPage(j,readPageData)!=0){
			free(readPageData);
			return -1;
		}

		//Finding the value of number of slots in this page
		unsigned short totalSlotCount;
		memcpy(&totalSlotCount, (char *)readPageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));

		// Iterate through the records to find a record that suits the criteria
		for(int i=0;i<=totalSlotCount;i++) {

			// Get the value stored in the condition attribute(column)
			void *conditionAttributeData = malloc(100);
			// Read the value of that attribute

			RID ridToRead;
			ridToRead.pageNum = i;
			ridToRead.slotNum = j;
			int rc = readAttribute(fileHandle,recordDescriptor,ridToRead,conditionAttribute,conditionAttributeData);
			if(rc!=0){
				free(conditionAttributeData);
				continue;
			}

			// Indicates if the condition has been met
			int conditionMet = 0;

			unsigned char *nullsIndicatorByte = (unsigned char *) malloc(1);
			memcpy(nullsIndicatorByte,(char *)conditionAttributeData, 1);
			bool nullBit = nullsIndicatorByte[0] & (1 << 7);
			free(nullsIndicatorByte);
			if(value==NULL && compOp==NO_OP){
				conditionMet = 1;
			}else if(value==NULL && compOp==EQ_OP){
				if(nullBit){
					conditionMet = 1;
				}else{
					conditionMet = 0;
				}
			}else if(value!=NULL && !nullBit){
				if(attrTypeOfConditionAttribute==TypeInt) {
						int currentAttributeValue = 0;
						memcpy(&currentAttributeValue,(char *)conditionAttributeData+1,sizeof(int));
						int valueToBeComparedWith = *(int*)value;
						conditionMet = rbfm_ScanIterator.intCompare(currentAttributeValue,valueToBeComparedWith, compOp);
					}
					if(attrTypeOfConditionAttribute==TypeReal) {
						float currentAttributeValue = 0;
						memcpy(&currentAttributeValue,(char *)conditionAttributeData+1,sizeof(float));
						float valueToBeComparedWith = *(float*)value;
						conditionMet = rbfm_ScanIterator.floatCompare(currentAttributeValue,valueToBeComparedWith, compOp);
					}
					if(attrTypeOfConditionAttribute==TypeVarChar) {
						int strLength=0;
						conditionAttributeData = (char *)conditionAttributeData+1;
						memcpy(&strLength,conditionAttributeData,sizeof(int));
						conditionAttributeData = (char *)conditionAttributeData+4;
						string val = string((char*)conditionAttributeData, strLength);
						string valueToBeComparedWith = string((char*)(value));
						//string valueToBeComparedWith = *(static_cast<std::string*>(value));
						conditionMet = rbfm_ScanIterator.varcharCompare(val,valueToBeComparedWith, compOp);
					}
			}else{
				conditionMet = 0;
			}

			if (conditionMet == 1) {
				rbfm_ScanIterator.rids.push_back(ridToRead);
			}
			free(conditionAttributeData);
		}
		free(readPageData);
	}


	return 0;
}*/

RC RBFM_ScanIterator:: getNextRecord(RID &rid, void *data) {

	// Get the current page to scan
	int pageNumber = currentPageCounter;
	// Get the current slot to start from
	int slotNumber = currentSlotCounter;

	int totalPages = fileHandle->totalPages;

	for(int j = pageNumber;j<totalPages;j++) {
		//Allocate memory of 4096 to read the page data
		void *readPageData = malloc(PAGE_SIZE);

		//ReadPage();  RC readPage(PageNum pageNum, void *data);
		if(fileHandle->readPage(pageNumber,readPageData)!=0){
			free(readPageData);
			return -1;
		}

		//Finding the value of number of slots in this page
		unsigned short totalSlotCount;
		memcpy(&totalSlotCount, (char *)readPageData + (PAGE_SIZE - (sizeof(short) * 2)), sizeof(short));

		// Generate RIDs to iterate through
		//RID rid;
		rid.pageNum = j;
		// Iterate through the records to find a record that suits the criteria
		for(int i=slotNumber;i<=totalSlotCount;i++) {

			// Set the slot number/record number
			rid.slotNum = i;

			// Get the object of RecordBasedFileManager

			// Get the value stored in the condition attribute(column)
			void *conditionAttributeData = malloc(100);
			// Read the value of that attribute

			int rc = RecordBasedFileManager::instance()->readAttribute(*fileHandle,recordDescriptor,rid,conditionAttribute,conditionAttributeData);


			if(rc!=0){
				free(conditionAttributeData);
				continue;
			}

			// Indicates if the condition has been met
			int conditionMet = 0;

			unsigned char *nullsIndicatorByte = (unsigned char *) malloc(1);
			memcpy(nullsIndicatorByte,(char *)conditionAttributeData, 1);
			bool nullBit = nullsIndicatorByte[0] & (1 << 7);
			free(nullsIndicatorByte);
			if(value==NULL && compOp==NO_OP){
				conditionMet = 1;
			}else if(value==NULL && compOp==EQ_OP){
				if(nullBit){
					conditionMet = 1;
				}else{
					conditionMet = 0;
				}
			}else if(value!=NULL && !nullBit){
				if(attrType==TypeInt) {
						int currentAttributeValue = 0;
						memcpy(&currentAttributeValue,(char *)conditionAttributeData+1,sizeof(int));
						int valueToBeComparedWith = *(int*)value;
						conditionMet = intCompare(currentAttributeValue,valueToBeComparedWith, compOp);
					}
					if(attrType==TypeReal) {
						float currentAttributeValue = 0;
						memcpy(&currentAttributeValue,(char *)conditionAttributeData+1,sizeof(float));
						float valueToBeComparedWith = *(float*)value;
						conditionMet =floatCompare(currentAttributeValue,valueToBeComparedWith, compOp);
					}
					if(attrType==TypeVarChar) {
						int strLength=0;
						conditionAttributeData = (char *)conditionAttributeData+1;
						memcpy(&strLength,conditionAttributeData,sizeof(int));
						conditionAttributeData = (char *)conditionAttributeData+4;
						string val = string((char*)conditionAttributeData, strLength);


						int lengthOfValue=0;
						memcpy(&lengthOfValue,(char *)value,sizeof(int));
				/*		char* charArray = new char[lengthOfValue+1];
						for(int i=0;i<lengthOfValue;i++){
							char ch;
							memcpy(&ch,(char *)value+4+i,1);
							charArray[i] = ch;
						}
						charArray[lengthOfValue] = '\0'; */

						void *varcharValue = malloc(lengthOfValue+1);
						memcpy(varcharValue,(char *)value+4,lengthOfValue);
						char endChar = '\0';
						memcpy((char*)varcharValue+lengthOfValue,&endChar,1);
					//	string* valueToBeComparedWith = static_cast<std::string*>(varcharValue);
						//TODO: Ask TAs for help here. Following is working when called from getAttribute() but not working when called from test_p0
						string abc = string((char *)varcharValue);
					//	cout << "The string inside value is " << abc << endl;
						conditionMet = varcharCompare(val, abc, compOp);
					//	delete[] charArray;
						free(varcharValue);
					}
			}else{
				conditionMet = 0;
			}

			if (conditionMet == 1) {
				// We have found a record which has the data which we care about

				// Update the current slot value to the next one so that the next scan operation starts from the next record
				if (i != totalSlotCount) {
					// If we are not in the last slot, update the slot count
					currentSlotCounter = i+1;
				} else {
					//TODO: Add a check to make sure we are not in the last page
					// Goto the next page
				//	unsigned t = RecordBasedFileManager::instance()->
					if(currentPageCounter != (fileHandle->getNumberOfPages())-1) {
						currentPageCounter++;
						// Reset to the first record in the new page
						currentSlotCounter = 1;
					}else{
						currentSlotCounter++;
					}
				}

				int sizeOfNullBitArray = ceil((double) attributeNames.size() / 8);
				unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
				memset(nullsIndicator,0,sizeOfNullBitArray);
				int offset = sizeOfNullBitArray;

				for(int i=0;i<attributeNames.size();i++) {
					void *eachColumnData = malloc(100);
					RecordBasedFileManager::instance()->readAttribute(*fileHandle,recordDescriptor, rid, attributeNames[i], eachColumnData);
					char firstByte;
					memcpy(&firstByte,(char *)eachColumnData,1);
					bool mask = (firstByte>>7)&1;
					if (!mask) {
						nullsIndicator[i/8] &= ~(1 << (7 - i%8));
					} else {
						nullsIndicator[i/8] |=  (1 << (7 - i%8));
					}
					if(recordDescriptor[requiredAttributeIndex[i]].type==TypeVarChar) {
						int length;
						memcpy(&length,(char *)eachColumnData+1,sizeof(int));
						memcpy((char *)data+offset,&length,sizeof(int));
						offset += sizeof(int);
						memcpy((char *)data+offset,(char *)eachColumnData+1+sizeof(int),length);
						offset += length;
					}else if(recordDescriptor[requiredAttributeIndex[i]].type==TypeInt){
						int var;
						memcpy(&var,(char *)eachColumnData+1,sizeof(int));
						memcpy((char *)data+offset,(char *)eachColumnData+1,sizeof(int));
						offset += sizeof(int);
					}else{
						float var;
						memcpy(&var,(char *)eachColumnData+1,sizeof(float));
						memcpy((char *)data+offset,(char *)eachColumnData+1,sizeof(float));
						offset += sizeof(int);
					}
					free(eachColumnData);
				}
				memcpy((char *)data,nullsIndicator,sizeOfNullBitArray);
				free(nullsIndicator);
				free(readPageData);
				return 0;
			}else{
				if (i != totalSlotCount) {
					// If we are not in the last slot, update the slot count
					currentSlotCounter = i+1;
				} else {
					//TODO: Add a check to make sure we are not in the last page
					// Goto the next page
				//	unsigned t = RecordBasedFileManager::instance()->
					if(currentPageCounter != (fileHandle->getNumberOfPages())-1) {
						currentPageCounter++;
						// Reset to the first record in the new page
						currentSlotCounter = 1;
					}else{
						currentSlotCounter++;
					}
				}
			}
		}
		free(readPageData);
	}
	currentPageCounter = 0;
	currentSlotCounter = 1;

	RecordBasedFileManager::instance()->closeFile(*fileHandle);
	return RBFM_EOF;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute,		//
		const CompOp compOp,                  // comparison type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {


	// Pass all the parameters to RBFM ScanIterator as that does the bulk of the work

	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = (void *)value;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.fileHandle = &fileHandle;

	rbfm_ScanIterator.attributeNames = attributeNames;
	// Perform any common calculations across all scan iterations
//	cout << "CURRENT PAGE COUNTER under Scan " << rbfm_ScanIterator.currentPageCounter <<endl; //TODO: DEBUG
	// This includes
	// a) What is the column number of condition attribute
	// b) What else ?


	int rdSize = recordDescriptor.size();
	for(int i=0;i<rdSize;i++)
	{
		//string name = recordDescriptor[i].name;
		if(strcmp(recordDescriptor[i].name.c_str(),conditionAttribute.c_str())==0) {
			rbfm_ScanIterator.whichColumn = i;
			rbfm_ScanIterator.attrType = recordDescriptor[i].type;
			rbfm_ScanIterator.attrLength = recordDescriptor[i].length;

		}
	}

	for (short i = 0; i < recordDescriptor.size(); ++i) {
		for (short j = 0; j < attributeNames.size(); ++j) {
			if (attributeNames[j] == recordDescriptor[i].name) {
				rbfm_ScanIterator.requiredAttributeIndex.push_back(i);
				rbfm_ScanIterator.requiredColumnLength+=recordDescriptor[i].length;
				break;
			}
		}
	}
//	cout << "CURRENT PAGE COUNTER under Scan " << rbfm_ScanIterator.currentPageCounter <<endl; //TODO: DEBUG

	return 0;
}



//RC RBFM_ScanIterator::Compare(auto conditionAttributeValue,auto actualValue,CompOp compOp) {
//    switch (compOp) {
//        case EQ_OP:
//        	return !(conditionAttributeValue == actualValue);
//        case LT_OP:
//        	return (conditionAttributeValue <= actualValue);
//        case GT_OP:
//        	return (conditionAttributeValue >= actualValue);
//        case LE_OP:
//        	return (conditionAttributeValue <  actualValue);
//        case GE_OP:
//        	return (conditionAttributeValue >  actualValue);
//        case NE_OP:
//        	return !(conditionAttributeValue != actualValue);
//        default:
//        	return 0;
//    }
//}
RC RBFM_ScanIterator::floatCompare(float conditionAttributeValue,float valueToBeComparedWith,CompOp compOp) {
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

RC RBFM_ScanIterator::intCompare(int conditionAttributeValue, int valueToBeComparedWith, CompOp compOp) {
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

RC RBFM_ScanIterator::varcharCompare(string conditionAttributeValue, string valueToBeComparedWith, CompOp compOp) {

//	cout << "Comparing " << conditionAttributeValue << " to " << valueToBeComparedWith << endl;
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
