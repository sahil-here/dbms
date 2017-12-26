
#include "rm.h"


RelationManager* RelationManager::instance()
{
	static RelationManager _rm;
	return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

// Calculate actual bytes for nulls-indicator for the given field counts
int getByteForNullsIndicator(int fieldCount) {

	return ceil((double) fieldCount / CHAR_BIT);
}

RC RelationManager::prepareTableRecord(int fieldCount, unsigned char *nullFieldsIndicator,const int Table_ID, const int nameLength, const string &name, const int fileNamelength, const string &fileName, const int tablePermission, void *buffer, int *recordSize)
{
	int offset = 0;

	// Null-indicators
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = getByteForNullsIndicator(fieldCount);

	// Null-indicator for the fields
	memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;

	// Is the table_ID field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 7);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &Table_ID, sizeof(int));
		offset += sizeof(int);
	}

	// Is the tableName field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 6);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &nameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, name.c_str(), nameLength);
		offset += nameLength;
	}

	// Is the tableFileName field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 5);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &fileNamelength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, fileName.c_str(), fileNamelength);
		offset += fileNamelength;
	}

	// Is the tablePermission field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 4);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &tablePermission, sizeof(float));
		offset += sizeof(float);
	}

	*recordSize = offset;

	return 0;
}

RC RelationManager::prepareIndexRecord(int fieldCount,  unsigned char *nullFieldsIndicator, const string &tableName, const string &indexName, const string &attributeName, void *buffer, int *recordSize){
	int offset = 0;

		// Null-indicators
		bool nullBit = false;
		int nullFieldsIndicatorActualSize = getByteForNullsIndicator(fieldCount);

		// Null-indicator for the fields
		memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
		offset += nullFieldsIndicatorActualSize;

		// Beginning of the actual data
		// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
		// e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

		// Is the tableName field not-NULL?
		nullBit = nullFieldsIndicator[0] & (1 << 7);
		if (!nullBit) {
			int length = tableName.length();
			memcpy((char *)buffer + offset, &length, sizeof(int));
			offset += sizeof(int);
			memcpy((char *)buffer + offset, tableName.c_str(), tableName.length());
			offset += tableName.length();
		}

		// Is the indexName field not-NULL?
		nullBit = nullFieldsIndicator[0] & (1 << 6);

		if (!nullBit) {
			int length = indexName.length();
			memcpy((char *)buffer + offset, &length, sizeof(int));
			offset += sizeof(int);
			memcpy((char *)buffer + offset, indexName.c_str(), indexName.length());
			offset += indexName.length();
		}

		// Is the attributeName field not-NULL?
		nullBit = nullFieldsIndicator[0] & (1 << 5);
		if (!nullBit) {
			int length = attributeName.length();
			memcpy((char *)buffer + offset, &length, sizeof(int));
			offset += sizeof(int);
			memcpy((char *)buffer + offset, attributeName.c_str(), attributeName.length());
			offset += attributeName.length();
		}

		*recordSize = offset;

		return 0;
}


//prepareTableRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,1,8,"table-id","TypeInt",sizeof(int),1,1);

RC RelationManager::prepareColumnRecord(int fieldCount, unsigned char *nullFieldsIndicator,const int Table_ID,const int nameLength, const string &name,
		int attrType,const int colLength,const int colPosition,const int colVersion,void *buffer, int *recordSize ) {

	int offset = 0;

	// Null-indicators
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = getByteForNullsIndicator(fieldCount);

	// Null-indicator for the fields
	memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;

	// Beginning of the actual data
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

	// Is the table_ID field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 7);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &Table_ID, sizeof(int));
		offset += sizeof(int);
	}

	// Is the tableName field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 6);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &nameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, name.c_str(), nameLength);
		offset += nameLength;
	}

	// Is the type field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 5);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &attrType, sizeof(int));
		offset += sizeof(float);
	}

	// Is the columnLength field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 4);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &colLength, sizeof(int));
		offset += sizeof(float);
	}

	// Is the columnPosition field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 3);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &colPosition, sizeof(int));
		offset += sizeof(float);
	}

	// Is the columnVersion field not-NULL?
	nullBit = nullFieldsIndicator[0] & (1 << 2);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &colVersion, sizeof(int));
		offset += sizeof(float);
	}

	*recordSize = offset;

	return 0;

}


//
//RC RelationManager::maxTable() {
//
//	fstream fileCreates; //object of fstream class
//
//	if(std::fstream("MAXTABLEID")) {
//			  return -1;
//		  }
//
//		   //opening file "sample.txt" in out(write) mode
//	fileCreates.open("MAXTABLEID",ios::out|ios::binary);
//
//    if(!fileCreates.is_open()) return -1;
//
//	struct stat st;
//	if(stat("MAXTABLEID", &st) != 0) {
//		// File does not exist
//		return -1;
//	}
//	if (st.st_size == 0) {
//
//		int a[1] = {0};
//		a[0]=0;
//		fileCreates.seekp(0); //SEEKP for WRITE
//		fileCreates.write((char *)&a,PAGE_SIZE);
//		flush(fileCreates);
//		fileCreates.seekp(0); //HERE
//
//	}
//	fileCreates.close();
//	return 0;
//}


RC RelationManager::createCatalog()
{
	//Two files names table.tbl and Column.clm should be created.
	string temp = "table.tbl";
	bool a = RecordBasedFileManager::instance()->createFile(temp);

	FileHandle myTableHandle;
	//RC openFile(const string &fileName, FileHandle &fileHandle);
	int rc = RecordBasedFileManager::instance()->openFile(temp,myTableHandle);
	if (rc != 0) {
				return -1;
			}

	//cout<< "TOTAL PAGES create catalogue "<<myTableHandle.getNumberOfPages() <<endl;

	vector<Attribute> tableRecordDescriptor = getTableMetaData();

	void *tableMeta = malloc(100);
	void *returnedData = malloc(100);
	int recordSize = 0;
	// Initialize a NULL field indicator
	int nullFieldsIndicatorActualSize = getByteForNullsIndicator(tableRecordDescriptor.size());
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);


	RID rid;
	prepareTableRecord(tableRecordDescriptor.size(),nullsIndicator,1,5,"table",9,"table.tbl",1,tableMeta,&recordSize);

//	cout<< "TOTAL PAGES Create catalogue Table "<<myTableHandle.getNumberOfPages() <<endl;


	RecordBasedFileManager::instance()->insertRecord(myTableHandle,tableRecordDescriptor,tableMeta,rid);

	prepareTableRecord(tableRecordDescriptor.size(),nullsIndicator,2,6,"column",10,"column.clm",1,tableMeta,&recordSize);

	RecordBasedFileManager::instance()->insertRecord(myTableHandle,tableRecordDescriptor,tableMeta,rid);

	prepareTableRecord(tableRecordDescriptor.size(),nullsIndicator,3,5,"index",5,"index",1,tableMeta,&recordSize);

	RecordBasedFileManager::instance()->insertRecord(myTableHandle,tableRecordDescriptor,tableMeta,rid);

	rc = RecordBasedFileManager::instance()->closeFile(myTableHandle);
	if (rc != 0) {
			return -1;
		}

	// Column File
	string temp1 = "column.clm";
	bool b = RecordBasedFileManager::instance()->createFile(temp1);

	FileHandle myColumnHandle;
	//RC openFile(const string &fileName, FileHandle &fileHandle);
	RecordBasedFileManager::instance()->openFile(temp1,myColumnHandle);

	vector<Attribute> columnRecordDescriptor = getColumnMetaData();
	void *columnMeta = malloc(200);
	recordSize=0;

	int nullFieldsIndicatorActualSizeColumn = getByteForNullsIndicator(columnRecordDescriptor.size());
	unsigned char *nullsIndicatorColumn = (unsigned char *) malloc(nullFieldsIndicatorActualSizeColumn);
	memset(nullsIndicatorColumn, 0, nullFieldsIndicatorActualSizeColumn);

	RID rid1;
	////// 1
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,1,8,"table-id",0,4,1,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	////////// 2
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,1,10,"table-name",2,50,2,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	/////// 3
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,1,9,"file-name",2,50,3,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	/////// 4
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,2,8,"table-id",0,4,1,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	///// 5
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,2,11,"column-name",2,50,2,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	////////// 6
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,2,11,"column-type",0,4,3,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	///////// 7
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,2,13,"column-length",0,4,4,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);
	// print DEBUG
	//	RecordBasedFileManager::instance()->printRecord(columnRecordDescriptor,columnMeta);

	///////// 8
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,2,15,"column-position",0,4,5,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	//For Index Catalog table
	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,3,10,"table-name",2,50,1,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,3,10,"index-name",2,50,2,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,3,14,"attribute-name",2,50,3,1,columnMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);

	rc = RecordBasedFileManager::instance()->createFile("index");
	if(rc==-1){
		return -1;
	}

	RecordBasedFileManager::instance()->closeFile(myColumnHandle);
	 free(nullsIndicator);
	 free(tableMeta);
	 free(returnedData);
	 free(columnMeta);
	 free(nullsIndicatorColumn);
	 return (a&b);
}

vector<Attribute> RelationManager::getIndexMetaData(){
	Attribute indexAttribute;
	vector<Attribute> indexRecordDescriptor;

	indexAttribute.name = "table-name";
	indexAttribute.length = (AttrLength)50;
	indexAttribute.type = TypeVarChar;
	indexRecordDescriptor.push_back(indexAttribute);

	indexAttribute.name = "index-name";
	indexAttribute.length = (AttrLength)50;
	indexAttribute.type = TypeVarChar;
	indexRecordDescriptor.push_back(indexAttribute);

	indexAttribute.name = "attribute-name";
	indexAttribute.length = (AttrLength)50;
	indexAttribute.type = TypeVarChar;
	indexRecordDescriptor.push_back(indexAttribute);

	return indexRecordDescriptor;

}


vector<Attribute> RelationManager::getColumnMetaData() {
	Attribute columnAttribute;
	vector<Attribute> columnRecordDescriptor;

	//Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int,column-version:int)
	columnAttribute.name = "table-id";
	columnAttribute.length = sizeof(int);
	columnAttribute.type = TypeInt ;
	columnRecordDescriptor.push_back(columnAttribute);

	columnAttribute.name = "column-name";
	columnAttribute.length = (AttrLength)50;
	columnAttribute.type = TypeVarChar;
	columnRecordDescriptor.push_back(columnAttribute);

	columnAttribute.name = "column-type";
	columnAttribute.length = sizeof(int);
	columnAttribute.type = TypeInt ;
	columnRecordDescriptor.push_back(columnAttribute);

	columnAttribute.name = "column-length";
	columnAttribute.length = sizeof(int);
	columnAttribute.type = TypeInt ;
	columnRecordDescriptor.push_back(columnAttribute);

	columnAttribute.name = "column-position";
	columnAttribute.length = sizeof(int);
	columnAttribute.type = TypeInt ;
	columnRecordDescriptor.push_back(columnAttribute);

	/*columnAttribute.name = "column-version";
	columnAttribute.length = sizeof(int);
	columnAttribute.type = TypeInt ;
	columnRecordDescriptor.push_back(columnAttribute);*/

	return columnRecordDescriptor;
}

vector<Attribute> RelationManager::getTableMetaData()
{
	//Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
	Attribute tableAttribute;
	vector<Attribute> tableRecordDescriptor;

	tableAttribute.name = "table-id";
	tableAttribute.length = sizeof(int);
	tableAttribute.type = TypeInt ;
	tableRecordDescriptor.push_back(tableAttribute);

	tableAttribute.name = "table-name";
	tableAttribute.length = (AttrLength)50;
	tableAttribute.type = TypeVarChar;
	tableRecordDescriptor.push_back(tableAttribute);

	tableAttribute.name = "file-name";
	tableAttribute.length = (AttrLength)50;
	tableAttribute.type = TypeVarChar;
	tableRecordDescriptor.push_back(tableAttribute);

	/*tableAttribute.name = "table-permission";
	tableAttribute.length = sizeof(int);
	tableAttribute.type = TypeInt ;
	tableRecordDescriptor.push_back(tableAttribute);*/

	return tableRecordDescriptor;

}

RC RelationManager::deleteCatalog()
{
	//First find and delete all indexes
	vector<string> indexNames;
	vector<RID> indexRids;
	RC rc = getAllIndexForATable("",indexNames,indexRids);
	if(rc !=0) return -1;

	for(int i=0;i<indexNames.size();i++){
		RecordBasedFileManager::instance()->destroyFile(indexNames[i]);
	}


	FileHandle myFileHandle;
	rc = RecordBasedFileManager::instance()->openFile("table.tbl",myFileHandle);
	if(rc != 0)
	{
		return -1;
	}
	RBFM_ScanIterator rbsiTable;
	const vector<string> tableAttrs ({"table-name"});

	rc = RecordBasedFileManager::instance()->scan(myFileHandle, getTableMetaData(), "", NO_OP, NULL, tableAttrs, rbsiTable);
	if(rc != 0) {
			RecordBasedFileManager::instance()->closeFile(myFileHandle);
			rbsiTable.close();
			return -1;
	}

	vector<string> tablesToBeDeleted;
	RID rid;
	void *returnedData = malloc(PAGE_SIZE);
	memset(returnedData,0,PAGE_SIZE);
	while(rbsiTable.getNextRecord(rid, returnedData) != RM_EOF) {

		int strLength=0;
		memcpy(&strLength,(char *)returnedData+1,sizeof(int));
		string val = string((char*)returnedData+1+4, strLength);
		tablesToBeDeleted.push_back(val);
	}
	free(returnedData);
	rbsiTable.close();

	for(int i=tablesToBeDeleted.size()-1;i>=0;i--){
		if(strcmp(tablesToBeDeleted[i].c_str(),"table")==0){
			rc = RecordBasedFileManager::instance()->destroyFile("table.tbl");
		}else if(strcmp(tablesToBeDeleted[i].c_str(),"column")==0){
			rc = RecordBasedFileManager::instance()->destroyFile("column.clm");
		}else if(strcmp(tablesToBeDeleted[i].c_str(),"index")==0){
			rc = RecordBasedFileManager::instance()->destroyFile("index");
		}else{
			rc = deleteTable(tablesToBeDeleted[i]);
		}
		if(rc == -1){
			tablesToBeDeleted.clear();
			return -1;
		}
	}
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RID rid;
	int tableID = getMaxTableId()+1;

	int rc;
	FileHandle myTableHandle;
	rc = RecordBasedFileManager::instance()->openFile("table.tbl",myTableHandle);
	if (rc != 0) {
			return -1;
		}

	vector<Attribute> tableRecordDescriptor = getTableMetaData();

	void *tableMeta = malloc(100);
//	void *returnedData = malloc(100);
	int recordSize = 0;
	// Initialize a NULL field indicator
	int nullFieldsIndicatorActualSize = getByteForNullsIndicator(tableRecordDescriptor.size());
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);


	prepareTableRecord(tableRecordDescriptor.size(),nullsIndicator,tableID,tableName.length(),tableName,tableName.length(),tableName,2,tableMeta,&recordSize);
	//printTuple(tableRecordDescriptor, tableMeta);
	RecordBasedFileManager::instance()->insertRecord(myTableHandle,tableRecordDescriptor,tableMeta,rid);

	free(nullsIndicator);
	free(tableMeta);
//	rc  = setMaxTableId(tableID);
//	if( rc != 0) {
//		return -1;
//	}

	rc = RecordBasedFileManager::instance()->closeFile(myTableHandle);	//Create a new file with tableName as tableName
	if (rc != 0) {
			return -1;
		}



	string temp1 = "column.clm";
	FileHandle myColumnHandle;
	rc = RecordBasedFileManager::instance()->openFile(temp1,myColumnHandle);
	if (rc != 0) {
			return -1;
		}

	vector<Attribute> columnRecordDescriptor = getColumnMetaData();
	void *columnMeta = malloc(100);

	int nullFieldsIndicatorActualSizeColumn = getByteForNullsIndicator(columnRecordDescriptor.size());
	unsigned char *nullsIndicatorColumn = (unsigned char *) malloc(nullFieldsIndicatorActualSizeColumn);
	memset(nullsIndicatorColumn, 0, nullFieldsIndicatorActualSizeColumn);

	for(int i=0;i<attrs.size();i++) {
		RID rid1;
		int recordSize1 = 0;
		Attribute attr = attrs.at(i);
		prepareColumnRecord(columnRecordDescriptor.size(),nullsIndicatorColumn,tableID,attr.name.length(),attr.name,attr.type,attr.length,i+1,1,columnMeta,&recordSize1);
		RecordBasedFileManager::instance()->insertRecord(myColumnHandle,columnRecordDescriptor,columnMeta,rid1);
	}

	free(nullsIndicatorColumn);
	free(columnMeta);

	rc = RecordBasedFileManager::instance()->closeFile(myColumnHandle);
	if (rc != 0) {
			return -1;
		}

	//RC openFile(const string &fileName, FileHandle &fileHandle);
	rc = RecordBasedFileManager::instance()->createFile(tableName);
	if (rc != 0) {
		return -1;
	}

	//TODO: FIND THE RETURN CODE FOR CREATE|CLOSE|OPEN FILES


	return 0;
}

int RelationManager::getMaxTableId(){
    FileHandle myFileHandle;
        int rc = RecordBasedFileManager::instance()->openFile("table.tbl",myFileHandle);
        if(rc != 0)
        {
            return -1;
        }
        RBFM_ScanIterator rbsiTable;
        const vector<string> tableAttrs ({"table-id"});

        vector<int> tableIds;

        RecordBasedFileManager::instance()->scan(myFileHandle, getTableMetaData(), "", NO_OP, NULL, tableAttrs, rbsiTable);

            RID rid;
            void *returnedData = malloc(PAGE_SIZE);
            memset(returnedData,0,PAGE_SIZE);
            while(rbsiTable.getNextRecord(rid, returnedData) != RM_EOF) {
                int tableId;
                memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
                tableIds.push_back(tableId);
            }

            free(returnedData);
            RecordBasedFileManager::instance()->closeFile(myFileHandle);
            rbsiTable.close();

            int maxTableId = 0;
            for(int i=0;i<tableIds.size();i++){
                if(tableIds[i]>maxTableId){
                    maxTableId=tableIds[i];
                }
            }

            return maxTableId;
}


//int RelationManager::getTableID() {
//
//	 fstream fs;
//	 fs.open("MAXTABLEID",ios::in|ios::binary |ios::out);
//
//	 	if(!fs.is_open()) {
//
//	 		return -1;
//	 	}
//	 	else {
//	 		int *temp=new int[1];
//	 		int maxtableid=0;
//	 		fs.seekg(0); //SEEKG for READ
//	 			    		//myFile.read ((char*)y, sizeof (Data) * 10);
//	 		fs.read((char*)temp,sizeof(int));
//	 		maxtableid = temp[0];
//	 		 cout<< "MAX TABLE ID FROM FILE MAXTABLEID "<<temp<<endl;
//	 		 fs.close();
//	 		 return maxtableid;
//
//	 	}
//
//
//
//	// return 0;
//
//}
//RC RelationManager::setMaxTableId(int tableID) {
//
//	fstream fs;
//	fs.open ("MAXTABLEID",ios::in | std::ios::out | std::ios::app);
//	fs.seekp(0); //SEEKP for WRITE
//	int a[1] = {0};
//	a[0]= {tableID };
//	fs.write((char *)&a,sizeof(int));
//	flush(fs);
//	fs.close();
//
//	return 0;
//}

RC RelationManager::deleteTable(const string &tableName)
{
	// This should
	// a) Update the catalog (Update tuple?) to reflect that the table has been deleted
	// b) Delete the table file (PFM layer delete file)
	//

	if(!strcmp(tableName.c_str(),"Tables")) {
			return -1;
		}	else
			if(!strcmp(tableName.c_str(),"Columns")) {
				return -1;
			}

	vector<Attribute> attributes;
	getAttributes(tableName,attributes);

	FileHandle myFileHandle;
	void *returnedDataFromTable = malloc(PAGE_SIZE);
	int rc = RecordBasedFileManager::instance()->openFile("table.tbl",myFileHandle);
	if(rc != 0)
	{
		return -1;
	}
	vector<RID> ridsToBeDeletedInTables;
	RBFM_ScanIterator rbsiTable;
	const vector<string> tableAttrs ({"table-id"});

	void *value = malloc(tableName.length()+4);
	int length = tableName.length();
	memcpy(value,&length,sizeof(int));
	memcpy((char *)value+4,tableName.c_str(),tableName.length());

	rc = RecordBasedFileManager::instance()->scan(myFileHandle, getTableMetaData(), "table-name", EQ_OP, value, tableAttrs, rbsiTable);
	if(rc != 0) {
			RecordBasedFileManager::instance()->closeFile(myFileHandle);
			rbsiTable.close();
			free(returnedDataFromTable);
			return -1;
	}

	RID tableRID;
	while(rbsiTable.getNextRecord(tableRID, returnedDataFromTable) != RM_EOF) {
		ridsToBeDeletedInTables.push_back(tableRID);
	}
	rbsiTable.close();

	if(ridsToBeDeletedInTables.size()==0){
		RecordBasedFileManager::instance()->closeFile(myFileHandle);
		free(returnedDataFromTable);
		return 0;
	}

//	 for (const RID &value : rids) {
//	        _rbfm->deleteRecord(fh, columnsColumns(), value);
//	    }
	//RecordBasedFileManager::instance()->deleteRecord(myFileHandle, getTableMetaData(),*it);

	for(std::vector<RID>::iterator it = ridsToBeDeletedInTables.begin(); it != ridsToBeDeletedInTables.end(); it++) {
		RecordBasedFileManager::instance()->deleteRecord(myFileHandle, getTableMetaData(),*it);
	}

	//We have just 4 fields in the TableFile, so returnedData will have max 1 byte as nullbit vector.
	int tableId;
	memcpy(&tableId, (char *)returnedDataFromTable + 1, sizeof(int));
	free(returnedDataFromTable);
	RecordBasedFileManager::instance()->closeFile(myFileHandle);

	void* returnedData = malloc(PAGE_SIZE);
	vector<RID> ridsToBeDeletedInColumns;
	RBFM_ScanIterator rbsiColumn;
	FileHandle columnFileHandle;
	const vector<string> colAttributes ({"column-name", "column-type", "column-length"});
	RecordBasedFileManager::instance()->openFile("column.clm", columnFileHandle);

	rc =  RecordBasedFileManager::instance()->scan(columnFileHandle, getColumnMetaData(), "table-id", EQ_OP, (void *)&tableId, colAttributes, rbsiColumn);
	if(rc != 0) {
			RecordBasedFileManager::instance()->closeFile(columnFileHandle);
			rbsiColumn.close();
			free(returnedData);
			return -1;
	}

	RID columnRID;
	while(rbsiColumn.getNextRecord(columnRID, returnedData) != RM_EOF) {
		ridsToBeDeletedInColumns.push_back(columnRID);
	}
	rbsiColumn.close();

	for(std::vector<RID>::iterator it = ridsToBeDeletedInColumns.begin(); it != ridsToBeDeletedInColumns.end(); ++it) {
		rc = RecordBasedFileManager::instance()->deleteRecord(columnFileHandle,getColumnMetaData(),*it);
		if(rc!=0){
			RecordBasedFileManager::instance()->closeFile(columnFileHandle);
			rbsiColumn.close();
			free(returnedData);
			return -1;
		}
	}

	free(returnedData);
	RecordBasedFileManager::instance()->closeFile(columnFileHandle);

	string fileName = tableName.c_str();
	if(strcmp(fileName.c_str(),"table")==0){
		fileName = "table.tbl";
	}else if(strcmp(fileName.c_str(),"column")==0){
		fileName = "column.clm";
	}

	RecordBasedFileManager::instance()->destroyFile(fileName);

	vector<string> indexNames;
	vector<RID> indexRids;
	rc = getAllIndexForATable(tableName, indexNames, indexRids);
	if(rc !=0) {
		return -1;
	}

	FileHandle fileHandle;
	rc = RecordBasedFileManager::instance()->openFile("index",fileHandle);
	if(rc !=0){
		return -1;
	}

	for(int i=0;i<indexRids.size();i++){
		rc = RecordBasedFileManager::instance()->deleteRecord(fileHandle,attributes,indexRids[i]);
		if(rc !=0) return -1;
	}

	rc = RecordBasedFileManager::instance()->closeFile(fileHandle);
	if(rc !=0) return -1;

	return 0;
}

RC RelationManager::printCatalog(){
	FileHandle myFileHandle;
		int rc = RecordBasedFileManager::instance()->openFile("table.tbl",myFileHandle);
		if(rc != 0)
		{
			return -1;
		}
		RBFM_ScanIterator rbsiTable;
		const vector<string> tableAttrs ({"table-id"});

		RecordBasedFileManager::instance()->scan(myFileHandle, getTableMetaData(), "table-name", NO_OP, NULL, tableAttrs, rbsiTable);

		RID tableRID;
		void *returnedData = malloc(PAGE_SIZE);
		while(rbsiTable.getNextRecord(tableRID, returnedData) != RM_EOF) {
			void *data = malloc(100);
			RecordBasedFileManager::instance()->readRecord(myFileHandle,getTableMetaData(),tableRID,data);
			RecordBasedFileManager::instance()->printRecord(getTableMetaData(),data);
			free(data);
			}
		rbsiTable.close();
		RecordBasedFileManager::instance()->closeFile(myFileHandle);



		FileHandle myColumnFile;
				int rc1 = RecordBasedFileManager::instance()->openFile("column.clm",myColumnFile);
				if(rc1 != 0)
				{
					return -1;
				}
				RBFM_ScanIterator rbsiColumn;
				const vector<string> columnAttrs ({"column-name"});

				RecordBasedFileManager::instance()->scan(myColumnFile, getColumnMetaData(), "column-name", NO_OP, NULL, columnAttrs, rbsiColumn);

				RID columnRID;
				void *returnedData1 = malloc(PAGE_SIZE);
				while(rbsiColumn.getNextRecord(columnRID, returnedData1) != RM_EOF) {
					void *data = malloc(100);
					RecordBasedFileManager::instance()->readRecord(myColumnFile,getColumnMetaData(),columnRID,data);
					RecordBasedFileManager::instance()->printRecord(getColumnMetaData(),data);
					free(data);
					}
				rbsiColumn.close();

				RecordBasedFileManager::instance()->closeFile(myColumnFile);
				free(returnedData);
				free(returnedData1);

	return 0;
}

/*RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	// This should
	// a) Call scan on the tables file to get the table ID for the given table name
	// b) Call scan on the columns file to get all the rows for the given table ID

	FileHandle myFileHandle;
	int rc = RecordBasedFileManager::instance()->openFile("table.tbl",myFileHandle);
	if(rc != 0)
	{
		return -1;
	}
	RBFM_ScanIterator rbsiTable;
	const vector<string> tableAttrs ({"table-id"});
//
	//	scan(FileHandle &fileHandle,
//
//			const vector<Attribute> &recordDescriptor,
//			const string &conditionAttribute,		//
//			const CompOp compOp,                  // comparison type such as "<" and "="
//			const void *value,                    // used in the comparison
//			const vector<string> &attributeNames, // a list of projected attributes
//			RBFM_ScanIterator &rbfm_ScanIterator)

	RecordBasedFileManager::instance()->scan(myFileHandle, getTableMetaData(), "table-name", EQ_OP,  tableName.c_str(), tableAttrs, rbsiTable);

	 	RID rid;
	    void *returnedData = malloc(PAGE_SIZE);
	    int tableId;
	 /*   if(rbsiTable.getNextRecord(rid, returnedData) == RM_EOF) {
	    	RecordBasedFileManager::instance()->closeFile(myFileHandle);
	    	rbsiTable.close();
	        free(returnedData);
	        return -1;
	    }*/

	/*    while(rbsiTable.getNextRecord(rid, returnedData) != RM_EOF){
	    	memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
	    }

//	    RecordBasedFileManager::instance()->closeFile(myFileHandle);
	    rbsiTable.close();
	    memset(returnedData,0,PAGE_SIZE);

	    //We have just 4 fields in the TableFile, so returnedData will have max 1 byte as nullbit vector.
	//    int tableId;
	//    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));

	    RBFM_ScanIterator rbsiColumn;
	    FileHandle columnFileHandle;
	    const vector<string> colAttributes ({"column-name", "column-type", "column-length"});
	    RecordBasedFileManager::instance()->openFile("column.clm", columnFileHandle);


	    rc =  RecordBasedFileManager::instance()->scan(columnFileHandle, getColumnMetaData(), "table-id", EQ_OP, (void *)&tableId, colAttributes, rbsiColumn);
	    if(rc != 0) {
	        	RecordBasedFileManager::instance()->closeFile(columnFileHandle);
	        	rbsiColumn.close();
	            free(returnedData);
	            return -1;
	    }

	    while(rbsiColumn.getNextRecord(rid, returnedData) != RM_EOF) {
	           int offset = 1;

	           int len;
	           memcpy(&len, (char *)returnedData + offset, sizeof(int));
	           offset += sizeof(int);

	           string tmp = string((char*)returnedData+offset, len);
	           offset += len;

	           int type;
	           memcpy(&type, (char *)returnedData + offset, sizeof(int));
	           offset += sizeof(int);

	           int length;
	           memcpy(&length, (char *)returnedData + offset, sizeof(int));

	           Attribute attr;
	           attr.name = tmp;
	           attr.type = (AttrType)type;
	           attr.length = (AttrLength)length;
	           attrs.push_back(attr);
	       }

	       rbsiColumn.close();
	      // RecordBasedFileManager::instance()->closeFile(columnFileHandle);

	       free(returnedData);


	return 0;
}*/

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	// This should
	// a) Call scan on the tables file to get the table ID for the given table name
	// b) Call scan on the columns file to get all the rows for the given table ID
	string copying = "";
		copying = tableName.c_str();
		if(!strcmp(tableName.c_str(),"Tables")) {
			copying ="";
			copying = "table";
		}else
			if(!strcmp(tableName.c_str(),"Columns")) {
				copying ="";
				copying = "column";
			}


	FileHandle myFileHandle;
	int rc = RecordBasedFileManager::instance()->openFile("table.tbl",myFileHandle);
	if(rc != 0)
	{
		return -1;
	}
	RBFM_ScanIterator rbsiTable;
	const vector<string> tableAttrs ({"table-id"});
//
	//	scan(FileHandle &fileHandle,
//
//			const vector<Attribute> &recordDescriptor,
//			const string &conditionAttribute,		//
//			const CompOp compOp,                  // comparison type such as "<" and "="
//			const void *value,                    // used in the comparison
//			const vector<string> &attributeNames, // a list of projected attributes
//			RBFM_ScanIterator &rbfm_ScanIterator)

	void *value = malloc(copying.length()+4);
	int length = copying.length();
	memcpy(value,&length,4);
	memcpy((char *)value+4,copying.c_str(),length);


	RecordBasedFileManager::instance()->scan(myFileHandle, getTableMetaData(), "table-name", EQ_OP, value, tableAttrs, rbsiTable);

	 	RID rid;
	    void *returnedData = malloc(PAGE_SIZE);
	    memset(returnedData,0,PAGE_SIZE);
	    while(rbsiTable.getNextRecord(rid, returnedData) != RM_EOF) {
	    }

	    RecordBasedFileManager::instance()->closeFile(myFileHandle);
	    rbsiTable.close();

	    //We have just 4 fields in the TableFile, so returnedData will have max 1 byte as nullbit vector.
	    int tableId;
	    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));

	    if(tableId==0 || tableId>1000){
	    //	cout << "Table Id is incorrect : " << tableId ;
	    	return -1;
	    }

	    RBFM_ScanIterator rbsiColumn;
	    FileHandle columnFileHandle;
	    const vector<string> colAttributes ({"column-name", "column-type", "column-length"});
	    RecordBasedFileManager::instance()->openFile("column.clm", columnFileHandle);


	    rc =  RecordBasedFileManager::instance()->scan(columnFileHandle, getColumnMetaData(), "table-id", EQ_OP, (void *)&tableId, colAttributes, rbsiColumn);
	    if(rc != 0) {
	        	RecordBasedFileManager::instance()->closeFile(columnFileHandle);
	        	rbsiColumn.close();
	            free(returnedData);
	            return -1;
	    }

	    while(rbsiColumn.getNextRecord(rid, returnedData) != RM_EOF) {
	           int offset = 1;

	           int len;
	           memcpy(&len, (char *)returnedData + offset, sizeof(int));
	           offset += sizeof(int);

	           string tmp = string((char*)returnedData+offset, len);
	           offset += len;

	           int type;
	           memcpy(&type, (char *)returnedData + offset, sizeof(int));
	           offset += sizeof(int);

	           int length;
	           memcpy(&length, (char *)returnedData + offset, sizeof(int));

	           Attribute attr;
	           attr.name = tmp;
	           attr.type = (AttrType)type;
	           attr.length = (AttrLength)length;
	           attrs.push_back(attr);
	       }

	       rbsiColumn.close();
	      // RecordBasedFileManager::instance()->closeFile(columnFileHandle);

	       free(returnedData);


	return 0;
}

void RelationManager::getAttributeName(const string& indexName, string &attributeName) {
    auto i = 0;
    char delim = '_';
    vector<string> v;
    auto pos = indexName.find(delim);
    while (pos != string::npos) {
      v.push_back(indexName.substr(i, pos-i));
      i = ++pos;
      pos = indexName.find(delim, pos);

      if (pos == string::npos)
         v.push_back(indexName.substr(i, indexName.length()));
    }

    attributeName = v.back();
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	//Insert tuple will always check whether we have permission to insert the given tuple or not.
	//If yes, we will have to generate the record and then RBFM->InsertRecord to insert the data
	//If no return -1
	if(!(strcmp(tableName.c_str(),"table") || strcmp(tableName.c_str(),"column"))){
		return -1;
	}else{

		vector<Attribute> attrs;
		   int rc = getAttributes(tableName, attrs);
		   if(rc != 0) return -1;

		   FileHandle myFile;
		   rc = RecordBasedFileManager::instance()->openFile(tableName, myFile);
		   if(rc != 0) return -1;

		   rc = RecordBasedFileManager::instance()->insertRecord(myFile, attrs,  data, rid);
		   if(rc != 0) return -1;

		//   cout<< "The number of Pages in file " << myFile.fileNameDebug << " after insert tuple : " << myFile.getNumberOfPages() << endl;

		//   rc = RecordBasedFileManager::instance()->closeFile(myFile);
		//   if(rc != 0) return -1;

		   vector<string> indexNames;
		   vector<RID> indexRids;
		   rc = getAllIndexForATable(tableName, indexNames, indexRids);
		   if(rc !=0) return -1;

		   for(int i=0;i<indexNames.size();i++){
			   IXFileHandle ixFileHandle;
			   rc = IndexManager::instance()->openFile(indexNames[i],ixFileHandle);
			   if(rc !=0) return -1;

			   string attributeName;
			   getAttributeName(indexNames[i], attributeName);

			   Attribute attribute;
			   for(int i=0;i<attrs.size();i++){
				   if(strcmp(attributeName.c_str(),attrs[i].name.c_str())==0){
					   attribute = attrs[i];
				   }
			   }

			   void *attributeValue = malloc(attribute.length+1);
			   rc = RecordBasedFileManager::instance()->readAttribute(myFile, attrs, rid, attributeName, attributeValue);
			   if(rc !=0) return -1;

			   void *key = malloc(attribute.length+1);
			   if(attribute.type==TypeVarChar){
				   int length;
				   memcpy(&length,(char *)attributeValue+1,4);
				   memcpy(key,&length,4);
				   memcpy((char *)key+4,(char *)attributeValue+5,length);
			   }else{
				   memcpy(key,(char *)attributeValue+1,4);
			   }

			   rc = IndexManager::instance()->insertEntry(ixFileHandle, attribute, key, rid);
			   if(rc !=0) return -1;

			   rc = IndexManager::instance()->closeFile(ixFileHandle);
			   if(rc !=0) return -1;

			   free(key);
			   free(attributeValue);
		   }

		   rc = RecordBasedFileManager::instance()->closeFile(myFile);
		   if(rc != 0) return -1;
		   return 0;

	}
}



RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	// Permission check if you can delete or not
	// This should use deleteRecord given an RID
	if(tableName=="table" || tableName=="column"){
				return -1;
	}else{

		vector<Attribute> attrs;
		int rc = getAttributes(tableName, attrs);
		if(rc != 0) return -1;

		FileHandle myFile;
		rc = RecordBasedFileManager::instance()->openFile(tableName, myFile);
		if(rc != 0) return -1;

		vector<string> indexNames;
		   vector<RID> indexRids;
		   rc = getAllIndexForATable(tableName, indexNames, indexRids);
		   if(rc !=0) return -1;

		   for(int i=0;i<indexNames.size();i++){
			   IXFileHandle ixFileHandle;
			   rc = IndexManager::instance()->openFile(indexNames[i],ixFileHandle);
			   if(rc !=0) return -1;

			   string attributeName;
			   getAttributeName(indexNames[i], attributeName);

			   Attribute attribute;
			   for(int i=0;i<attrs.size();i++){
				   if(strcmp(attributeName.c_str(),attrs[i].name.c_str())==0){
					   attribute = attrs[i];
				   }
			   }

			   void *attributeValue = malloc(attribute.length+1);
			   rc = RecordBasedFileManager::instance()->readAttribute(myFile, attrs, rid, attributeName, attributeValue);
			   if(rc !=0) return -1;

			   void *key = malloc(attribute.length+1);
			   if(attribute.type==TypeVarChar){
				   int length;
				   memcpy(&length,(char *)attributeValue+1,4);
				   memcpy(key,&length,4);
				   memcpy((char *)key+4,(char *)attributeValue+5,length);
			   }else{
				   memcpy(key,(char *)attributeValue+1,4);
			   }

			   rc = IndexManager::instance()->deleteEntry(ixFileHandle, attribute, key, rid);
			   if(rc !=0) return -1;

			   rc = IndexManager::instance()->closeFile(ixFileHandle);
			   if(rc !=0) return -1;

			   free(key);
			   free(attributeValue);
		   }

		rc = RecordBasedFileManager::instance()->deleteRecord(myFile,attrs,rid);
		if(rc != 0) return -1;

		rc = RecordBasedFileManager::instance()->closeFile(myFile);
		if(rc != 0) return -1;



		return 0;

	}
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	// Permission check if you can update or not
	// Prepare the record to be updated
	// This should use updateRecord
	if(tableName=="table" || tableName=="column"){
					return -1;
	}else{

		vector<Attribute> attrs;
		int rc = getAttributes(tableName, attrs);
		if(rc != 0) return -1;

		FileHandle myFile;
		rc = RecordBasedFileManager::instance()->openFile(tableName, myFile);
		if(rc != 0) return -1;

		rc = RecordBasedFileManager::instance()->updateRecord(myFile,attrs,data,rid);
		if(rc != 0) return -1;

		rc = RecordBasedFileManager::instance()->closeFile(myFile);
		if(rc != 0) return -1;

		return 0;
	}
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	// Permission check ?? (I maybe wrong)
	// Read record
	// Extract the actual data from the record

	if(tableName=="table" || tableName=="column"){
			return -1;
	}else{

		vector<Attribute> attrs;
		int rc = getAttributes(tableName, attrs);
	    if(rc != 0) return -1;

	    FileHandle myFile;
	    rc = RecordBasedFileManager::instance()->openFile(tableName, myFile);
	    if(rc != 0) return -1;

	    rc = RecordBasedFileManager::instance()->readRecord(myFile,attrs,rid,data);
	    if(rc != 0) return -1;

	    rc = RecordBasedFileManager::instance()->closeFile(myFile);
	    if(rc != 0) return -1;

	    return 0;

	}
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	// If print record has the functionality to extract data from a record, we can use that
	return RecordBasedFileManager::instance()->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	// Check if you have permission (I maybe wrong)
	// If yes, this should call readAttribute of PFM layer
	if(tableName=="table" || tableName=="column"){
			return -1;
	}else{

		vector<Attribute> attrs;
		int rc = getAttributes(tableName, attrs);
	    if(rc != 0) return -1;

	    FileHandle myFile;
	    rc = RecordBasedFileManager::instance()->openFile(tableName, myFile);
	    if(rc != 0) return -1;

	    rc = RecordBasedFileManager::instance()->readAttribute(myFile,attrs,rid,attributeName,data);
	    if(rc != 0) return -1;

	    rc = RecordBasedFileManager::instance()->closeFile(myFile);
	    if(rc != 0) return -1;

	    return 0;

	}
}


RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute,
		const CompOp compOp,
		const void *value,
		const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator)
{
	// This should just call the RBFM Scan and initialize it

	string fileName = "";
		string copying = "";
		copying = tableName.c_str();
		fileName = tableName.c_str();
		if(!strcmp(tableName.c_str(),"Tables")) {
			copying ="";
			copying = "table";
			fileName = "table.tbl";
		}	else
			if(!strcmp(tableName.c_str(),"Columns")) {
				copying ="";
				copying = "column";
				fileName = "column.clm";
			}



//		FileHandle fileHandle;
//		RecordBasedFileManager::instance()->openFile(fileName,fileHandle);

		vector<Attribute> attrs;
	    int rc = getAttributes(copying.c_str(), attrs);
	    if(rc != 0) return -1;

	    FileHandle *fh = new FileHandle();
		rc = RecordBasedFileManager::instance()->openFile(fileName, *fh);
		if(rc != 0) return -1;

		RBFM_ScanIterator *rbfmScanIterator = new RBFM_ScanIterator();
		rbfmScanIterator->value = (void *) value;

		rc = RecordBasedFileManager::instance()->scan(*fh, attrs, conditionAttribute, compOp, value, attributeNames, *rbfmScanIterator);
		if(rc != 0) {
		   delete fh;
		   delete rbfmScanIterator;
		   return -1;
		}
		rm_ScanIterator.rbfmScanIterator = rbfmScanIterator;
		rm_ScanIterator.fileHandle = fh;
	    return 0;

}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

	// This would call the getNextRecord method of RBFM ScanIterator
	// Once it gets the nextRecord, it will use the method which converts a record into actual data
	// It will then return the tuple.

	 return rbfmScanIterator->getNextRecord(rid,data);

}

RC RM_ScanIterator::close() {
	delete fileHandle;
	return rbfmScanIterator->close();
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	return -1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	int rc;
	FileHandle fileHandle;
	rc = RecordBasedFileManager::instance()->openFile("index",fileHandle);
	if (rc != 0) {
		return -1;
	}

	vector<Attribute> indexRecordDescriptor = getIndexMetaData();
	RID rid;
	void *indexMeta = malloc(100);
	int recordSize = 0;
	// Initialize a NULL field indicator
	int nullFieldsIndicatorActualSize = getByteForNullsIndicator(indexRecordDescriptor.size());
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);


	prepareIndexRecord(indexRecordDescriptor.size(),nullsIndicator, tableName, tableName+"_"+attributeName, attributeName, indexMeta,&recordSize);
	RecordBasedFileManager::instance()->insertRecord(fileHandle,indexRecordDescriptor,indexMeta,rid);

	free(nullsIndicator);
	free(indexMeta);

	rc = RecordBasedFileManager::instance()->closeFile(fileHandle);
	if (rc != 0) {
			return -1;
	}

	rc = IndexManager::instance()->createFile(tableName+"_"+attributeName);
	if(rc == -1){
		return -1;
	}

	IXFileHandle ixfileHandle;
	rc = IndexManager::instance()->openFile(tableName+"_"+attributeName,ixfileHandle);
	if( rc !=0) return -1;

	//Now for this table, scan the table and insert every entry into this newly created index
	FileHandle tableFileHandle;
	rc = RecordBasedFileManager::instance()->openFile(tableName,tableFileHandle);

	vector<Attribute> attributes;
	getAttributes(tableName, attributes);

	Attribute attribute;
	for(int i=0;i<attributes.size();i++){
		if(strcmp(attributes[i].name.c_str(),attributeName.c_str())==0){
			attribute = attributes[i];
		}
	}

	RBFM_ScanIterator rbsi;
	const vector<string> attrs ({attributeName});
	void *returnedDataFromTable = malloc(100);

	rc = RecordBasedFileManager::instance()->scan(tableFileHandle, attributes,"", NO_OP, NULL, attrs, rbsi);
	if(rc != 0) {
		RecordBasedFileManager::instance()->closeFile(tableFileHandle);
		rbsi.close();
		free(returnedDataFromTable);
		return -1;
	}

	RID returnedRid;
	while(rbsi.getNextRecord(returnedRid, returnedDataFromTable) != RM_EOF) {
		void *key = malloc(attribute.length+1);
		if(attribute.type==TypeVarChar){
			int length;
			memcpy(&length,(char *)returnedDataFromTable+1,4);
			memcpy(key,&length,4);
			memcpy((char *)key+4,(char *)returnedDataFromTable+5,length);
		}else{
			memcpy(key, (char *)returnedDataFromTable+1,4);
		}
		rc = IndexManager::instance()->insertEntry(ixfileHandle,attribute,key,returnedRid);
		if(rc !=0){
			free(returnedDataFromTable);
			free(key);
			return -1;
		}

		free(key);
	}
	rbsi.close();
	rc = IndexManager::instance()->closeFile(ixfileHandle);
	if(rc !=0) return -1;

	return 0;

}

RC RelationManager::getAllIndexForATable(const string &tableName, vector<string> &indexNames, vector<RID> &indexRids){

	FileHandle myFileHandle;
	void *returnedDataFromTable = malloc(PAGE_SIZE);
	int rc = RecordBasedFileManager::instance()->openFile("index",myFileHandle);
	if(rc != 0)
	{
		return -1;
	}
	RBFM_ScanIterator rbsi;
	const vector<string> attrs ({"index-name"});

	if(tableName.empty()){
		rc = RecordBasedFileManager::instance()->scan(myFileHandle, getIndexMetaData(),"table-name", NO_OP, NULL, attrs, rbsi);
	}else{
		void *value = malloc(tableName.length()+4);
		int length = tableName.length();
		memcpy(value,&length,sizeof(int));
		memcpy((char *)value+4,tableName.c_str(),tableName.length());
		rc = RecordBasedFileManager::instance()->scan(myFileHandle, getIndexMetaData(),"table-name", EQ_OP, value, attrs, rbsi);
	}

	if(rc != 0) {
		RecordBasedFileManager::instance()->closeFile(myFileHandle);
		rbsi.close();
		free(returnedDataFromTable);
		return -1;
	}

	RID rid;
	while(rbsi.getNextRecord(rid, returnedDataFromTable) != RM_EOF) {
		int lengthOfValue=0;
		memcpy(&lengthOfValue,(char *)returnedDataFromTable+1,sizeof(int));
		void *varcharValue = malloc(lengthOfValue+1);
		memcpy(varcharValue,(char *)returnedDataFromTable+5,lengthOfValue);
		char endChar = '\0';
		memcpy((char*)varcharValue+lengthOfValue,&endChar,1);
		string indexName = string((char *)varcharValue);
		indexNames.push_back(indexName);
		indexRids.push_back(rid);
	}
	rbsi.close();

	free(returnedDataFromTable);
	rc = RecordBasedFileManager::instance()->closeFile(myFileHandle);
	if(rc==-1){
		return -1;
	}
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	//It will delete the entry from the index catalog table
	FileHandle myFileHandle;
	void *returnedDataFromTable = malloc(PAGE_SIZE);
	int rc = RecordBasedFileManager::instance()->openFile("index",myFileHandle);
	if(rc != 0)
	{
		return -1;
	}
	vector<RID> ridsToBeDeleted;
	RBFM_ScanIterator rbsi;
	const vector<string> attrs ({"index-name"});

	string indexName = tableName + "_" + attributeName;

	void *value = malloc(indexName.length()+4);
	int length = indexName.length();
	memcpy(value,&length,sizeof(int));
	memcpy((char *)value+4,indexName.c_str(),indexName.length());

	rc = RecordBasedFileManager::instance()->scan(myFileHandle, getIndexMetaData(),"index-name", EQ_OP, value, attrs, rbsi);
	if(rc != 0) {
		RecordBasedFileManager::instance()->closeFile(myFileHandle);
		rbsi.close();
		free(returnedDataFromTable);
		return -1;
	}

	RID ridToBeDeleted;
	while(rbsi.getNextRecord(ridToBeDeleted, returnedDataFromTable) != RM_EOF) {
		ridsToBeDeleted.push_back(ridToBeDeleted);
	}
	rbsi.close();

	if(ridsToBeDeleted.size()==0){
		RecordBasedFileManager::instance()->closeFile(myFileHandle);
		free(returnedDataFromTable);
		return 0;
	}

	for(std::vector<RID>::iterator it = ridsToBeDeleted.begin(); it != ridsToBeDeleted.end(); it++) {
		RecordBasedFileManager::instance()->deleteRecord(myFileHandle, getTableMetaData(),*it);
	}
	free(returnedDataFromTable);
	rc = RecordBasedFileManager::instance()->closeFile(myFileHandle);
	if(rc==-1){
		return -1;
	}
	rc = IndexManager::instance()->destroyFile(tableName+"_"+attributeName);
	if(rc==-1){
		return -1;
	}
	return 0;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	vector<Attribute> attributes;
	RC rc = getAttributes(tableName, attributes);
	if(rc==-1){
		return -1;
	}

	Attribute attribute;
	for(int i=0;i<attributes.size();i++){
		if(strcmp(attributes[i].name.c_str(),attributeName.c_str())==0){
			attribute = attributes[i];
		}
	}
	IXFileHandle *ixfileHandle = new IXFileHandle();
	rc = IndexManager::instance()->openFile(tableName+"_"+attributeName, *ixfileHandle);
	if(rc != 0) return -1;

	IX_ScanIterator *ix_ScanIterator = new IX_ScanIterator();

	rc = IndexManager::instance()->scan(*ixfileHandle, attribute,
	        lowKey,
	        highKey,
	        lowKeyInclusive,
	        highKeyInclusive,
	        *ix_ScanIterator);
	if(rc != 0) {
	   delete ixfileHandle;
	   delete ix_ScanIterator;
	   return -1;
	}
	rm_IndexScanIterator.ixScanIterator = ix_ScanIterator;
	rm_IndexScanIterator.ixfileHandle = ixfileHandle;
	return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
	return ixScanIterator->getNextEntry(rid,key);
}
