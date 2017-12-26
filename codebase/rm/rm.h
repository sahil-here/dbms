
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <fstream>
#include <stdint.h>
#include "../rbf/rbfm.h"
#include <unordered_set>
#include <errno.h>
#include <sys/stat.h>
#include <cassert>
#include <string.h>
#include "../ix/ix.h"


using namespace std;

# define RM_EOF (-1)  // end of a scan operator
#define TableCatalog ("Tables")
#define ColumnCatalog ("Columns")

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
	RM_ScanIterator() {};
	~RM_ScanIterator() {};

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data) ;//{ return RM_EOF; };
	RC close() ;//{ return -1; };

	RBFM_ScanIterator *rbfmScanIterator;
	FileHandle *fileHandle;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close() {return -1;};             			// Terminate index scan

  IX_ScanIterator *ixScanIterator;
  IXFileHandle *ixfileHandle;
};

// Relation Manager
class RelationManager
{
public:
	static RelationManager* instance();

	RC createCatalog();

	RC deleteCatalog();

	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC deleteTuple(const string &tableName, const RID &rid);

	RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

	// Print a tuple that is passed to this utility method.
	// The format is the same as printRecord().
	RC printTuple(const vector<Attribute> &attrs, const void *data);

	RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

	// Scan returns an iterator to allow the caller to go through the results one by one.
	// Do not store entire results in the scan iterator.
	RC scan(const string &tableName,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparison type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);


	RC prepareTableRecord(int fieldCount,unsigned char *nullFieldsIndicator,const int table_ID,const int nameLength,
			const string &name,const int fileNamelength,const string &fileName, const int tablePermission,void *buffer,int *recordSize);

	RC prepareColumnRecord(int fieldCount, unsigned char *nullFieldsIndicator,const int Table_ID,const int nameLength, const string &name,
			int attrType,const int colLength,const int colPosition,const int colVersion,void *buffer, int *recordSize ) ;

	RC prepareIndexRecord(int fieldCount,  unsigned char *nullFieldsIndicator, const string &tableName,
			const string &indexName, const string &attributeName, void *buffer, int *recordSize);

	RC getAllIndexForATable(const string &tableName, vector<string> &indexNames, vector<RID> &indexRids);

	void getAttributeName(const string& indexName, string &attributeName);


	//FILE* stats;
//	int getTableID() ;
//	RC setMaxTableId(int tableID);

	vector<Attribute> getTableMetaData();

	vector<Attribute> getColumnMetaData();

	vector<Attribute> getIndexMetaData();

	RC printCatalog();

	//fstream MAXTABLEID;
	RC getMaxTableId();

	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName,
				 const string &attributeName,
				 const void *lowKey,
				 const void *highKey,
				 bool lowKeyInclusive,
				 bool highKeyInclusive,
				 RM_IndexScanIterator &rm_IndexScanIterator);

	// Extra credit work (10 points)
public:
	RC addAttribute(const string &tableName, const Attribute &attr);

	RC dropAttribute(const string &tableName, const string &attributeName);


protected:
	RelationManager();
	~RelationManager();

};

#endif
