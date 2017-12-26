#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC insert(IXFileHandle &ixfileHandle, Attribute attribute,  const void *key, const unsigned &pageNumber, const RID &rid, void *newKey, int &newPageNumber);

        unsigned short findOffsetForNewKey(Attribute attribute, void *pageData, const void *key, unsigned short endOffset);

        unsigned short findOffsetForNewKeyInLeafNode(Attribute attribute, void *pageData, const void *key, unsigned short endOffset);
        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        void validateAllLeafNodes(IXFileHandle &ixfileHandle, const Attribute &attribute);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        RC compareKey(Attribute attribute,const void *key, void *keyToBeComparedWith, CompOp compOp);

        RC floatCompare(float &conditionAttributeValue,float &valueToBeComparedWith,CompOp compOp);

        RC intCompare(int &conditionAttributeValue, int &valueToBeComparedWith, CompOp compOp);

        RC varcharCompare(string conditionAttributeValue, string valueToBeComparedWith, CompOp compOp);

        RC getKeyValue(Attribute attribute, const void *pageData, void *keyValue, unsigned short startOffset) const;

        int findLeftMostLeafPage(IXFileHandle &ixfileHandle, const Attribute attribute);

        unsigned getKeyLength(Attribute attribute, const void *pageData, unsigned short startOffset) const;

        int findLeafPage(IXFileHandle &ixfileHandle, const Attribute attribute, const void *key, int &pageNumber);

        void printNonLeafNodesOfBtree(IXFileHandle &ixfileHandle, const Attribute attribute) const;

        void validateNonLeafNode(IXFileHandle &ixfileHandle, Attribute attribute, PageNum &pageNum);

        void recursivePrint(unsigned &PageNumber, IXFileHandle &ixfileHandle, const Attribute attribute, bool onlyNonLeafNodes) const;
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IX_ScanIterator {
    public:

		int position;
		Attribute attribute;
		vector<int> intKeys;
		vector<float> floatKeys;
		vector<string> varcharKeys;
		vector<RID> rids;
		IXFileHandle *ixfileHandle;

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {

    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
