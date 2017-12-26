
#include "qe.h"
#include <cfloat>
#include <sstream>

Filter::Filter(Iterator* input, const Condition &condition)
{
	this->input = input;
	this->condition = condition;
}

RC Filter::getNextTuple(void *data)
{
	while(input->getNextTuple(data)==0){
		if(checkCondition(data)==1){
			return 0;
		}
	}
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
	input->getAttributes(attrs);
}

int Filter::checkCondition(void *data){
	int result = 0;
	vector<Attribute> attributes;
	getAttributes(attributes);

	Attribute conditionAttr;
	for(int i=0;i<attributes.size();i++){
		if(strcmp(attributes[i].name.c_str(),condition.lhsAttr.c_str())==0){
			conditionAttr = attributes[i];
		}
	}

	void *lhsAttrData = malloc(conditionAttr.length);
	memset(lhsAttrData, 0, conditionAttr.length);
	if(readAttribute(attributes, data, lhsAttrData, condition.lhsAttr) == -1)
	{
		free(lhsAttrData);
		return 0;
	}

	if(condition.bRhsIsAttr == true){
		Attribute rightConditionAttr;
		for(int i=0;i<attributes.size();i++){
			if(strcmp(attributes[i].name.c_str(),condition.rhsAttr.c_str())==0){
				rightConditionAttr = attributes[i];
			}
		}
		void *rhsAttrData = malloc(conditionAttr.length);
		memset(rhsAttrData, 0, conditionAttr.length);

		if(readAttribute(attributes, data, rhsAttrData, condition.rhsAttr) == -1)
		{
			free(lhsAttrData);
			free(rhsAttrData);
			return 0;
		}

		result = compareValue(lhsAttrData, rhsAttrData, rightConditionAttr.type, condition.op);
		free(rhsAttrData);
	}else{
		result = compareValue(lhsAttrData, condition.rhsValue.data, conditionAttr.type, condition.op);
	}

	free(lhsAttrData);
	return result;
}

RC Filter::readAttribute(const vector<Attribute> &attributes, const void *inputData, void *attributeData, string &attrName)
{
	int sizeOfNullBitArray = ceil((double) attributes.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator, inputData, sizeOfNullBitArray);
	bool nullBit = false;
	int i=0;
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
		nullBit = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!nullBit){
			if(it->type==TypeVarChar){
				int length;
				memcpy(&length,(char *)inputData+offset,4);
				offset += 4;
				if(it->name==attrName){
					memcpy((char *)attributeData, &length,4);
					memcpy((char *)attributeData+4, (char *)inputData+offset,length);
					break;
				}else{
					offset += length;
				}
			}else{
				if(it->name==attrName){
					memcpy((char *)attributeData, (char *)inputData+offset,4);
					break;
				}else{
					offset += 4;
				}
			}
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

RC Filter::compareValue(const void *leftData, const void *rightData, AttrType attrType, CompOp compOp)
{
	if(attrType == TypeInt)
    {
        return intCompare(*(int *)leftData, *(int *)rightData, compOp);
    }

    if(attrType == TypeReal)
    {
        return floatCompare(*(float *)leftData, *(float *)rightData, compOp);
    }

    if(attrType == TypeVarChar)
    {
    	int leftLength=0;
    	memcpy(&leftLength,(char *)leftData,sizeof(int));
    	void *leftValue = malloc(leftLength+1);
    	memcpy(leftValue,(char *)leftData+4,leftLength);
    	char endChar = '\0';
    	memcpy((char*)leftValue+leftLength,&endChar,1);
    	string left = string((char *)leftValue);

    	int rightLenth=0;
		memcpy(&rightLenth,(char *)rightData,sizeof(int));
		void *rightValue = malloc(rightLenth+1);
		memcpy(rightValue,(char *)rightData+4,rightLenth);
		memcpy((char*)rightValue+rightLenth,&endChar,1);
		string right = string((char *)rightValue);

        return varcharCompare(left, right, compOp);
    }

    return 0;
}

RC Filter::floatCompare(float conditionAttributeValue,float valueToBeComparedWith,CompOp compOp) {
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

RC Filter::intCompare(int conditionAttributeValue, int valueToBeComparedWith, CompOp compOp) {
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

RC Filter::varcharCompare(string conditionAttributeValue, string valueToBeComparedWith, CompOp compOp) {

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

Project::Project(Iterator *input, const vector<string> &attrNames){
	this->input = input;
	input->getAttributes(allAttributes);
	for(int i=0;i<allAttributes.size();i++){
		for(int j=0;j<attrNames.size();j++){
			if(strcmp(allAttributes[i].name.c_str(),attrNames[j].c_str())==0){
				projectedAttributes.push_back(allAttributes[i]);
				break;
			}
		}
	}
}

void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = projectedAttributes;
}

RC Project::getNextTuple(void *data)
{
    void *record = malloc(PAGE_SIZE);
    if(input->getNextTuple(record) == -1){
        free(record);
        return -1;
    }

    int sizeOfNullBitArray = ceil((double) projectedAttributes.size() / CHAR_BIT);
    unsigned short offset = sizeOfNullBitArray;
    unsigned char *nullBitArray = (unsigned char *) malloc(sizeOfNullBitArray);
    memset(nullBitArray,0,sizeOfNullBitArray);
    for(int i=0;i<projectedAttributes.size();i++){
    	void *attributeData = malloc(projectedAttributes[i].length);
    	int attributeLength = 0;
    	bool isNull = false;
    	readAttribute(allAttributes, record, attributeData, projectedAttributes[i].name, attributeLength, isNull);
    	if (!isNull) {
    		nullBitArray[i/8] &= ~(1 << (7 - i%8));
		} else {
			nullBitArray[i/8] |=  (1 << (7 - i%8));
		}
    	memcpy((char *)data+offset,attributeData, attributeLength);
    	offset += attributeLength;
    }
    memcpy(data, nullBitArray, sizeOfNullBitArray);
    free(nullBitArray);
    free(record);
    return 0;
}

RC Project::readAttribute(const vector<Attribute> &attributes, const void *inputData, void *attributeData, string &attrName, int &attributeLength, bool &isNull)
{
	int sizeOfNullBitArray = ceil((double) attributes.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator, inputData, sizeOfNullBitArray);
	isNull = false;
	int i=0;
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
		isNull = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!isNull){
			if(it->type==TypeVarChar){
				int length;
				memcpy(&length,(char *)inputData+offset,4);
				offset += 4;
				if(it->name==attrName){
					memcpy((char *)attributeData, &length,4);
					memcpy((char *)attributeData+4, (char *)inputData+offset,length);
					attributeLength = length + 4;
					break;
				}else{
					offset += length;
				}
			}else{
				if(it->name==attrName){
					memcpy((char *)attributeData, (char *)inputData+offset,4);
					attributeLength = 4;
					break;
				}else{
					offset += 4;
				}
			}
		}else{
			attributeLength = 0;
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        ){

	this->leftInput = leftIn;
	this->rightInput = rightIn;
	this->condition = condition;
	this->numPages = numPages;
	this->blockIterator = 0;

	leftInput->getAttributes(leftAttrs);
	rightInput->getAttributes(rightAttrs);

	rightData = malloc(calculateMaxSizeOfTuple(rightAttrs));
	getNextBlock();

}

BNLJoin::~BNLJoin()
{
	leftAttrs.clear();
	rightAttrs.clear();
	free(rightData);
	freePreviousBlock();
}

RC BNLJoin::getNextBlock(){
	freePreviousBlock();
	int size = 0;
	void *firstEntry = malloc(calculateMaxSizeOfTuple(leftAttrs));
	if(leftInput->getNextTuple(firstEntry)==-1){
		free(firstEntry);
		return -1;
	}else{
		size += calculateLengthOfTuple(leftAttrs,firstEntry);
		leftTuplesBlock.push_back(firstEntry);
	}
	while(size<numPages*PAGE_SIZE){
		void *blockEntry = malloc(calculateMaxSizeOfTuple(leftAttrs));
		if(leftInput->getNextTuple(blockEntry)!=-1){
			size += calculateLengthOfTuple(leftAttrs,blockEntry);
			leftTuplesBlock.push_back(blockEntry);
		}else{
			break;
		}
	}
	return 0;
}

RC BNLJoin::freePreviousBlock(){
	for(int i=0;i<leftTuplesBlock.size();i++){
		free(leftTuplesBlock[i]);
	}
	leftTuplesBlock.clear();
	return 0;
}

RC BNLJoin::getNextTuple(void *data){

	while(rightInput->getNextTuple(rightData)!=-1){
		for(;blockIterator<leftTuplesBlock.size();blockIterator++){
			if(checkCondition(leftTuplesBlock[blockIterator],rightData)==1){
				mergeTuples(leftTuplesBlock[blockIterator],rightData, data);
				return 0;
			}
		}
		blockIterator = 0;
	}
	rightInput->setIterator();
	if(getNextBlock()==-1){
		return -1;
	}else{
		return getNextTuple(data);
	}
}



void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	for(int i=0;i<leftAttrs.size();i++){
		attrs.push_back(leftAttrs[i]);
	}

	for(int i=0;i<rightAttrs.size();i++){
		attrs.push_back(rightAttrs[i]);
	}
}

int BNLJoin::checkCondition(const void* leftTuple, const void* rightTuple){
	int isTrue = 0; //0 means condition is false

	Attribute conditionAttr;
	for(int i=0;i<leftAttrs.size();i++){
		if(strcmp(leftAttrs[i].name.c_str(),condition.lhsAttr.c_str())==0){
			conditionAttr = leftAttrs[i];
		}
	}

	void *lhsAttrData = malloc(conditionAttr.length);
	if(readAttribute(leftAttrs, leftTuple, lhsAttrData, condition.lhsAttr) == -1)
	{
		free(lhsAttrData);
		return 0;
	}

	void *rhsAttrData = malloc(conditionAttr.length);
	if(readAttribute(rightAttrs, rightTuple, rhsAttrData, condition.rhsAttr) == -1)
	{
		free(lhsAttrData);
		free(rhsAttrData);
		return 0;
	}

	isTrue = compareValue(lhsAttrData, rhsAttrData, conditionAttr.type, condition.op);

	free(lhsAttrData);
	free(rhsAttrData);
	return isTrue;
}

RC BNLJoin::mergeTuples(const void *leftData, const void *rightData, void *mergedData)
{
	int lengthOfLeft = calculateLengthOfTuple(leftAttrs, leftData);
	int lengthOfRight = calculateLengthOfTuple(rightAttrs, rightData);

	vector<Attribute> mergedAttrs;
	getAttributes(mergedAttrs);
	int nullBitArraySize = ceil((double) mergedAttrs.size() / CHAR_BIT);
	void *nullBitArray = malloc(nullBitArraySize);
	memset(nullBitArray,0,nullBitArraySize);
	memcpy(mergedData,nullBitArray, nullBitArraySize);
	free(nullBitArray);

	int leftNullBitSize = ceil((double) leftAttrs.size() / CHAR_BIT);
	int rightNullBitSize = ceil((double) rightAttrs.size() / CHAR_BIT);

	memcpy((char *)mergedData+nullBitArraySize,(char *)leftData+leftNullBitSize,lengthOfLeft-leftNullBitSize);
	memcpy((char *)mergedData+nullBitArraySize+lengthOfLeft-leftNullBitSize,(char *)rightData+rightNullBitSize,lengthOfRight-rightNullBitSize);

	return 0;
}

int BNLJoin::calculateLengthOfTuple(vector<Attribute> &recordDescriptor, const void* data){
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
			if(!nullBit){
				if(it->type == TypeVarChar){
					int varLength;
					memcpy(&varLength, (char *)data+offset, sizeof(int));
					offset += sizeof(int);
					offset += varLength;
				}else if(it->type == TypeInt){
					offset += sizeof(int);
				}else if(it->type == TypeReal){
					offset += sizeof(float);
				}
			}
			i++;
		}
		free(nullsIndicator);
		return offset;
}

unsigned int BNLJoin::calculateMaxSizeOfTuple(const vector<Attribute> &recordDescriptor){
	unsigned int size = 0;
	size += ceil((double) recordDescriptor.size() / CHAR_BIT); //Size of null bit array
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
		size += it->length;
	}
	return size;
}

RC BNLJoin::readAttribute(const vector<Attribute> &attributes, const void *inputData, void *attributeData, string &attrName)
{
	int sizeOfNullBitArray = ceil((double) attributes.size() / CHAR_BIT);
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
		if(it->type==TypeVarChar){
			int length;
			memcpy(&length,(char *)inputData+offset,4);
			offset += 4;
			if(it->name==attrName){
				memcpy((char *)attributeData, &length,4);
				memcpy((char *)attributeData+4, (char *)inputData+offset,length);
				break;
			}else{
				offset += length;
			}
		}else{
			if(it->name==attrName){
				memcpy((char *)attributeData, (char *)inputData+offset,4);
				break;
			}else{
				offset += 4;
			}
		}
	}
	return 0;
}

RC BNLJoin::compareValue(const void *leftData, const void *rightData, AttrType attrType, CompOp compOp)
{
	if(attrType == TypeInt)
    {
        return intCompare(*(int *)leftData, *(int *)rightData, compOp);
    }

    if(attrType == TypeReal)
    {
        return floatCompare(*(float *)leftData, *(float *)rightData, compOp);
    }

    if(attrType == TypeVarChar)
    {
    	int leftLength=0;
    	memcpy(&leftLength,(char *)leftData,sizeof(int));
    	void *leftValue = malloc(leftLength+1);
    	memcpy(leftValue,(char *)leftData+4,leftLength);
    	char endChar = '\0';
    	memcpy((char*)leftValue+leftLength,&endChar,1);
    	string left = string((char *)leftValue);

    	int rightLenth=0;
		memcpy(&rightLenth,(char *)rightData,sizeof(int));
		void *rightValue = malloc(rightLenth+1);
		memcpy(rightValue,(char *)rightData+4,rightLenth);
		memcpy((char*)rightValue+rightLenth,&endChar,1);
		string right = string((char *)rightValue);

        return varcharCompare(left, right, compOp);
    }

    return 0;
}

RC BNLJoin::floatCompare(float conditionAttributeValue,float valueToBeComparedWith,CompOp compOp) {
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

RC BNLJoin::intCompare(int conditionAttributeValue, int valueToBeComparedWith, CompOp compOp) {
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

RC BNLJoin::varcharCompare(string conditionAttributeValue, string valueToBeComparedWith, CompOp compOp) {

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

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
		this->leftInput = leftIn;
		this->rightInput = rightIn;
		this->condition = condition;

		leftInput->getAttributes(leftAttrs);
		rightInput->getAttributes(rightAttrs);
		for(int i=0;i<leftAttrs.size();i++){
			if(strcmp(leftAttrs[i].name.c_str(),condition.lhsAttr.c_str())==0){
				this->conditionAttr = leftAttrs[i];
			}
		}
}

INLJoin::~INLJoin()
{
	leftAttrs.clear();
	rightAttrs.clear();
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{

	for(int i=0;i<leftAttrs.size();i++){
		attrs.push_back(leftAttrs[i]);
	}

	for(int i=0;i<rightAttrs.size();i++){
		attrs.push_back(rightAttrs[i]);
	}
}

RC INLJoin::getNextTuple(void *data)
{
	void *leftData = malloc(calculateMaxSizeOfTuple(leftAttrs));
	void *rightData = malloc(calculateMaxSizeOfTuple(rightAttrs));

	while(leftInput->getNextTuple(leftData)!=-1){
		void *lhsAttrData = malloc(conditionAttr.length);
		int attributeLength = 0;
		bool isNull = false;
		readAttribute(leftAttrs, leftData, lhsAttrData, condition.lhsAttr, attributeLength, isNull);
		rightInput->setIterator(lhsAttrData, lhsAttrData, true, true);
		free(lhsAttrData);
		while(rightInput->getNextTuple(rightData) != -1){
			mergeTuples(leftData, rightData, data);
			free(leftData);
			free(rightData);
			return 0;
		}
	}
	free(leftData);
	free(rightData);
	return -1;
}

RC INLJoin::readAttribute(const vector<Attribute> &attributes, const void *inputData, void *attributeData, string &attrName, int &attributeLength, bool &isNull)
{
	int sizeOfNullBitArray = ceil((double) attributes.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator, inputData, sizeOfNullBitArray);
	isNull = false;
	int i=0;
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
		isNull = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!isNull){
			if(it->type==TypeVarChar){
				int length;
				memcpy(&length,(char *)inputData+offset,4);
				offset += 4;
				if(it->name==attrName){
					memcpy((char *)attributeData, &length,4);
					memcpy((char *)attributeData+4, (char *)inputData+offset,length);
					attributeLength = length + 4;
					break;
				}else{
					offset += length;
				}
			}else{
				if(it->name==attrName){
					memcpy((char *)attributeData, (char *)inputData+offset,4);
					attributeLength = 4;
					break;
				}else{
					offset += 4;
				}
			}
		}else{
			attributeLength = 0;
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

unsigned int INLJoin::calculateMaxSizeOfTuple(const vector<Attribute> &recordDescriptor){
	unsigned int size = 0;
	size += ceil((double) recordDescriptor.size() / CHAR_BIT); //Size of null bit array
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
		size += it->length;
	}
	return size;
}

RC INLJoin::mergeTuples(const void *leftData, const void *rightData, void *mergedData)
{
	int lengthOfLeft = calculateLengthOfTuple(leftAttrs, leftData);
	int lengthOfRight = calculateLengthOfTuple(rightAttrs, rightData);

	vector<Attribute> mergedAttrs;
	getAttributes(mergedAttrs);
	int nullBitArraySize = ceil((double) mergedAttrs.size() / CHAR_BIT);
	void *nullBitArray = malloc(nullBitArraySize);
	memset(nullBitArray,0,nullBitArraySize);
	memcpy(mergedData,nullBitArray, nullBitArraySize);
	free(nullBitArray);

	int leftNullBitSize = ceil((double) leftAttrs.size() / CHAR_BIT);
	int rightNullBitSize = ceil((double) rightAttrs.size() / CHAR_BIT);

	memcpy((char *)mergedData+nullBitArraySize,(char *)leftData+leftNullBitSize,lengthOfLeft-leftNullBitSize);
	memcpy((char *)mergedData+nullBitArraySize+lengthOfLeft-leftNullBitSize,(char *)rightData+rightNullBitSize,lengthOfRight-rightNullBitSize);

	return 0;
}

int INLJoin::calculateLengthOfTuple(vector<Attribute> &recordDescriptor, const void* data){
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
			if(!nullBit){
				if(it->type == TypeVarChar){
					int varLength;
					memcpy(&varLength, (char *)data+offset, sizeof(int));
					offset += sizeof(int);
					offset += varLength;
				}else if(it->type == TypeInt){
					offset += sizeof(int);
				}else if(it->type == TypeReal){
					offset += sizeof(float);
				}
			}
			i++;
		}
		free(nullsIndicator);
		return offset;
}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
				  ){
	this->input = input;
	this->aggAttr = aggAttr;
	this->aggregateOp = op;
	input->getAttributes(attrs);
	this->isEnd = false;
}

RC Aggregate::getNextTuple(void *data){
	if(isEnd==true){
		return -1;
	}

	float sum = 0;
	float min = FLT_MAX;
	float max = FLT_MIN;
	float count = 0;
	float avg = 0;
	bool isNullKey = false;

	void *inputData = malloc(calculateMaxSizeOfTuple(attrs));
	while(input->getNextTuple(inputData)!=-1){
		void *attrData = malloc(aggAttr.length);
		memset(attrData,0,aggAttr.length);
		int attributeLength = 0;
		bool isNull = false;
		readAttribute(attrs, inputData, attrData, aggAttr.name, attributeLength, isNull);
		if(!isNull){
			float value;
			if(aggAttr.type==TypeInt){
				int intValue = *(int *)attrData;
				value = (float) intValue;
			}else{
				value = *(float *)attrData;
			}

			switch (aggregateOp)
				{
					case MIN:		min = (min>value)?value:min;
									break;
					case MAX:		max = (max<value)?value:max;
									break;
					case SUM:		sum += value;
									break;
					case AVG:		sum += value;
									count++;
									break;
					case COUNT:		count++;
									break;
				};
		}else{
			isNullKey = isNull;
		}
	}

	bitset<8> nullByte = 00000000;
	if(isNullKey){
		nullByte = 10000000;
	}
	memcpy((char *) data, &nullByte, 1);

	switch (aggregateOp){
			case MIN:		memcpy((char *)data+1,&min,4);
							break;
			case MAX:		memcpy((char *)data+1,&max,4);
							break;
			case SUM:		memcpy((char *)data+1,&sum,4);
							break;
			case AVG:		avg = sum/count;
							memcpy((char *)data+1,&avg,4);
							break;
			case COUNT:		memcpy((char *)data+1,&count,4);
							break;
	};
	isEnd = true;
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	Attribute attribute;
	attribute.type = TypeReal;
	attribute.name = getAttributeName();
	attribute.length = 4;
	attrs.clear();
	attrs.push_back(attribute);
}

string Aggregate::getAttributeName() const{
	switch (aggregateOp)
		    {
		        case MIN:		return "MIN(" + aggAttr.name + ")";
		        case MAX:		return "MAX(" + aggAttr.name + ")";
		        case SUM:		return "SUM(" + aggAttr.name + ")";
		        case AVG:		return "AVG(" + aggAttr.name + ")";
		        case COUNT:		return "COUNT(" + aggAttr.name + ")";
		        default:		return "";
		    };
}

RC Aggregate::readAttribute(const vector<Attribute> &attributes, const void *inputData, void *attributeData, string &attrName, int &attributeLength, bool &isNull)
{
	int sizeOfNullBitArray = ceil((double) attributes.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator, inputData, sizeOfNullBitArray);
	isNull = false;
	int i=0;
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
		isNull = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!isNull){
			if(it->type==TypeVarChar){
				int length;
				memcpy(&length,(char *)inputData+offset,4);
				offset += 4;
				if(it->name==attrName){
					memcpy((char *)attributeData, &length,4);
					memcpy((char *)attributeData+4, (char *)inputData+offset,length);
					attributeLength = length + 4;
					break;
				}else{
					offset += length;
				}
			}else{
				if(it->name==attrName){
					memcpy((char *)attributeData, (char *)inputData+offset,4);
					attributeLength = 4;
					break;
				}else{
					offset += 4;
				}
			}
		}else{
			attributeLength = 0;
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

unsigned int Aggregate::calculateMaxSizeOfTuple(const vector<Attribute> &recordDescriptor){
	unsigned int size = 0;
	size += ceil((double) recordDescriptor.size() / CHAR_BIT); //Size of null bit array
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
		size += it->length;
	}
	return size;
}

/*GHJoin::GHJoin(		Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){
	this->leftInput = leftIn;
	this->rightInput = rightIn;
	this->condition = condition;
	this->numPartitions = numPartitions;
	leftIn->getAttributes(this->leftAttrs);
	rightIn->getAttributes(this->rightAttrs);
	for(int i=0;i<leftAttrs.size();i++){
		if(strcmp(condition.lhsAttr.c_str(),leftAttrs[i].name.c_str())==0){
			conditionAttr = leftAttrs[i];
		}
		leftAttrNames.push_back(leftAttrs[i].name);
	}
	for(int i=0;i<rightAttrs.size();i++){
		rightAttrNames.push_back(rightAttrs[i].name);
	}
	doPartition();
	//Loading first partition in memory
	RBFM_ScanIterator rbfmScanIterator;
	RecordBasedFileManager::instance()->scan(leftPartitionFiles[0], leftAttrs, "", NO_OP, NULL, leftAttrNames, rbfmScanIterator);
	RID rid;
	void *returnedData = malloc(calculateMaxSizeOfTuple(leftAttrs));
	while(rbfmScanIterator.getNextRecord(rid, returnedData) != RM_EOF) {
		void *dataToPush = malloc(calculateMaxSizeOfTuple(leftAttrs));
		memcpy(dataToPush,returnedData,calculateMaxSizeOfTuple(leftAttrs));
		inmemoryPart.push_back(dataToPush);
		memset(returnedData,0,calculateMaxSizeOfTuple(leftAttrs));
	}
	free(returnedData);
	rbfmScanIterator.close();
	partitionIter = 1;
	leftIter = 0;
}

RC GHJoin::loadNextPartition(){
	freePrevPartition();
	if(partitionIter==numPartitions){
		return -1;
	}
	RBFM_ScanIterator rbfmScanIterator;
	RecordBasedFileManager::instance()->scan(leftPartitionFiles[partitionIter], leftAttrs, "", NO_OP, NULL, leftAttrNames, rbfmScanIterator);
	RID rid;
	void *returnedData = malloc(calculateMaxSizeOfTuple(leftAttrs));
	while(rbfmScanIterator.getNextRecord(rid, returnedData) != RM_EOF) {
		void *dataToPush = malloc(calculateMaxSizeOfTuple(leftAttrs));
		memcpy(dataToPush,returnedData,calculateMaxSizeOfTuple(leftAttrs));
		inmemoryPart.push_back(dataToPush);
		memset(returnedData,0,calculateMaxSizeOfTuple(leftAttrs));
	}
	free(returnedData);
	rbfmScanIterator.close();
	partitionIter++;
	return 0;
}

void GHJoin::freePrevPartition(){
	for(int i=0;i<inmemoryPart.size();i++){
		free(inmemoryPart[i]);
	}
	inmemoryPart.clear();
}

void GHJoin::doPartition(){
	for(int i=0;i<numPartitions;i++){
		stringstream ss;
		ss << "left";
		ss << "_join_";
		ss << i;
		string leftFileName = ss.str();
		RecordBasedFileManager::instance()->createFile(leftFileName.c_str());
		FileHandle leftFileHandle;
		RecordBasedFileManager::instance()->openFile(leftFileName.c_str(), leftFileHandle);
		leftPartitionFiles.push_back(leftFileHandle);

		stringstream ss1;
		ss1 << "right";
		ss1 << "_join_";
		ss1 << i;
		string rightFileName = ss1.str();
		RecordBasedFileManager::instance()->createFile(rightFileName.c_str());
		FileHandle rightFileHandle;
		RecordBasedFileManager::instance()->openFile(rightFileName.c_str(), rightFileHandle);
		rightPartitionFiles.push_back(rightFileHandle);
	}
	void *leftData = malloc(calculateMaxSizeOfTuple(leftAttrs));
	while(leftInput->getNextTuple(leftData)!=-1){
		void* leftAttrData = malloc(conditionAttr.length);
		int attributeLength = 0;
		bool isNull = false;
		readAttribute(leftAttrs, leftData, leftAttrData, conditionAttr.name, attributeLength, isNull);
		int hashValue = getHashValue(leftAttrData);
		RID rid;
		RecordBasedFileManager::instance()->insertRecord(leftPartitionFiles[hashValue],leftAttrs,leftData,rid);
	}
	//Close if they are not opened again
	free(leftData);
	void *rightData = malloc(calculateMaxSizeOfTuple(rightAttrs));
	while(rightInput->getNextTuple(rightData)!=-1){
		void* rightAttrData = malloc(conditionAttr.length);
		int attributeLength = 0;
		bool isNull = false;
		readAttribute(rightAttrs, rightData, rightAttrData, conditionAttr.name, attributeLength, isNull);
		int hashValue = getHashValue(rightAttrData);
		RID rid;
		RecordBasedFileManager::instance()->insertRecord(rightPartitionFiles[hashValue],rightAttrs,rightData,rid);
	}
	free(rightData);
}

unsigned int GHJoin::calculateMaxSizeOfTuple(const vector<Attribute> &recordDescriptor){
	unsigned int size = 0;
	size += ceil((double) recordDescriptor.size() / CHAR_BIT); //Size of null bit array
	for(std::vector<Attribute>::const_iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it){
		size += it->length;
	}
	return size;
}

RC GHJoin::readAttribute(const vector<Attribute> &attributes, const void *inputData, void *attributeData, string &attrName, int &attributeLength, bool &isNull)
{
	int sizeOfNullBitArray = ceil((double) attributes.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(sizeOfNullBitArray);
	//Getting the null bit array
	memcpy(nullsIndicator, inputData, sizeOfNullBitArray);
	isNull = false;
	int i=0;
	unsigned short offset = sizeOfNullBitArray;
	for(std::vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
		isNull = nullsIndicator[i/8] & (1 << (7-(i%8)));
		if(!isNull){
			if(it->type==TypeVarChar){
				int length;
				memcpy(&length,(char *)inputData+offset,4);
				offset += 4;
				if(it->name==attrName){
					memcpy((char *)attributeData, &length,4);
					memcpy((char *)attributeData+4, (char *)inputData+offset,length);
					attributeLength = length + 4;
					break;
				}else{
					offset += length;
				}
			}else{
				if(it->name==attrName){
					memcpy((char *)attributeData, (char *)inputData+offset,4);
					attributeLength = 4;
					break;
				}else{
					offset += 4;
				}
			}
		}else{
			attributeLength = 0;
		}
		i++;
	}
	free(nullsIndicator);
	return 0;
}

int GHJoin::getHashValue(void *key)
{
    switch (conditionAttr.type)
    {
        case TypeInt:
        {
            int value;
            memcpy(&value, key, sizeof(int));
            return ((value*2654435761) % (2^32))%numPartitions;
        }

        case TypeReal:
        {
        	float value;
        	memcpy(&value, key, sizeof(float));
        	unsigned int num = (unsigned int)value*100;
        	return ((num*2654435761) % (2^32))%numPartitions;
        }

        case TypeVarChar:
        {

        	int length=0;
			memcpy(&length,(char *)key,sizeof(int));
			void *value = malloc(length+1);
			memcpy(value,(char *)key+4,length);
			char endChar = '\0';
			memcpy((char*)value+length,&endChar,1);
			string str = string((char *)value);
            int hash = 0;
            int offset = 'a' - 1;
            for(string::const_iterator it=str.begin(); it!=str.end(); ++it) {
              hash = hash << 1 | (*it - offset);
            }
            return hash%numPartitions;
        }

        default:
        	return -1;
    }
}

GHJoin::~GHJoin(){
	freePrevPartition();
	for(int i=0;i<leftPartitionFiles.size();i++){
		RecordBasedFileManager::instance()->closeFile(leftPartitionFiles[i]);
	}
	leftPartitionFiles.clear();
	for(int i=0;i<rightPartitionFiles.size();i++){
		RecordBasedFileManager::instance()->closeFile(rightPartitionFiles[i]);
	}
	rightPartitionFiles.clear();
}

RC GHJoin::getNextTuple(void *data){
	RBFM_ScanIterator rbfmScanIterator;
	RecordBasedFileManager::instance()->scan(rightPartitionFiles[partitionIter], rightAttrs, "", NO_OP, NULL, rightAttrNames, rbfmScanIterator);
	RID rid;
	void *returnedData = malloc(calculateMaxSizeOfTuple(rightAttrs));
	while(rbfmScanIterator.getNextRecord(rid, returnedData) != RM_EOF) {
		for(;leftIter<inmemoryPart.size();leftIter++){
			if(compareValue(inmemoryPart[leftIter],returnedData, conditionAttr.type, EQ_OP)==1){
				mergeTuples(inmemoryPart[leftIter], returnedData, data);
				return 0;
			}
		}
		leftIter = 0;
	}
	if(loadNextPartition()==-1){
		free(returnedData);
		rbfmScanIterator.close();
		return -1;
	}else{
		free(returnedData);
		rbfmScanIterator.close();
		partitionIter++;
		return getNextTuple(data);
	}
}

RC GHJoin::compareValue(const void *leftData, const void *rightData, AttrType attrType, CompOp compOp)
{
	if(attrType == TypeInt)
    {
        return intCompare(*(int *)leftData, *(int *)rightData, compOp);
    }

    if(attrType == TypeReal)
    {
        return floatCompare(*(float *)leftData, *(float *)rightData, compOp);
    }

    if(attrType == TypeVarChar)
    {
    	int leftLength=0;
    	memcpy(&leftLength,(char *)leftData,sizeof(int));
    	void *leftValue = malloc(leftLength+1);
    	memcpy(leftValue,(char *)leftData+4,leftLength);
    	char endChar = '\0';
    	memcpy((char*)leftValue+leftLength,&endChar,1);
    	string left = string((char *)leftValue);

    	int rightLenth=0;
		memcpy(&rightLenth,(char *)rightData,sizeof(int));
		void *rightValue = malloc(rightLenth+1);
		memcpy(rightValue,(char *)rightData+4,rightLenth);
		memcpy((char*)rightValue+rightLenth,&endChar,1);
		string right = string((char *)rightValue);

        return varcharCompare(left, right, compOp);
    }

    return 0;
}

RC GHJoin::floatCompare(float conditionAttributeValue,float valueToBeComparedWith,CompOp compOp) {
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

RC GHJoin::intCompare(int conditionAttributeValue, int valueToBeComparedWith, CompOp compOp) {
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

RC GHJoin::varcharCompare(string conditionAttributeValue, string valueToBeComparedWith, CompOp compOp) {

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

RC GHJoin::mergeTuples(const void *leftData, const void *rightData, void *mergedData)
{
	int lengthOfLeft = calculateLengthOfTuple(leftAttrs, leftData);
	int lengthOfRight = calculateLengthOfTuple(rightAttrs, rightData);

	vector<Attribute> mergedAttrs;
	getAttributes(mergedAttrs);
	int nullBitArraySize = ceil((double) mergedAttrs.size() / CHAR_BIT);
	void *nullBitArray = malloc(nullBitArraySize);
	memset(nullBitArray,0,nullBitArraySize);
	memcpy(mergedData,nullBitArray, nullBitArraySize);
	free(nullBitArray);

	int leftNullBitSize = ceil((double) leftAttrs.size() / CHAR_BIT);
	int rightNullBitSize = ceil((double) rightAttrs.size() / CHAR_BIT);

	memcpy((char *)mergedData+nullBitArraySize,(char *)leftData+leftNullBitSize,lengthOfLeft-leftNullBitSize);
	memcpy((char *)mergedData+nullBitArraySize+lengthOfLeft-leftNullBitSize,(char *)rightData+rightNullBitSize,lengthOfRight-rightNullBitSize);

	return 0;
}

int GHJoin::calculateLengthOfTuple(vector<Attribute> &recordDescriptor, const void* data){
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
			if(!nullBit){
				if(it->type == TypeVarChar){
					int varLength;
					memcpy(&varLength, (char *)data+offset, sizeof(int));
					offset += sizeof(int);
					offset += varLength;
				}else if(it->type == TypeInt){
					offset += sizeof(int);
				}else if(it->type == TypeReal){
					offset += sizeof(float);
				}
			}
			i++;
		}
		free(nullsIndicator);
		return offset;
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const{

	for(int i=0;i<leftAttrs.size();i++){
		attrs.push_back(leftAttrs[i]);
	}

	for(int i=0;i<rightAttrs.size();i++){
		attrs.push_back(rightAttrs[i]);
	}
}*/

// ... the rest of your implementations go here
