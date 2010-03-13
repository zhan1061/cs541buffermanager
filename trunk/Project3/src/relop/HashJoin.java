package relop;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.*;

import java.util.ArrayList;

/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator {
	private ArrayList<HashIndex> _lstOpenHashIndices = new ArrayList<HashIndex>();
	private ArrayList<HeapFile> _lstOpenHeapFiles = new ArrayList<HeapFile>();
	private Integer _lcol = null;
	private Integer _rcol = null;
	private BucketScan _leftBucketScan = null;
	private BucketScan _rightBucketScan = null;
	private static int _lastBucketScanID;
	private static int _lastHeapFileID;
	
	HashTableDup memoryHashTable = new HashTableDup();
	int position_in_bucket = 0;
	
	Tuple innerTuple;
	Tuple nextTuple;
	IndexScan outerIndexScan;
	IndexScan innerIndexScan;
	
	int out_column;
	int in_column;
	int currentHash;
	
	Tuple matchingTuples[] =null;
	static{
		_lastBucketScanID = 0;
		_lastHeapFileID = 0;
	}
	
	/**
	 * Constructs a hash join, given the left and right iterators and which
	 * columns to match (relative to their individual schemas).
	 */
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		this.out_column = lcol;
		this.in_column = rcol;
		schema = Schema.join(left.schema, right.schema);
		makeIndexScan(true, left, lcol);
		makeIndexScan(false, right, rcol);		
		
		_lcol = lcol;
		_rcol = rcol;

		_leftBucketScan = getAppropriateBucketScan(left, lcol);
		_rightBucketScan = getAppropriateBucketScan(right, rcol);
	}
	
	private IndexScan getIndexScan(Iterator iter, HeapFile heapFile, FileScan iFileScan, int indexKey)
	{
		HashIndex hashIndex = new HashIndex(null);
		while(iFileScan.hasNext()){
			hashIndex.insertEntry(new SearchKey(iFileScan.getNext().getField(indexKey)), iFileScan.getLastRID());
		}
		return new IndexScan(iter.schema, hashIndex, heapFile);
	}
	
	private void makeIndexScan(boolean outer, Iterator iter, int indexKey){
		HeapFile heapFile=null;
		IndexScan iScan=null;

		if(iter instanceof IndexScan){
			iScan=(IndexScan)iter;
		}
		else{
			FileScan iFileScan=null;
			if(iter instanceof FileScan){
				iFileScan = (FileScan)iter;
				heapFile = iFileScan._heapFile;
				iScan = getIndexScan(iter,heapFile, iFileScan, indexKey);
			}
			else{
				heapFile = new HeapFile(null);
				while(iter.hasNext()){
					heapFile.insertRecord(iter.getNext().data);
				}
				iFileScan = new FileScan(iter.schema, heapFile);
				iScan = getIndexScan(iter, heapFile, iFileScan, indexKey);
			}
			
		}
		if(outer){
			outerIndexScan=iScan;
		}
		else{
			innerIndexScan=iScan;
		}

	}//end of makeIndexScan
	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		System.out.println("Hash join op....");
		//outerIndexScan.explain(depth); 
		//innerIndexScan.explain(depth); 
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
    	innerIndexScan.restart();
    	outerIndexScan.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return (innerIndexScan.isOpen()&&outerIndexScan.isOpen());
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		///////// Vinay ///////////////	
		innerIndexScan.close();
		outerIndexScan.close();
		///////// Vinay ///////////////	
		/*
		// TODO: Cleanup temp files, temp indices		
		_leftBucketScan.close();
		_rightBucketScan.close();
		
		for(HeapFile heapFile : _lstOpenHeapFiles){
			try{
				heapFile.deleteFile();
			}catch(Exception exception){
				exception.printStackTrace();
			}
		}
		
		for(HashIndex hashIndex : _lstOpenHashIndices){
			hashIndex.deleteFile();
		}*/			
	}

	
	
	public void initMemoryHashTable(int hashCode){
		currentHash = hashCode;
		outerIndexScan.restart();
		memoryHashTable.clear();
		while(outerIndexScan.hasNext() && outerIndexScan.getNextHash() != currentHash){
			outerIndexScan.getNext();
		}
		while(outerIndexScan.getNextHash() == currentHash && outerIndexScan.hasNext() ){
			//reached the right partition. so now do this:
			Tuple outerTuple = outerIndexScan.getNext();
			memoryHashTable.add(new SearchKey(outerTuple.getField(out_column).toString()), outerTuple);
		}
	}

//	public void runloop()
//	{
//		for(;position_in_bucket < matchingTuples.length; position_in_bucket++){
//			if(innerTuple.getField(in_column).equals(matchingTuples[position_in_bucket].getField(out_column))){
//				nextTuple = Tuple.join(matchingTuples[position_in_bucket++], innerTuple, schema);
//			}
//		}
//	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		//int innerHashValue;
		int temp = 0;
		if(matchingTuples!=null){
			if(position_in_bucket==matchingTuples.length-1){
				position_in_bucket=0;
				matchingTuples=null;
				return hasNext();
			}
			else{
				while(position_in_bucket < matchingTuples.length){
					if(innerTuple.getField(in_column).equals(matchingTuples[position_in_bucket].getField(out_column))){
						nextTuple=Tuple.join(matchingTuples[position_in_bucket++],innerTuple,schema);
						return true;
					}
					position_in_bucket++;
				}
				position_in_bucket=0;
				matchingTuples=null;
				return hasNext();
			}
		}
		else{
		    int innerHashValue=innerIndexScan.getNextHash();
			if(innerHashValue!=currentHash){
				initMemoryHashTable(innerHashValue);
			}
			if(innerIndexScan.hasNext()){
				innerTuple=innerIndexScan.getNext();
				matchingTuples= memoryHashTable.getAll(new SearchKey(innerTuple.getField(in_column).toString()));
				if(matchingTuples!=null){
					while(position_in_bucket < matchingTuples.length){
						if(innerTuple.getField(in_column).equals(matchingTuples[position_in_bucket].getField(out_column))){
							nextTuple = Tuple.join(matchingTuples[position_in_bucket++],innerTuple,schema);
							return true;
						}
						position_in_bucket++;
					}
					position_in_bucket=0;
					matchingTuples=null;
					return hasNext();
				}
				position_in_bucket=0;
				matchingTuples=null;
				return hasNext();
			}
			else{
				matchingTuples=null;
				return false;
			}
		  
		}
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		if(nextTuple == null){
			throw new IllegalStateException("No tuples...");
		}else{
			return nextTuple;
		}
	}
	
	/**
	 * Returns the correct BucketScan for iterator.
	 * 
	 * @author anurag
	 * @param iterator
	 * @return
	 */
	private BucketScan getAppropriateBucketScan(Iterator iterator, int keyFieldNumber){
		BucketScan bucketScan = null;
		
		if(iterator instanceof FileScan){
			bucketScan = getBucketScan((FileScan)iterator, keyFieldNumber);
		}else if(iterator instanceof Selection){
			bucketScan = getBucketScan((Selection)iterator, keyFieldNumber);
		}else if(iterator instanceof SimpleJoin){
			bucketScan = getBucketScan((SimpleJoin)iterator, keyFieldNumber);
		}
		
		return bucketScan;
	}

	/**
	 * Returns BucketScan for fileScan.
	 * 
	 * @author anurag
	 * @param fileScan
	 * @param keyFieldNumber
	 * @return BucketScan containing index entries for records in file. 
	 */
	private BucketScan getBucketScan(FileScan fileScan, int keyFieldNumber){
		BucketScan bucketScan = null;
		// Create temporary HashIndex
		HashIndex tmpHashIndex = new HashIndex("tmpHashIndex%" + _lastBucketScanID++);
		
		fileScan.restart();
		
		while(fileScan.hasNext()){
			Tuple tuple = fileScan.getNext();
			// Get the RID just fetched and add it to tmpHashIndex
			tmpHashIndex.insertEntry(new SearchKey(tuple.getField(keyFieldNumber)), fileScan.getLastRID());			
		}
		
		_lstOpenHashIndices.add(tmpHashIndex);
		
		bucketScan = tmpHashIndex.openScan();
		
		return bucketScan;
	}

	/**
	 * Returns BucketScan for selection.
	 * 
	 * @author anurag
	 * @param selection
	 * @param keyFieldNumber
	 * @return
	 */
	private BucketScan getBucketScan(Selection selection, int keyFieldNumber){
		BucketScan bucketScan = null;
		// Create temporary HashIndex
		HashIndex tmpHashIndex = new HashIndex("tmpHashIndex%" + _lastBucketScanID++);
		HeapFile tmpHeapFile = new HeapFile("tmpHeapFile%" + _lastHeapFileID++);
		
		selection.restart();
		
		while(selection.hasNext()){
			Tuple tuple = selection.getNext();
			// Insert this tuple into tmpHeapFile.
			RID rid = tmpHeapFile.insertRecord(tuple.data);
			
			tmpHashIndex.insertEntry(new SearchKey(tuple.getField(keyFieldNumber)), rid);
		}
		
		_lstOpenHashIndices.add(tmpHashIndex);
		_lstOpenHeapFiles.add(tmpHeapFile);
		
		bucketScan = tmpHashIndex.openScan();
		
		return bucketScan;
	}
	
	/**
	 * Returns BucketScan for simpleJoin.
	 * 
	 * @author anurag
	 * @param simpleJoin
	 * @param keyFieldNumber
	 * @return
	 */
	private BucketScan getBucketScan(SimpleJoin simpleJoin, int keyFieldNumber){
		BucketScan bucketScan = null;
		// Create temporary HashIndex
		HashIndex tmpHashIndex = new HashIndex("tmpHashIndex%" + _lastBucketScanID++);
		HeapFile tmpHeapFile = new HeapFile("tmpHeapFile%" + _lastHeapFileID++);
		
		simpleJoin.restart();
		
		while(simpleJoin.hasNext()){
			Tuple tuple = simpleJoin.getNext();
			// Insert this tuple into tmpHeapFile.
			RID rid = tmpHeapFile.insertRecord(tuple.data);
			
			tmpHashIndex.insertEntry(new SearchKey(tuple.getField(keyFieldNumber)), rid);
		}
		
		_lstOpenHashIndices.add(tmpHashIndex);
		_lstOpenHeapFiles.add(tmpHeapFile);
		
		bucketScan = tmpHashIndex.openScan();
		
		return bucketScan;
	}
	
		
	
	/**
	 * Getter method for left BucketScan.
	 * For debugging purposes.
	 * 
	 * @author anurag
	 * @return
	 */
	public BucketScan getLeftBucketScan(){
		return _leftBucketScan;		
	}
	
	/**
	 * Getter method for left BucketScan.
	 * For debugging purposes.
	 * 
	 * @author anurag
	 * @return
	 */
	public BucketScan getRightBucketScan(){
		return _rightBucketScan;
	}
} // public class HashJoin extends Iterator
