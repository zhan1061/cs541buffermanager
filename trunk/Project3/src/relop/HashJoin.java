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
	
	static{
		_lastBucketScanID = 0;
		_lastHeapFileID = 0;
	}
	
	/**
	 * Constructs a hash join, given the left and right iterators and which
	 * columns to match (relative to their individual schemas).
	 */
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		_lcol = lcol;
		_rcol = rcol;

		_leftBucketScan = getAppropriateBucketScan(left, lcol);
		_rightBucketScan = getAppropriateBucketScan(right, rcol);
	}
	
	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
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
		}			
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		throw new UnsupportedOperationException("Not implemented");
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
