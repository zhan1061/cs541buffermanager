package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

	private BucketScan bucketScan;
	private HashIndex hashIndex;
	private HeapFile file;
	
	
	
	
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  this.schema=schema;
	  this.hashIndex=index;
	  this.file=file;
	  this.bucketScan=this.hashIndex.openScan();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Index Scan....");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if(bucketScan!=null ){
		  bucketScan.close();
	  }
	  bucketScan=hashIndex.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return (bucketScan!=null);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  if(bucketScan!=null){
		  bucketScan.close();
		  bucketScan=null;
	  }
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  return bucketScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  RID rid=bucketScan.getNext();
	  byte []data=file.selectRecord(rid);
	  if(data!=null){
		  return new Tuple(schema,data);
	  }else{
		  throw new IllegalStateException("No more tuples left....");
	  }
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  return bucketScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  return bucketScan.getNextHash();
  }

} // public class IndexScan extends Iterator
