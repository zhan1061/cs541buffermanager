package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	private HashIndex hashIndex;
	private HeapFile file;
	private HashScan hashScan;
	private SearchKey key;
	
	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		this.schema=schema;
		this.file=file;
		this.hashIndex=index;
		this.hashScan=this.hashIndex.openScan(key);
		this.key=key;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		System.out.println("keyscan....");		
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if(hashScan!=null){
			hashScan.close();
		}
		hashScan=hashIndex.openScan(key);
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return hashScan!=null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if(hashScan!=null){
			hashScan.close();
			hashScan=null;
		}	
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		boolean retVal=hashScan.hasNext();
		return retVal;	
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		RID rid=hashScan.getNext();
		byte []data =file.selectRecord(rid);
		if(data!=null){
			return new Tuple (schema, data);
		}else{
			throw new IllegalStateException("No more tuples....");
		}
	}

} // public class KeyScan extends Iterator
