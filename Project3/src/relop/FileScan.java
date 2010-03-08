package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	protected HeapFile _heapFile = null;
	private HeapScan _heapScan = null;
	private boolean _bOpen = false;
	private RID _currRID = null;
	
	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile file) {
		_heapFile = file;
		this.schema = schema;		
		_heapScan = _heapFile.openScan();
		_bOpen = true;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// No children for this guy.
		indent(depth);
		System.out.println("FileScan : Iterator");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// Reinitialize _heapScan
		_heapScan.close();
		_bOpen = false;
		_heapScan = _heapFile.openScan();
		_bOpen = true;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return _bOpen;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		_currRID = null;
		_heapScan.close();
		_bOpen = false;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return _heapScan.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() throws IllegalStateException{
		_currRID = new RID();
		byte[] nextRecordData = _heapScan.getNext(_currRID);
		Tuple tuple = new Tuple(schema, nextRecordData);		
		
		return tuple;
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		return _currRID;
	}

} // public class FileScan extends Iterator
