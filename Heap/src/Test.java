import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;

public class Test {
	public static void bootstrap(){
		String dbpath = "/tmp/hptest"+System.getProperty("user.name")+".minibase-db"; 
	    String logpath = "/tmp/hptest"+System.getProperty("user.name")+".minibase-log"; 
	    
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		//Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}
	}
	
	public static void heapfileTest(){		
		System.out.println((SystemDefs.JavabaseBM).getNumUnpinnedBuffers() + " / " + (SystemDefs.JavabaseBM).getNumBuffers());
		Heapfile heapfile = null;
		
		try{
			 heapfile = new Heapfile("file_1");
			 
			 System.out.println("Heapfile object created. Directory PID: " + heapfile.getPageDirectoryPID());
		}catch(Exception exception){
			exception.printStackTrace();
		}
		
//		try{
//			Heapfile heapfile = new Heapfile("file_2");
//		}catch(Exception exception){
//			exception.printStackTrace();
//		}
	}
	
	public static void main(String[] args){
		bootstrap();
		
		heapfileTest();
		
		System.out.println("Slot size: " + HFPage.SIZE_OF_SLOT);
		System.out.println("Page size: " + (SystemDefs.JavabaseDB).db_page_size());
		
		// Allocate one page. This call will also pin it.
		Page page = new Page();
		PageId firstPageId = null;
		HFPage hfPage = null;
		
		try{
			firstPageId = (SystemDefs.JavabaseBM).newPage(page, 1);
		}catch(Exception exception){
			exception.printStackTrace();
		}
		
		if(firstPageId != null){
			// First page is now pinned. Create an HFPage from it.
			// Unpin page.
//			try{
//				(SystemDefs.JavabaseBM).unpinPage(firstPageId, false);
//			}catch(Exception exception){
//				System.out.println(exception.getMessage());
//			}
			
			hfPage = new HFPage(page);
			
			try{
				// Initialize page.
				hfPage.init(firstPageId, page);
			}catch(Exception exception){
				System.out.println(exception.getMessage());
			}
		}
		
		if(hfPage != null){
			// Add record to HFPage.
			DummyRecord rec = new DummyRecord(32);
			rec.ival = 5;
			rec.fval = (float) (5*2.5);
			rec.name = "record" + 5;
			
			try{
				RID rid1 = hfPage.insertRecord(rec.toByteArray());
				
				// Change rec slightly
				rec.ival = 10;
				rec.fval = (float) 11.32;
				rec.name = "record" + 10;
				
				RID rid2 = hfPage.insertRecord(rec.toByteArray());
				
				// Change rec slightly
				rec.ival = 13;
				rec.fval = (float) 13.32;
				rec.name = "record" + 13;
				
				System.out.println("Space available in hfPage: " + hfPage.available_space());
				
				RID rid3 = hfPage.insertRecord(rec.toByteArray());
				
				System.out.println("Space available in hfPage: " + hfPage.available_space());
				System.out.println("Page no.: " + rid1.pageNo + "; Slot no.: " + rid1.slotNo);
				System.out.println("Page no.: " + rid2.pageNo + "; Slot no.: " + rid2.slotNo);
				System.out.println("Page no.: " + rid3.pageNo + "; Slot no.: " + rid3.slotNo);
				
				// Now, lets read the records we just wrote. We should
				// test scanning and 'random' access.
				// Access record2.
				Tuple tuple2 = hfPage.getRecord(rid2);
				byte[] tupleData = tuple2.getTupleByteArray();
				
				rec = new DummyRecord(tuple2);
				
				// Output tuple2's contents.
				System.out.println("ival: " + rec.ival + "; fval: " + rec.fval + "; strval: " +
						rec.name);
				
				// Delete rid2
				System.out.println("Deleting rid-2...");
				hfPage.deleteRecord(rid2);
				
				System.out.println("Starting record scan...");
				
				// Scan.
				RID curRID = hfPage.firstRecord();
				
				while(curRID != null){
					Tuple scanTuple = hfPage.getRecord(curRID);
					rec = new DummyRecord(scanTuple);
					
					System.out.println("ival: " + rec.ival + "; fval: " + rec.fval + "; strval: " +
							rec.name);
					
					curRID = hfPage.nextRecord(curRID);
				}
				
			}catch(Exception exception){
				exception.printStackTrace();
			}
		}else{
			System.out.println("Couldn't get new HFPage.");
		}
	}
}

class DummyRecord  {

	//content of the record
	public int    ival; 
	public float  fval;      
	public String name;  

	//length under control
	private int reclen;

	private byte[]  data;

	/** Default constructor
	 */
	public DummyRecord() {}

	/** another constructor
	 */
	public DummyRecord (int _reclen) {
		setRecLen (_reclen);
		data = new byte[_reclen];
	}

	/** constructor: convert a byte array to DummyRecord object.
	 * @param arecord a byte array which represents the DummyRecord object
	 */
	public DummyRecord(byte [] arecord) 
	throws java.io.IOException {
		setIntRec (arecord);
		setFloRec (arecord);
		setStrRec (arecord);
		data = arecord; 
		setRecLen(name.length());
	}

	/** constructor: translate a tuple to a DummyRecord object
	 *  it will make a copy of the data in the tuple
	 * @param atuple: the input tuple
	 */
	public DummyRecord(Tuple _atuple) 
	throws java.io.IOException{   
		data = new byte[_atuple.getLength()];
		data = _atuple.getTupleByteArray();
		setRecLen(_atuple.getLength());

		setIntRec (data);
		setFloRec (data);
		setStrRec (data);

	}

	/** convert this class objcet to a byte array
	 *  this is used when you want to write this object to a byte array
	 */
	public byte [] toByteArray() 
	throws java.io.IOException {
		//    data = new byte[reclen];
		Convert.setIntValue (ival, 0, data);
		Convert.setFloValue (fval, 4, data);
		Convert.setStrValue (name, 8, data);
		return data;
	}

	/** get the integer value out of the byte array and set it to
	 *  the int value of the DummyRecord object
	 */
	public void setIntRec (byte[] _data) 
	throws java.io.IOException {
		ival = Convert.getIntValue (0, _data);
	}

	/** get the float value out of the byte array and set it to
	 *  the float value of the DummyRecord object
	 */
	public void setFloRec (byte[] _data) 
	throws java.io.IOException {
		fval = Convert.getFloValue (4, _data);
	}

	/** get the String value out of the byte array and set it to
	 *  the float value of the HTDummyRecorHT object
	 */
	public void setStrRec (byte[] _data) 
	throws java.io.IOException {
		// System.out.println("reclne= "+reclen);
		// System.out.println("data size "+_data.size());
		name = Convert.getStrValue (8, _data, reclen-8);
	}

	//Other access methods to the size of the String field and 
	//the size of the record
	public void setRecLen (int size) {
		reclen = size;
	}

	public int getRecLength () {
		return reclen;
	}  
}
