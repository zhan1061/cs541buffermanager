package heap;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;

public class Scan {
	private int _currDirPID = GlobalConst.INVALID_PAGE;
	private int _currHFPID = GlobalConst.INVALID_PAGE;
	private HFPage _currHFPage = null;
	private DirectoryPage _currDirPage = null;
	private int _currPageDirEntryOffset = 0;
	private boolean _bScanComplete = false;
	private RID _currRID = null;
	private Heapfile _heapfile = null;
	
	public Scan(Heapfile heapfile) throws ChainException{
		// Initialize currDirPID the first directory page of the file.
		_currDirPID = heapfile.getPageDirectoryPID(); 
		_currPageDirEntryOffset = -1;
		_currHFPID = GlobalConst.INVALID_PAGE;
		_currHFPage = null;
		_bScanComplete = false;
		_currRID = null;
		_heapfile = heapfile;		
		Page page = new Page();
		
		try{
			PageId currDirPageId = new PageId(_currDirPID);
			
			SystemDefs.JavabaseBM.pinPage(currDirPageId, page, false);
			
			_currDirPage = new DirectoryPage(page);
		}catch(Exception exception){
			throw new ChainException(exception, "Scan initialization failed.");
		}
	}
	
	public void closeScan(){
		if(_currDirPID != GlobalConst.INVALID_PAGE){
			// Unpin this.
			try{
				SystemDefs.JavabaseBM.unpinPage(new PageId(_currDirPID), false);
			}catch(Exception exception){
				// Do nothing.
			}
		}
		
		if(_currHFPID != GlobalConst.INVALID_PAGE){
			// Unpin this.
			try{
				SystemDefs.JavabaseBM.unpinPage(new PageId(_currHFPID), false);
			}catch(Exception exception){
				// Do nothing.
			}
		}
		
		_currPageDirEntryOffset = -1;
		_currHFPID = GlobalConst.INVALID_PAGE;
		_currHFPage = null;
		_bScanComplete = false;
		_currRID = null;
	}
	
	public boolean position(RID rid) throws InvalidTupleSizeException, IOException{
		if(rid == null || (rid.pageNo.pid < 0) || (rid.slotNo < 0)){
			throw new InvalidTupleSizeException(null, "Invalid tuple.");
		}
		
		int currPgDirPID = _heapfile.getPageDirectoryPID();
		
		// Start search loop.
		while(currPgDirPID != GlobalConst.INVALID_PAGE){
			// Pin page with PID currPgDirPID.
			Page currPage = new Page();
			PageId currPageId = new PageId(currPgDirPID);
			
			try{
				SystemDefs.JavabaseBM.pinPage(currPageId, currPage, false);
			}catch(Exception exception){
				throw new IOException("Unable to pin directory page.");
			}
			
			DirectoryPage currDirPage = new DirectoryPage(currPage);
			
			for(int pageDirectoryEntryOffset = 0; pageDirectoryEntryOffset < currDirPage.getTotalEntries() - 1;
				pageDirectoryEntryOffset++){
				PageDirectoryEntry pageDirectoryEntry = currDirPage.getPageDirectoryEntry(pageDirectoryEntryOffset);
				
				if(pageDirectoryEntry.getPID() == rid.pageNo.pid){
					// We'll now look inside the HFPage referred to by this PID.
					HFPage hfPage = new HFPage();
					
					try{
						SystemDefs.JavabaseBM.pinPage(new PageId(rid.pageNo.pid), hfPage, false);
						
						if(hfPage.getRecord(rid) != null){
							// Unpin the current directory and HF pages
							if(_currDirPID != GlobalConst.INVALID_PAGE){
								SystemDefs.JavabaseBM.unpinPage(new PageId(_currDirPID), false);
							}
							
							if(_currHFPID != GlobalConst.INVALID_PAGE){
								SystemDefs.JavabaseBM.unpinPage(new PageId(_currHFPID), false);
							}
							
							// Found the record. Set our private variables.
							_currDirPID = currPgDirPID;
							_currHFPID = rid.pageNo.pid;
							_currPageDirEntryOffset = pageDirectoryEntryOffset;
							_currRID = new RID(rid.pageNo, rid.slotNo);
							_currDirPage = currDirPage;
							_currHFPage = hfPage;
							_bScanComplete = false;
							
							return true;
						}else{
							// Unpin the directory page before returning.
							SystemDefs.JavabaseBM.unpinPage(currPageId, true);
							SystemDefs.JavabaseBM.unpinPage(new PageId(rid.pageNo.pid), true);	
							
							// Page not found.
							return false;
						}
					}catch(Exception exception){
						throw new IOException("Unable to pin directory page.");
					}
				}
			}
			
			// Next directory.
			currPgDirPID = currDirPage.getNextDirectory().getPID();
			
			try{
				SystemDefs.JavabaseBM.unpinPage(currPageId, true);
			}catch(Exception exception){
				throw new IOException("Unable to pin directory page.");
			}
		}
		
		// Page not found.
		return false;
	}

	
	/**
	 * Returns next tuple in the scan.
	 * @param rid
	 * @return
	 * @throws InvalidTupleSizeException
	 * @throws IOException
	 */
	public Tuple getNext(RID rid) throws InvalidTupleSizeException, IOException{
		if(rid == null){
			throw new InvalidTupleSizeException(null, "Invalid tuple.");
		}
		
		// Keep looking till the first non-null HFPage is encountered.
		while(_currHFPage == null || _currHFPage.empty() || (_currRID != null && _currHFPage.nextRecord(_currRID) == null)){
			try {
				fetchNextHFPage();

				_currRID = null;
			} catch (ChainException e) {
				_bScanComplete = true;

				return null;
			}

			if(_bScanComplete){
				return null;
			}
		}

		Tuple nextTuple = null;

		// Return the next record from the current HFPage
		if(_currRID == null){
			_currRID = _currHFPage.firstRecord();
		}else{
			_currRID = _currHFPage.nextRecord(_currRID);
		}

		try{
			nextTuple = _currHFPage.getRecord(_currRID);
			rid.pageNo = new PageId(_currRID.pageNo.pid);
			rid.slotNo = _currRID.slotNo;			
		}catch(Exception exception){
			throw new InvalidTupleSizeException(exception, "Unable to get record.");
		}

		return nextTuple;
	}
	
	/**
	 * Fetches the next page into currHFPage. Pins it too.
	 * Before pinning the next page, the current one will be unpinned.
	 */
	private void fetchNextHFPage() throws ChainException{
		// Unpin the current HFPage
		if(_currHFPID != GlobalConst.INVALID_PAGE){
			PageId currHFPageId = new PageId(_currHFPID);
			
			try{
				SystemDefs.JavabaseBM.unpinPage(currHFPageId, false);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to unpin HFPage.");
			}
		}
		
		// Check if we have reached the end of this directory.
		if(_currPageDirEntryOffset >= _currDirPage.getTotalEntries() - 1){
			// End of directory reached. Get next directory, if available.
			// Also, reset _currPageDirEntryOffset.
			int nextDirPagePID = _currDirPage.getNextDirectory().getPID();
			
			// Unpin current directory page and pin the next one.
			SystemDefs.JavabaseBM.unpinPage(new PageId(_currDirPID), false);
			
			// If nextDirPagePID is invalid, we've reached the end of the scan.
			// Reset all private tracking variables to invalid and exit.
			if(nextDirPagePID == GlobalConst.INVALID_PAGE){
				_currDirPID = GlobalConst.INVALID_PAGE;
				_currHFPID = GlobalConst.INVALID_PAGE;
				_currHFPage = null;
				_currDirPage = null;
				_currPageDirEntryOffset = -1;
				_bScanComplete = true;
				
				return;
			}
			
			_currDirPID = nextDirPagePID;
			Page page = new Page();
			
			try{
				SystemDefs.JavabaseBM.pinPage(new PageId(_currDirPID), page, false);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to pin directory page.");
			}
			
			_currDirPage = new DirectoryPage(page);
			// Set current directory entry offset to -1 to indicate
			// that scanning hasn't begun yet on this directory.
			_currPageDirEntryOffset = -1;			
		}
		
		_currPageDirEntryOffset++;
		
		PageDirectoryEntry pageDirectoryEntry = _currDirPage.getPageDirectoryEntry(_currPageDirEntryOffset);
		// Fetch the HFPage in.
		_currHFPID = pageDirectoryEntry.getPID();
		
		if(_currHFPID != GlobalConst.INVALID_PAGE){
			// Set current HFPage.
			Page page = new Page();

			try{
				SystemDefs.JavabaseBM.pinPage(new PageId(_currHFPID), page, false);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to pin HFPage.");
			}

			_currHFPage = new HFPage(page);
		}else{
			_currHFPage = null;
		}
	}
}
