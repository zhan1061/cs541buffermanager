package bufmgr;

import java.io.*;
import java.util.*;
import diskmgr.*;
import global.*;
import chainexception.*;

/*
 * Describes a frame.
 */
class FrameDescriptor{
	private PageId _pageNumber = null;
	private int _pinCount;
	private boolean _bDirty;
	private boolean _bEmpty;
	
	public FrameDescriptor(){
		// Initialize members
		_pageNumber = new PageId();
		_pageNumber.pid = Constants.NO_PAGE;
		_pinCount = 0;
		_bDirty = false;
		_bEmpty = false;
	}

	public int getPage(){
		return _pageNumber.pid;
	}

	public int getPinCount(){
		return _pinCount;
	}

	public boolean isDirty(){
		return _bDirty;
	}

	public void setPage(int page){
		_pageNumber.pid = page;
	}

	public void setDirty(boolean bDirty){
		_bDirty = bDirty;
	}

	public void setPinCount(int pinCount){
		_pinCount = pinCount;
	}

	public void incrementPinCount(){
		_pinCount++;
	}

	public void decrementPinCount(){
		_pinCount--;
	}
}

public class BufMgr{
	String _replacer = "clock";
	byte[][] _pool = null;
	FrameDescriptor[] _arrFrameDescriptor = null;
	String _dbName = "singh47_db";
	PageHashtable _pageHashtable = null;
	int _numBufs = 0;

	int _lastFrameAllotted = 0;
	
	ClockReplace replace = null;
	
	public BufMgr(int numBufs, String replacer) throws ChainException{
		_pool = new byte[numBufs][GlobalConst.MINIBASE_PAGESIZE];
		_replacer = replacer;
		_numBufs = numBufs;
		_arrFrameDescriptor = new FrameDescriptor[_numBufs];
        
		//creating obj of ClockReplace.java
		replace = new ClockReplace();
		
		for(int frameDescriptorOffset = 0; frameDescriptorOffset < _arrFrameDescriptor.length; frameDescriptorOffset++){
			_arrFrameDescriptor[frameDescriptorOffset] = new FrameDescriptor();
		}

		_pageHashtable = new PageHashtable(Constants.PAGE_HASHTABLE_SIZE);
		SystemDefs.JavabaseBM = this;
		
		// Initialize replace queue by adding all frames to it.
		for(int frameNumber = 0; frameNumber < _numBufs; frameNumber++){
			replace.addToList(frameNumber);
		}
	}

	/**
	 * Allocate new pages.
	 * Call DB object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page
	 * and pin it. (This call allows a client of the Buffer Manager
	 * to allocate pages on disk.) If buffer is full, i.e., you
	 * can't find a frame for the first page, ask DB to deallocate
	 * all these pages, and return null.
	 *
	 * @param firstpage the address of the first page.
	 * @param howmany total number of allocated new pages.
	 *
	 * @return the first page id of the new pages.  null, if error.
	 */
	public PageId newPage(Page firstPage, int howmany) throws ChainException{
		if(howmany <= 0){
			// Invalid parameters.
			throw new InvalidRunSizeException(null, "Invalid run size specified.");
		}

		if(firstPage == null){
			throw new InvalidPageAddressException(null, "Invalid first page address supplied.");
		}

		if(!isBufferFull()){
			// Buffer not full.
			PageId firstPageId = new PageId();
			
			// Try to allocate a page on disk.
			try{
				(SystemDefs.JavabaseDB).allocate_page(firstPageId, howmany);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to allocate " + howmany + " pages.");
			}
			
			// Page allocated.
			// Pin the first page.
			pinPage(firstPageId, firstPage, false);			
			
			return firstPageId;
		}else{
			// Buffer full.
			return null;
		}
	}

	public void pinPage(PageId pinPgId, Page page, boolean emptyPage) throws ChainException{
		if(page == null){
			return;
		}
		
		int frameToUse;
		
		// Check if page is in pool already - consult hashtable.
		if((frameToUse = _pageHashtable.getPageFrame(pinPgId.pid)) != Constants.NO_FRAME_FOUND){
			// Page is in pool.
			if(_arrFrameDescriptor[frameToUse].getPinCount() == 0){
				// Remove page from replace-queue since it's no longer a replacement candidate.
				replace.removeFromList(frameToUse);
			}
			
			// Increment pin count
			_arrFrameDescriptor[frameToUse].incrementPinCount();
			
			page.setpage(_pool[frameToUse]);
		}else{
			// Page not in pool.
			if((frameToUse = replace.getReplaceFrame()) == Constants.NO_FRAME_FOUND){
				throw new BufferPoolExceededException(null, "No unpinned frames available.");
			}else{
				// Remove the old mapping from the hashtable, if it exists.
				int pageToRemoveFromTable = _arrFrameDescriptor[frameToUse].getPage();
				
				if(pageToRemoveFromTable >= 0 & pageToRemoveFromTable < SystemDefs.JavabaseDB.db_num_pages()){
					if(_arrFrameDescriptor[frameToUse].isDirty()){
						PageId pageIdToRemove = new PageId();
						pageIdToRemove.pid = pageToRemoveFromTable;
						flushPage(pageIdToRemove);
					}
					
					_pageHashtable.removeMappingForPage(pageToRemoveFromTable);
				}
			}
			
			// Assign frameToUse to pinPgId in the hashtable
			_pageHashtable.setPageFrame(pinPgId.pid, frameToUse);
			
			// Set page address.
			page.setpage(_pool[frameToUse]);
			
			// Read page into disk.
			try{
				(SystemDefs.JavabaseDB).read_page(pinPgId, page);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to read page.");
			}
			
			// Set pin count to 1 in the descriptor and set bDirty to false.
			_arrFrameDescriptor[frameToUse].setPage(pinPgId.pid);
			_arrFrameDescriptor[frameToUse].setPinCount(1);
			_arrFrameDescriptor[frameToUse].setDirty(false);
		}			
	}
	
	public void unpinPage(PageId upgid, boolean dirty)throws PagePinnedException, HashEntryNotFoundException
	    {
	        int frameNum = _pageHashtable.getPageFrame(upgid.pid);
	        if(frameNum == Constants.NO_FRAME_FOUND)
	        {
	        	throw new HashEntryNotFoundException(null, "Page not in buffer");
	        }
	        else
	        {
	            if(_arrFrameDescriptor[frameNum].getPinCount() == 0){
	    			throw new PagePinnedException(null, "Pin count is already 0");
	            }
	            
	            _arrFrameDescriptor[frameNum].setDirty(dirty);   
	            _arrFrameDescriptor[frameNum].decrementPinCount();
	            
	            if(_arrFrameDescriptor[frameNum].getPinCount() == 0){
	                //ClockReplace.java' s object is replace...
	                replace.addToList(frameNum);
	            }
	        }
	    }
	
	
	/**
	 * Used to flush a particular page of the buffer pool to disk.
	 * This method calls the write_page method of the diskmgr package.
	 *
	 * @param pageid the page number in the database.
	 */
	public void flushPage(PageId pageid) throws ChainException{
		if(pageid == null){
			throw new InvalidPageAddressException(null, "Invalid page address specified.");
		}
		
		// Check if there exists a frame for this page.
		int frameForPage;
		
		if((frameForPage = _pageHashtable.getPageFrame(pageid.pid)) != Constants.NO_FRAME_FOUND){
			// We have a valid frame to flush to disk.
			Page pageToWrite = new Page();
			pageToWrite.setpage(_pool[frameForPage]);
			
			// Write frame to disk.
			try{
				(SystemDefs.JavabaseDB).write_page(pageid, pageToWrite);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to write frame to disk.");
			}
		}else{
			throw new PageNotInPoolException(null, "Page " + pageid.pid + " not in pool.");
		}
	}
	
	/** Flushes all pages of the buffer pool to disk
	 */
	public void flushAllPages() throws ChainException{
		ArrayList lstPages = _pageHashtable.getAllPages();
		
		for(int lstPagesOffset = 0; lstPagesOffset < lstPages.size(); lstPagesOffset++){
			PageId pageId = new PageId();
			pageId.pid = (Integer)lstPages.get(lstPagesOffset);
			
			flushPage(pageId);
		}
	}
	
	/*
	 * Returns true if all frames have non-zero pin counts.
	 */
	private boolean isBufferFull(){
		// Iterate through the descriptors to see if there is a page with a pin-count of 0.
		for(int frameDescriptorOffset = 0; frameDescriptorOffset < _arrFrameDescriptor.length; frameDescriptorOffset++){
			if(_arrFrameDescriptor[frameDescriptorOffset].getPinCount() == 0){
				// Buffer not full. We found a frame that hasn't been pinned yet.
				return false;
			}
		}
	
		// All frames have non-zero pin-counts. Buffer is full.
		return true;
	}
	
	/**
	 * This method should be called to delete a page that is on disk.
	 * This routine must call the method in diskmgr package to
	 * deallocate the page.
	 *
	 * @param globalPageId the page number in the data base.
	 * @throws ChainException 
	 */

	public void freePage(PageId globalPageId)throws ChainException{
		int frameNum = _pageHashtable.getPageFrame(globalPageId.pid);
		
		if(frameNum != Constants.NO_FRAME_FOUND && _arrFrameDescriptor[frameNum].getPinCount() > 1){
			throw new PagePinnedException(null, "Freeing page which is pinned");
		}
		
		try {
			if(frameNum != Constants.NO_FRAME_FOUND){
				if(_arrFrameDescriptor[frameNum].getPinCount() == 1){
					// Unpin.
					unpinPage(globalPageId, false);
				}else{
					// Do nothing.
				}				
			}
			
			try {
				SystemDefs.JavabaseDB.deallocate_page(globalPageId);
			} catch (InvalidRunSizeException e) {
				throw new BufException(null, "Error when trying to free page - invalid run size");
			} catch (InvalidPageNumberException e) {
				throw new BufException(null, "Error when trying to free page - invalid page number");
			} catch (FileIOException e) {
				throw new BufException(null, "Error when trying to free page - FileIOException");
			}
			
//			if(frameNum != Constants.NO_FRAME_FOUND){
//				_arrFrameDescriptor[frameNum] = new FrameDescriptor();
//				_pageHashtable.removeMappingForPage(globalPageId.pid);
//			}
		} catch (IOException e) {
			throw new BufException(null, "Error when trying to free page - IOException");
		}	
	}
	
	/** Gets the total number of buffers.
	 *
	 * @return total number of buffer frames.
	 */
	public int getNumBuffers() {
		return _numBufs;
	}

	/** Gets the total number of unpinned buffer frames.
	 *
	 * @return total number of unpinned buffer frames.
	 */
	public int getNumUnpinnedBuffers() {
		int numUnpinnedBuffers = 0;
		
		for(int arrFrameDescriptorOffset = 0; arrFrameDescriptorOffset < _arrFrameDescriptor.length; 
		arrFrameDescriptorOffset++){
			if(_arrFrameDescriptor[arrFrameDescriptorOffset].getPinCount() == 0){
				numUnpinnedBuffers++;
			}
		}
		
		return numUnpinnedBuffers;
	}

	/*
	 * Print out the hashtable. To be used for debugging purposes.
	 */
	public void dumpHashTable(){
		_pageHashtable.prettyPrint();
	}
}
