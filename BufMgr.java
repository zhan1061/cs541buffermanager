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
	PageId _pageNumber = null;
	int _pinCount;
	boolean _bDirty;
	
	public FrameDescriptor(){
		// Initialize members
		_pageNumber = new PageId();
		_pageNumber.pid = Constants.NO_PAGE;
		_pinCount = 0;
		_bDirty = false;
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
	DB _db = null;
	String _dbName = "singh47_db";

	public BufMgr(int numBufs, String replacer) throws ChainException{
		_pool = new byte[numBufs][GlobalConst.MINIBASE_PAGESIZE];
		_replacer = replacer;
		_arrFrameDescriptor = new FrameDescriptor[numBufs];

		for(int frameDescriptorOffset = 0; frameDescriptorOffset < _arrFrameDescriptor.length; frameDescriptorOffset++){
			_arrFrameDescriptor[frameDescriptorOffset] = new FrameDescriptor();
		}

		_db = new DB();
	
		try{
			_db.openDB(_dbName, GlobalConst.MINIBASE_DB_SIZE);
		}catch(Exception exception){
			throw new ChainException(exception, "Exception occurred while opening " + _dbName);
		}
	}

	public PageId newPage(Page firstPage, int howmany) throws ChainException{
		if(howmany <= 0){
			// Invalid parameters.
			throw new InvalidRunSizeException(null, "Invalid run size specified.");
		}

		if(firstPage == null){
			throw new InvalidPageAddressException(null, "Invalid first page address supplied.");
		}

		if(!isBufferFull()){
			// Find a frame.

			// Try to allocate.
			return null;
		}else{
			// Buffer full.
			return null;
		}
	}

	public void pinPage(PageId pinPgId, Page page, boolean emptyPage){
		// TODO: Add logic for pinPage
	}

	public void unpinPage(PageId PageId_in_a_DB, boolean dirty) {};

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
}
