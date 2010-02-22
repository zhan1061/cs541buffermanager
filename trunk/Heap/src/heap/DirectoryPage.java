package heap;

import chainexception.ChainException;
import global.PageId;
import global.SystemDefs;
import diskmgr.Page;

public class DirectoryPage extends Page {
	// Data has been inherited.
	private int _directoryEntrySize = 0;
	private int _totalEntries = 0;
	
	/**
	 * Sets data to be the same as page's data.
	 */
	public DirectoryPage(Page page){
		data = page.getpage();
		_directoryEntrySize = getDirectoryEntrySize(new PageDirectoryEntry());
		_totalEntries = (SystemDefs.JavabaseDB).db_page_size() / _directoryEntrySize;
	}
	
	/**
	 * Formats the directory page and initializes
	 * all directory entries.
	 */
	public void formatDirectory(){
		// Layout directory entries.
		PageDirectoryEntry initHFPageDirectoryEntry = new PageDirectoryEntry(
				new PageId(INVALID_PAGE).pid,
				0, PageDirectoryEntry.HFPAGE_ENTRY);
		PageDirectoryEntry initDirectoryPageDirectoryEntry = new PageDirectoryEntry(
				new PageId(INVALID_PAGE).pid,
				0, PageDirectoryEntry.DIRECTORYPAGE_ENTRY);
		byte[] serializedHFPageDirectoryEntry = SerializationUtil.getSerializedForm(initHFPageDirectoryEntry);
		byte[] serializedDirectoryPageDirectoryEntry = SerializationUtil.getSerializedForm(initDirectoryPageDirectoryEntry);
		
//		System.out.println("About to write " + _totalEntries + " entries to directory.");
		
		// Save the last entry for a pointer to the next directory page.
		for(int entryOffset = 0; entryOffset < _totalEntries - 1; entryOffset++){
			System.arraycopy(serializedHFPageDirectoryEntry, 0, data, entryOffset * _directoryEntrySize, 
					_directoryEntrySize);			
		}
		
		System.arraycopy(serializedDirectoryPageDirectoryEntry, 0, data, (_totalEntries - 1) * _directoryEntrySize, 
				_directoryEntrySize);
	}
	
	public int getTotalEntries(){
		return _totalEntries;
	}
	
	/**
	 * Get page directory entry at offset.
	 * @param offset
	 * @return PageDirectoryEntry at offset.
	 */
	public PageDirectoryEntry getPageDirectoryEntry(int offset){
		PageDirectoryEntry pageDirectoryEntry = null;
		
		if(offset >= 0 && offset < _totalEntries){
			byte[] pageDirectoryEntryData = new byte[_directoryEntrySize];
			
			// Get the right slice of data.
			System.arraycopy(data, offset * _directoryEntrySize, pageDirectoryEntryData, 0, _directoryEntrySize);
			
			// Deserialize the data itself.
			pageDirectoryEntry = (PageDirectoryEntry)SerializationUtil.getDeserializedForm(pageDirectoryEntryData);
		}
		
		return pageDirectoryEntry;
	}
	
	/**
	 * Returns the page directory entry corresponding to
	 * the next directory page.
	 * @return
	 */
	public PageDirectoryEntry getNextDirectory(){
		PageDirectoryEntry nextPageDirectoryEntry = null;
		
		nextPageDirectoryEntry = getPageDirectoryEntry(_totalEntries - 1);
		
		return nextPageDirectoryEntry;
	}
	
	/**
	 * Sets an entry pointing to the next directory page.
	 * @param nextDirectoryEntry
	 * @throws ChainException
	 */
	public void setNextDirectory(PageDirectoryEntry nextDirectoryEntry) throws ChainException{
		writePageDirectoryEntry(nextDirectoryEntry, _totalEntries - 1);
	}
	
	/**
	 * Write page directory entry at offset.
	 * @param pageDirectoryEntry
	 * @param offset
	 */
	public void writePageDirectoryEntry(PageDirectoryEntry pageDirectoryEntry, int offset) throws ChainException{
		if(offset >= 0 && offset < _totalEntries){
			byte[] pageDirectoryEntryData = SerializationUtil.getSerializedForm(pageDirectoryEntry);
			
			System.arraycopy(pageDirectoryEntryData, 0, data, offset * _directoryEntrySize, pageDirectoryEntryData.length);
		}else{
			throw new ChainException(null, "Invalid directory offset: " + offset);
		}
	}
	
	/**
	 * Returns the size of a page directory entry.
	 * @param pageDirectoryEntry
	 * @return Size of pageDirectoryEntry 
	 */
	private int getDirectoryEntrySize(PageDirectoryEntry pageDirectoryEntry){
		int entrySize = 0;
		
		entrySize = SerializationUtil.getSerializedForm(pageDirectoryEntry).length;
		
		return entrySize;
	}

	/**
	 * Updates space for page corresponding to hfPageId. Returns true if
	 * update is successful. Otherwise returns false.
	 * @param hfPageId
	 * @param availableSpace
	 * @return
	 */
	public boolean updateHFPageAvailableSpace(PageId hfPageId, int availableSpace) throws ChainException{
		int offset = 0;
		
		for(offset = 0; offset < _totalEntries - 1; offset++){
			// Get page directory entry at this offset.
			PageDirectoryEntry pageDirectoryEntry = getPageDirectoryEntry(offset);
			
			if(pageDirectoryEntry.getPID() == hfPageId.pid){
				pageDirectoryEntry.setAvailableSpace(availableSpace);
				
				// Write pageDirectoryEntry back.
				writePageDirectoryEntry(pageDirectoryEntry, offset);
				
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Returns PageId of the page with space to store a record of size
	 * recordLength. Also takes into account the amount of space
	 * needed to store slot details.
	 * @param recordLength
	 * @return
	 */
	public PageId getPageWithSpaceForRecord(int recordLength) throws ChainException{
		int offset = 0;
		
		for(offset = 0; offset < _totalEntries - 1; offset++){
			// Get page directory entry at this offset.
			PageDirectoryEntry pageDirectoryEntry = getPageDirectoryEntry(offset);
			
			if(pageDirectoryEntry.getPID() != INVALID_PAGE){
				// Pull page with this PID into the buffer pool.
				PageId hfPageId = new PageId(pageDirectoryEntry.getPID());
				Page foundPage = new Page();
				
				try{
					(SystemDefs.JavabaseBM).pinPage(hfPageId, foundPage, false);
					
					// Page pinned.
					HFPage foundHFPage = new HFPage(foundPage);
					
					// Check available space. If enough, return hfPageId.
					if(foundHFPage.available_space() >= recordLength + HFPage.SIZE_OF_SLOT){
						// Unpin page.
						(SystemDefs.JavabaseBM).unpinPage(hfPageId, false);
						
						return hfPageId;
					}else{
						// Unpin page.
						(SystemDefs.JavabaseBM).unpinPage(hfPageId, false);
					}
				}catch(Exception exception){
					throw new ChainException(exception, "Unable to pin HFPage.");
				}
			}
		}
		
		// We haven't yet found a valid HFPage with enough space available in it.
		// Look for a page directory entry that doesn't have an HFPage assigned to it.
		for(offset = 0; offset < _totalEntries - 1; offset++){
			// Get page directory entry at this offset.
			PageDirectoryEntry pageDirectoryEntry = getPageDirectoryEntry(offset);
			
			if(pageDirectoryEntry.getPID() == INVALID_PAGE){
				// Create new HFPage. Return its ID.
				PageId newPageId = new PageId();
				Page newPage = new Page();
				
				try{
					newPageId = (SystemDefs.JavabaseBM).newPage(newPage, 1);
					HFPage newHFPage = new HFPage(newPage);
					
					newHFPage.init(newPageId, newPage);
					pageDirectoryEntry.setPID(newPageId.pid);
					pageDirectoryEntry.setAvailableSpace(newHFPage.available_space());					
					
					// Write pageDirectoryEntry
					writePageDirectoryEntry(pageDirectoryEntry, offset);
					
					// Unpin the page.
					(SystemDefs.JavabaseBM).unpinPage(newPageId, true);
					
					return newPageId;
				}catch(Exception exception){
					throw new ChainException(exception, "Unable to create/init new HFPage.");
				}
			}
		}
		
		return null;
	}
}
