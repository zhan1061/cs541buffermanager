package heap;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;

public class Heapfile {
	private int _firstPgDirPID = Constants.INVALID_PID; 
	private DirectoryPage _directoryPage = null;
	
	public Heapfile(String fname) throws ChainException, IOException{
		if(SystemDefs.JavabaseDB != null){
			// Check if the relevant file entry exists. If it
			// does, set _firstPgDirPID to the corresponding
			// page ID. Otherwise, create a new file entry and
			// assign the returned page ID to _firstPgDirPID.
			PageId fileHeaderPageId = SystemDefs.JavabaseDB.get_file_entry(fname);
			
			if(fileHeaderPageId == null){
				// File entry doesn't exist. Create a page directory header
				// for this file and add the entry to the database.
				PageId pageDirectoryPageId = new PageId();
				Page pageDirectoryPage = new Page();
				
				pageDirectoryPageId = SystemDefs.JavabaseBM.newPage(pageDirectoryPage, 1);
				
				if(pageDirectoryPageId != null){
					_firstPgDirPID = pageDirectoryPageId.pid;
					
					// This directory page is a blank slate. Format it.
					_directoryPage = new DirectoryPage(pageDirectoryPage);
					
					// Format directory page.
					_directoryPage.formatDirectory();
					
					// Unpin page.
					SystemDefs.JavabaseBM.unpinPage(pageDirectoryPageId, true);
				}else{
					throw new ChainException(null, "Call to newPage for page directory failed.");
				}
			}else{
				// File entry found.
				_firstPgDirPID = fileHeaderPageId.pid;
				Page pageDirectoryPage = new Page();
				PageId pageDirectoryPageId = new PageId();
				pageDirectoryPageId.pid = _firstPgDirPID;
				
				(SystemDefs.JavabaseBM).pinPage(pageDirectoryPageId, pageDirectoryPage, false);
				
				_directoryPage = new DirectoryPage(pageDirectoryPage);
			}
			
			PageDirectoryEntry pageDirectoryEntry = _directoryPage.getPageDirectoryEntry(5);			
			System.out.println(pageDirectoryEntry.getPID() + " ; " + pageDirectoryEntry.getEntryType());
			
			pageDirectoryEntry = new PageDirectoryEntry(5, 10, 2);
			_directoryPage.writePageDirectoryEntry(pageDirectoryEntry, 3);
			
			pageDirectoryEntry = _directoryPage.getPageDirectoryEntry(3);			
			System.out.println(pageDirectoryEntry.getPID() + " ; " + pageDirectoryEntry.getEntryType());
			
			pageDirectoryEntry = _directoryPage.getPageDirectoryEntry(5);			
			System.out.println(pageDirectoryEntry.getPID() + " ; " + pageDirectoryEntry.getEntryType());
		}else{
			throw new ChainException(null, "DB object not created.");
		}
	}
	
	/**
	 * Returns the page directory PID.
	 * @return PID of the page directory.
	 */
	public int getPageDirectoryPID(){
		return _firstPgDirPID;
	}
	
	/**
	 * Pretty prints file directory.
	 * @param htFileDirectory
	 */
	private void prettyPrintFileDirectory(Hashtable<String, Integer> htFileDirectory){
		Enumeration<String> keys = htFileDirectory.keys();
		
		while(keys.hasMoreElements()){
			String filename = keys.nextElement();
			int directoryPID = htFileDirectory.get(filename);
			
			System.out.println(filename + " => " + directoryPID);
		}
	}
	
	public RID insertRecord(byte[] recordData) throws ChainException{
		RID rid = new RID();
		PageId hfPageId = getAvailableHFPageId(recordData.length);
		Page page = new Page();
		
		// Pin page
		try {
			SystemDefs.JavabaseBM.pinPage(hfPageId, page, false);

			// Create HFPage from page.
			HFPage hfPage = new HFPage(page);
			rid = hfPage.insertRecord(recordData);
			
			// Update directory entry corresponding to hfPage with new available space.
			updateHFPageAvailableSpace(hfPageId, hfPage.available_space());
			
			SystemDefs.JavabaseBM.unpinPage(hfPageId, true);
		} catch (Exception exception) {
			throw new ChainException(exception, "Unable to pin page.");
		}
		
		return rid;
	}
	
	private void updateHFPageAvailableSpace(PageId hfPageId, int availableSpace) throws ChainException{
		int currPgDirPID = _firstPgDirPID;
		
		// Start search loop.
		while(currPgDirPID != GlobalConst.INVALID_PAGE){
			// Pin page with PID currPgDirPID.
			Page currPage = new Page();
			PageId currPageId = new PageId(currPgDirPID);
			
			try{
				SystemDefs.JavabaseBM.pinPage(currPageId, currPage, false);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to pin directory page.");
			}
			
			DirectoryPage currDirPage = new DirectoryPage(currPage);
			
			if(currDirPage.updateHFPageAvailableSpace(hfPageId, availableSpace)){
				// Space updated.
				try{
					SystemDefs.JavabaseBM.unpinPage(currPageId, true);
				}catch(Exception exception){
					throw new ChainException(exception, "Unable to pin directory page.");
				}
				
				return;
			}
			
			// Next directory.
			currPgDirPID = currDirPage.getNextDirectory().getPID();
			
			try{
				SystemDefs.JavabaseBM.unpinPage(currPageId, true);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to pin directory page.");
			}
		}
	}
	
	public Tuple getRecord(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception{
		PageId curDirPageId = new PageId(_firstPgDirPID);
		Page currPage = new Page();
		DirectoryPage curDirPage = null;
		boolean exists = false;
		while(!exists && curDirPageId.pid!= GlobalConst.INVALID_PAGE){
			SystemDefs.JavabaseBM.pinPage(curDirPageId, currPage, false);
			curDirPage = new DirectoryPage(currPage); 
			
			for(int i=0; i<curDirPage._totalEntries; i++)
			{
				PageDirectoryEntry dirEntry = null;
				dirEntry = curDirPage.getPageDirectoryEntry(i);
				PageId tmp = new PageId(dirEntry.getPID());
				if (tmp.pid == rid.pageNo.pid)
				{
					exists = true;
					break;
				}
			}
			
			if(!exists){
				SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
				PageId tmp1 = new PageId(curDirPage.getNextDirectory().getPID());
				curDirPageId.pid = tmp1.pid;
				
			}
		}//end of while loop
		if(!exists){
			return null;
		}

		HFPage hfDataPage = new HFPage();
		SystemDefs.JavabaseBM.pinPage(rid.pageNo, hfDataPage, false);
		Tuple data = hfDataPage.getRecord(rid);
		SystemDefs.JavabaseBM.unpinPage(rid.pageNo, false);
		SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
		return data;
	}

	
	
	
	/**
	 * Returns PageId for the HFPage that has 'recordLength' space in it.
	 * @param recordLength
	 * @return PageId for the HFPage that has 'recordLength' space in it.
	 * @throws ChainException
	 */
	private PageId getAvailableHFPageId(int recordLength) throws ChainException{
		PageId availableHFPageId = null;
		int currPgDirPID = _firstPgDirPID;
		
		// Start search loop.
		while(true){
			// Pin page with PID currPgDirPID.
			Page currPage = new Page();
			PageId currPageId = new PageId(currPgDirPID);
			
			try{
				SystemDefs.JavabaseBM.pinPage(currPageId, currPage, false);
			}catch(Exception exception){
				throw new ChainException(exception, "Unable to pin directory page.");
			}
			
			DirectoryPage currDirPage = new DirectoryPage(currPage);
			
			// Search within currDirPage for available HFPage
			if((availableHFPageId = currDirPage.getPageWithSpaceForRecord(recordLength)) != null){
				// Unpin the current directory page. The page may have been modified.
				SystemDefs.JavabaseBM.unpinPage(currPageId, true);
				
				return availableHFPageId;
			}else{
				// No space available in this directory. Use the next directory.
				currPgDirPID = currDirPage.getNextDirectory().getPID();
				
				// Check if this is a valid PID.
				if(currPgDirPID == GlobalConst.INVALID_PAGE){
					// No directory entry found here. Create a new
					// directory page and set it as the next directory
					// of the current directory.
					PageId newPageDirectoryPageId = new PageId();
					Page newPageDirectoryPage = new Page();
					
					try{
						newPageDirectoryPageId = SystemDefs.JavabaseBM.newPage(newPageDirectoryPage, 1);

						if(newPageDirectoryPageId != null){
							// This directory page is a blank slate. Format it.
							DirectoryPage newDirPage = new DirectoryPage(newPageDirectoryPage);

							// Format directory page.
							newDirPage.formatDirectory();

							// Set this as the next directory entry.
							currDirPage.setNextDirectory(
									new PageDirectoryEntry(newPageDirectoryPageId.pid, 0, 
											PageDirectoryEntry.DIRECTORYPAGE_ENTRY));
							
							// Unpin new and current directory pages.
							SystemDefs.JavabaseBM.unpinPage(newPageDirectoryPageId, true);
							SystemDefs.JavabaseBM.unpinPage(new PageId(currPgDirPID), true);
							
							currPgDirPID = newPageDirectoryPageId.pid;
						}else{
							throw new ChainException(null, "Unable to allocate new page.");
						}
					}catch(Exception exception){
						throw new ChainException(exception, "Unable to create new directory page.");
					}
				}else{
					SystemDefs.JavabaseBM.unpinPage(new PageId(currPgDirPID), true);
				}
			}
		}
	}
}
