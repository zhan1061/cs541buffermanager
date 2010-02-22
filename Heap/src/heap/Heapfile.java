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
	private String fileName;
	
	public Heapfile(String fname) throws ChainException, IOException{
		this.fileName = fname;
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
					
					// Add new directory entry.
					try{
						SystemDefs.JavabaseDB.add_file_entry(fname, new PageId(_firstPgDirPID));
					}catch(Exception exception){
						throw new ChainException(exception, "Unable to add file entry to database.");
					}
				}else{
					throw new ChainException(null, "Call to newPage for page directory failed.");
				}
			}else{
				// File entry found.
				_firstPgDirPID = fileHeaderPageId.pid;
//				Page pageDirectoryPage = new Page();
//				PageId pageDirectoryPageId = new PageId();
//				pageDirectoryPageId.pid = _firstPgDirPID;
				
//				(SystemDefs.JavabaseBM).pinPage(pageDirectoryPageId, pageDirectoryPage, false);
//				
//				_directoryPage = new DirectoryPage(pageDirectoryPage);
			}			
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
	
	public Scan openScan() throws IOException, InvalidTupleSizeException, ChainException{
		Scan scan = new Scan(this);
		
		return scan;
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
		
//		try{
//			SystemDefs.JavabaseBM.flushAllPages();
//		}catch(Exception exception){
//			throw new ChainException(exception, "Couldn't flush pages.");
//		}
		
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
				
//				try{
//					SystemDefs.JavabaseBM.flushAllPages();
//				}catch(Exception exception){
//					throw new ChainException(exception, "Couldn't flush pages.");
//				}
				
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
		PageDirectoryEntry dirEntry = null;
		while(!exists && curDirPageId.pid!= GlobalConst.INVALID_PAGE){
			try{
				SystemDefs.JavabaseBM.pinPage(curDirPageId, currPage, false);
			}
			catch(Exception e){
				throw new HFBufMgrException(e,"Pin Page Failed");
			}
			curDirPage = new DirectoryPage(currPage); 
			
			for(int i=0; i<curDirPage.getTotalEntries(); i++)
			{
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
		SystemDefs.JavabaseBM.pinPage(new PageId(dirEntry.getPID()), hfDataPage, false);
		Tuple data = hfDataPage.getRecord(rid);
		SystemDefs.JavabaseBM.unpinPage(new PageId(dirEntry.getPID()), false);
		SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
		return data;
	}

	public int getRecCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		int totalRecords = 0;
		
		int currPgDirPID = getPageDirectoryPID();

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
				HFPage hfPage = new HFPage();

				if(pageDirectoryEntry.getPID() != GlobalConst.INVALID_PAGE){
					try{
						SystemDefs.JavabaseBM.pinPage(new PageId(pageDirectoryEntry.getPID()), hfPage, false);

						// Read the HFPage to get the number of records.
						for(int slotNumber = 0; slotNumber < hfPage.getSlotCnt(); slotNumber++){
							if(hfPage.getSlotOffset(slotNumber) != GlobalConst.INVALID_PAGE){
								totalRecords++;
							}
						}
						
						// Unpin HFPage
						SystemDefs.JavabaseBM.unpinPage(new PageId(pageDirectoryEntry.getPID()), false);
					}catch(Exception exception){
						throw new HFBufMgrException(exception, "Unable to pin/unpin heapfile page.");
					}
				}
			}

			// Next directory.
			currPgDirPID = currDirPage.getNextDirectory().getPID();

			try{
				SystemDefs.JavabaseBM.unpinPage(currPageId, true);
			}catch(Exception exception){
				throw new IOException("Unable to unpin directory page.");
			}
		}
		
		return totalRecords;
	}
	
	public boolean updateRecord(RID rid, Tuple newTuple)throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		PageId curDirPageId = new PageId(_firstPgDirPID);
		Page currPage = new Page();
		DirectoryPage curDirPage = null;
		boolean exists = false;
		PageDirectoryEntry dirEntry = null;
		while(!exists && curDirPageId.pid!= GlobalConst.INVALID_PAGE){
			try{
				SystemDefs.JavabaseBM.pinPage(curDirPageId, currPage, false);
			}
			catch(Exception e){
				throw new HFBufMgrException(e,"Pin Page Failed in updateRecord");
			}
			curDirPage = new DirectoryPage(currPage); 
			
			for(int i=0; i<curDirPage.getTotalEntries(); i++)
			{
				
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
			return false;
		}

		HFPage hfDataPage = new HFPage();
		SystemDefs.JavabaseBM.pinPage(new PageId(dirEntry.getPID()), hfDataPage, false);
		Tuple data = hfDataPage.getRecord(rid);
		
		if(data.getLength() == newTuple.getLength()){
			data.tupleCopy(newTuple);
			try{
				SystemDefs.JavabaseBM.unpinPage(new PageId(dirEntry.getPID()), true);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex, "Failed while unpinning page in updateRecord");
			}
			try{
			SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex, "Failed while unpinning page in updateRecord");
			}
		}
		else{
			try{
				SystemDefs.JavabaseBM.unpinPage(new PageId(dirEntry.getPID()), true);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex, "Failed while unpinning page in updateRecord");
			}
			try{
			SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex, "Failed while unpinning page in updateRecord");
			}
			throw new InvalidUpdateException(null, "Invalid Record Update - record not updated");
		}
		return true;
	}

	
	public boolean deleteRecord (RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException,Exception
	{
		PageId curDirPageId = new PageId(_firstPgDirPID);
		Page currPage = new Page();
		DirectoryPage curDirPage = null;
		boolean exists = false;
		PageDirectoryEntry dirEntry = null;
		
		while(!exists && curDirPageId.pid != GlobalConst.INVALID_PAGE){
			try{
				SystemDefs.JavabaseBM.pinPage(curDirPageId, currPage, false);
			}
			catch(Exception e){
				throw new HFBufMgrException(e,"Pin Page Failed in deleteRecord");
			}
			curDirPage = new DirectoryPage(currPage); 
			
			for(int i=0; i<curDirPage.getTotalEntries(); i++)
			{
				dirEntry = curDirPage.getPageDirectoryEntry(i);
				PageId tmp = new PageId(dirEntry.getPID());
				if (tmp.pid == rid.pageNo.pid)
				{
					exists = true;
					break;
				}
			}
			
			if(!exists){
				try{
					SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
				}
				catch(Exception ex){
					throw new HFBufMgrException(ex, "Unpin Page Failed in deleteRecord");
				}
				PageId tmp1 = new PageId(curDirPage.getNextDirectory().getPID());
				curDirPageId.pid = tmp1.pid;
				
			}
			
		}
		if(!exists){
			return false;
		}
		
		
		HFPage hfDataPage = new HFPage();
		try{
			SystemDefs.JavabaseBM.pinPage(new PageId(dirEntry.getPID()), hfDataPage, false);
		}
		catch(Exception e){
			try{
				SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex, "Failed while unpinning page in deleteRecord");
			}
			throw new HFBufMgrException(e, "Failed pinning page in deleteRecord");
		}
		hfDataPage.deleteRecord(rid);
		dirEntry.setAvailableSpace(hfDataPage.available_space());
		//dec recct by 1
		
		try{
			SystemDefs.JavabaseBM.unpinPage(new PageId(dirEntry.getPID()), true);
		}
		catch(Exception e){
			try{
				SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex, "Failed while unpinning page in deleteRecord");
			}
			
		}

		//delete the page itself if it is empty now aft deleting the record - should i do this before unpinning????
		if(hfDataPage.empty()){
			try{
				SystemDefs.JavabaseBM.freePage(rid.pageNo);
			}
			catch(Exception e){
				try{
					SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
				}
				catch(Exception ex){
					throw new HFBufMgrException(ex, "Failed while unpinning page in deleteRecord");
				}
			}
			
			//remove this from  directory page also
			//this fn does not exist as of now!!!
			//dirEntry.deleteEntry();
		}
        
		//handle the case if the directory page itself becomes empty
//		if(curDirPage.empty()){//implement empty() in DirectoryPage
//			
//		} 
		
		else{
			try{
				SystemDefs.JavabaseBM.unpinPage(curDirPageId, true);
			}
			catch(Exception ex){
				throw new HFBufMgrException(ex,"Unpin page failed in deleteRecord");
			}
		}
		return true;
	} //end of deleteRecord()
	
	/*---------------------------------------------------*/
	public void deleteFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException
	{
		PageId curDirPageId = new PageId(_firstPgDirPID);
		Page currPage = new Page();
		DirectoryPage curDirPage = null;
		boolean exists = false;
		PageDirectoryEntry dirEntry = null;
		PageId nextDirPageId = new PageId();
		
		while(!exists && curDirPageId.pid != GlobalConst.INVALID_PAGE){
			try{
				SystemDefs.JavabaseBM.pinPage(curDirPageId, currPage, false);
			}
			catch(Exception e){
				throw new HFBufMgrException(e,"Pin Page Failed in deleteFile");
			}
			curDirPage = new DirectoryPage(currPage); 
			
			for(int i=0; i<curDirPage.getTotalEntries(); i++)
			{
				dirEntry = curDirPage.getPageDirectoryEntry(i);
//				PageId tmp = new PageId(dirEntry.getPID());
//				if (tmp.pid == rid.pageNo.pid)
//				{
//					exists = true;
//					break;
//				}
				try {
					SystemDefs.JavabaseBM.freePage(new PageId(dirEntry.getPID()));
				} catch (Exception e) {
					throw new HFBufMgrException(e,"Free Page Failed during file delete");
				} 
				
			}
			
			PageId tmp1 = new PageId(curDirPage.getNextDirectory().getPID());
			nextDirPageId.pid = tmp1.pid;
			try{
					SystemDefs.JavabaseBM.unpinPage(curDirPageId, false);
			}
				catch(Exception ex){
					throw new HFBufMgrException(ex, "Unpin Page Failed in deleteFile");
				}
			try{
				
				SystemDefs.JavabaseBM.freePage(curDirPageId);
			}
			catch(Exception e){
				throw new HFBufMgrException(e,"Free Page Failed for directory page in deleteFile");
			}
			curDirPageId = nextDirPageId;
		}
		try {
			SystemDefs.JavabaseDB.delete_file_entry(fileName);
		} catch (Exception e) {
			throw new HFDiskMgrException(e, "Error deleting file entry");
		}
	}
/* --------------------------------------------------*/
	
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
				int oldPgDirPID = currPgDirPID;
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
							SystemDefs.JavabaseBM.unpinPage(new PageId(oldPgDirPID), true);
							
							currPgDirPID = newPageDirectoryPageId.pid;
						}else{
							throw new ChainException(null, "Unable to allocate new page.");
						}
					}catch(Exception exception){
						throw new ChainException(exception, "Unable to create new directory page.");
					}
				}else{
					SystemDefs.JavabaseBM.unpinPage(new PageId(oldPgDirPID), true);
				}
			}
		}
	}
}
