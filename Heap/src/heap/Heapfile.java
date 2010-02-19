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
					(SystemDefs.JavabaseBM).pinPage(pageDirectoryPageId, pageDirectoryPage, false);
					
					_directoryPage = new DirectoryPage(pageDirectoryPage);
					
					// Format directory page.
					_directoryPage.formatDirectory();
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
			System.out.println(pageDirectoryEntry.getPageId() + " ; " + pageDirectoryEntry.getEntryType());
			
			pageDirectoryEntry = new PageDirectoryEntry(5, 10, 2);
			_directoryPage.writePageDirectoryEntry(pageDirectoryEntry, 3);
			
			pageDirectoryEntry = _directoryPage.getPageDirectoryEntry(3);			
			System.out.println(pageDirectoryEntry.getPageId() + " ; " + pageDirectoryEntry.getEntryType());
			
			pageDirectoryEntry = _directoryPage.getPageDirectoryEntry(5);			
			System.out.println(pageDirectoryEntry.getPageId() + " ; " + pageDirectoryEntry.getEntryType());
		}else{
			throw new ChainException(null, "DB object not created.");
		}
	}
	
	/**
	 * It is assumed that the database has already been selected and that the
	 * disk manager and buffer manager modules are properly initialized.
	 * 
	 * @param fname Heap file name.
	 */
	public Heapfile(String fname, boolean bDummy) throws ChainException, IOException{
		// Read in file directory page.
		PageId fileDirectoryPageId = new PageId();
		Page fileDirectoryPage = new Page();
		Hashtable<String, Integer> htFileDirectory = null;
		
		try{
			// Pin the file-directory page
			fileDirectoryPageId.pid = Constants.FILE_DIR_PAGEID;
			(SystemDefs.JavabaseBM).pinPage(fileDirectoryPageId, fileDirectoryPage, false);
		}catch(Exception exception){
			throw new ChainException(exception, "Unable to access file directory."); 
		}
		
		if(fileDirectoryPage != null){
			// Check if header is present.
			byte[] data = fileDirectoryPage.getpage();
			boolean bHeaderMatch = true;
			
			for(int fileHeaderOffset = 0; fileHeaderOffset < Constants.FILE_DIR_HEADER.length();
				fileHeaderOffset++){
				if(Constants.FILE_DIR_HEADER.charAt(fileHeaderOffset) != (char)data[fileHeaderOffset]){
					bHeaderMatch = false;
				}
			}
			
			if(bHeaderMatch == false){
				// Header not found. This means that there is no file directory page.
				// This would be a good time to create one.
				for(int fileHeaderOffset = 0; fileHeaderOffset < Constants.FILE_DIR_HEADER.length();
					fileHeaderOffset++){
					data[fileHeaderOffset] = (byte)Constants.FILE_DIR_HEADER.charAt(fileHeaderOffset);
				}
				
				// Create a hashtable and lay it out on the page.
				htFileDirectory = new Hashtable<String, Integer>();
				
				// Find a new page for our page directory.
				Page pageDirectoryPage = new Page();
				PageId pageDirectoryPageId = (SystemDefs.JavabaseBM).newPage(pageDirectoryPage, 1);
				_firstPgDirPID = pageDirectoryPageId.pid;
				
				htFileDirectory.put(fname, pageDirectoryPageId.pid);
				
				// Write htFileDirectory.
				byte[] serializedHTFileDirectory = getSerializedForm(htFileDirectory);
				
				System.arraycopy(serializedHTFileDirectory, 0, data, Constants.FILE_DIR_HEADER.length(), 
						serializedHTFileDirectory.length);
			}else{
				// Header found. So the file directory does indeed exist. Read and update it (if required).
				// Read the serialized form of the hashtable size to figure out how big an array to create to
				// read in the table.
				byte[] serializedHTFileDirectory = new byte[data.length - Constants.FILE_DIR_HEADER.length()];
				
				System.arraycopy(data, Constants.FILE_DIR_HEADER.length(), serializedHTFileDirectory, 0, 
						serializedHTFileDirectory.length);
				
				htFileDirectory = (Hashtable<String, Integer>)getDeserializedForm(serializedHTFileDirectory);

				// Find a new page for our page directory.
				Page pageDirectoryPage = new Page();
				PageId pageDirectoryPageId = (SystemDefs.JavabaseBM).newPage(pageDirectoryPage, 1);				
				
				if(htFileDirectory.containsKey(fname) == false){
					_firstPgDirPID = pageDirectoryPageId.pid;
					htFileDirectory.put(fname, pageDirectoryPageId.pid);
				}else{
					_firstPgDirPID = htFileDirectory.get(fname);
				}
				
				prettyPrintFileDirectory(htFileDirectory);
			}
			
			// Unpin file directory page.
			(SystemDefs.JavabaseBM).unpinPage(fileDirectoryPageId, true);
//			(SystemDefs.JavabaseBM).unpinPage(fileDirectoryPageId, true);
			
			// Flush page.			
			(SystemDefs.JavabaseBM).flushPage(fileDirectoryPageId);
		}else{
			throw new ChainException(null, "Unable to get file directory page.");
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
	
	/**
	 * Returns the serialized form of object.
	 * 
	 * @param object Object to serialize
	 * @return Serialized form of object. Null returned in case of an exception.
	 */
	private byte[] getSerializedForm(Object object){
		byte[] serializedData = null;
		
		try{
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			
			objectOutputStream.writeObject(object);
			objectOutputStream.flush();
			byteArrayOutputStream.flush();
			
			serializedData = byteArrayOutputStream.toByteArray();
			
			// Close streams.
			objectOutputStream.close();
			byteArrayOutputStream.close();
		}catch(Exception exception){
			// Do nothing.
		}
		
		return serializedData;
	}
	
	/**
	 * Returns the deserialized form of data.
	 * 
	 * @param data Data to deserialize.
	 * @return Deserialized form of data.
	 */
	private Object getDeserializedForm(byte[] data){
		Object object = null;
		
		try{
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
			ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
			
			object = objectInputStream.readObject();
			
			// Close streams.
			objectInputStream.close();
			byteArrayInputStream.close();
		}catch(Exception exception){
			// Do nothing.
		}
		
		return object;
	}
}
