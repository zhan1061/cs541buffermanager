package heap;

import java.io.Serializable;

import global.*;
import diskmgr.*;

public class PageDirectoryEntry implements Serializable{
	public static int HFPAGE_ENTRY = 1;
	public static int DIRECTORYPAGE_ENTRY = 2;
	
	private int _pid;
	private int _availableSpace;
	private int _entryType;
	
	public PageDirectoryEntry(){
		// Dummy constructor.		
	}
	
	public PageDirectoryEntry(int pid, int availableSpace, int entryType){
		_pid = pid;
		_availableSpace = availableSpace;
		_entryType = entryType;
	}
	
	public int getPageId(){
		return _pid;
	}
	
	public int getAvailableSpace(){
		return _availableSpace;
	}
	
	public int getEntryType(){
		return _entryType;
	}
}
