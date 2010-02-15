package bufmgr;

import java.util.ArrayList;

import chainexception.ChainException;

import diskmgr.DuplicateEntryException;

class PageFramePair{
	int _page;
	int _frame;

	public PageFramePair(int page, int frame){
		_page = page;
		_frame = frame;
	}

	public int getPage(){
		return _page;
	}

	public int getFrame(){
		return _frame;
	}

	public void setFrame(int frame){
		_frame = frame;
	}
}

public class PageHashtable{

	ArrayList[] _arrPairList = null;
	int _tableSize = 0;

	public PageHashtable(int tableSize){
		_arrPairList = new ArrayList[tableSize];
		_tableSize = tableSize;

		for(int pairListOffset = 0; pairListOffset < _arrPairList.length; pairListOffset++){
			_arrPairList[pairListOffset] = new ArrayList();
		}
	}

	/**
	 * Return the pair corresponding to page
	 */
	public PageFramePair getPairForPage(int page){
		// Get the hash for this page.
		int pageHash = hash(page);
		ArrayList lstPair = _arrPairList[pageHash];

		if(lstPair != null){
			for(int lstPairOffset = 0; lstPairOffset < lstPair.size(); lstPairOffset++){
				PageFramePair pageFramePair = (PageFramePair)lstPair.get(lstPairOffset);

				if(pageFramePair.getPage() == page){
					return pageFramePair;
				}
			}

			// If control has reached this point, the page wasn't found.
			return null;
		}else{
			return null;
		}
	}

	public void setPageFrame(int page, int frame) throws ChainException{
		// If the pair for page already exists, set the frame.
		PageFramePair pageFramePair = getPairForPage(page);

		if(pageFramePair != null){
			throw new DuplicateEntryException(null, "Mapping for page " + page + " already exists.");
		}else{
			// Page's entry wasn't found. Create a new pair and add it to the appropriate list.
			PageFramePair newPageFramePair = new PageFramePair(page, frame);
			ArrayList lstPair = _arrPairList[hash(page)];

			lstPair.add(newPageFramePair);
		}
	}

	public int getPageFrame(int page){
		PageFramePair pageFramePair = null;

		if((pageFramePair = getPairForPage(page)) != null){
			return pageFramePair.getFrame();
		}else{
			return Constants.NO_FRAME_FOUND;
		}
	}

	/**
	 * Removes entry for a page if it exists.
	 */
	public void removeMappingForPage(int page){
		PageFramePair pageFramePair = getPairForPage(page);
		ArrayList lstPair = _arrPairList[hash(page)];
		
		// If pair exists, remove it.
		if(pageFramePair != null){
			lstPair.remove(pageFramePair);
		}
	}
	
	/*
	 * Returns a list of page numbers in the pool.
	 */
	public ArrayList getAllPages(){
		ArrayList lstPages = new ArrayList();
		
		for(int pairListOffset = 0; pairListOffset < _arrPairList.length; pairListOffset++){
			ArrayList lstPair = _arrPairList[pairListOffset];
			
			for(int lstPairOffset = 0; lstPairOffset < lstPair.size(); lstPairOffset++){
				PageFramePair pageFramePair = (PageFramePair)lstPair.get(lstPairOffset);
				
				lstPages.add(new Integer(pageFramePair.getPage()));
			}
		}
		
		return lstPages;
	}

	public void prettyPrint(){
		// Return a string representation of the hashtable.
		for(int pairListOffset = 0; pairListOffset < _arrPairList.length; pairListOffset++){
			ArrayList lstPair = _arrPairList[pairListOffset];
			
			for(int lstPairOffset = 0; lstPairOffset < lstPair.size(); lstPairOffset++){
				PageFramePair pageFramePair = (PageFramePair)lstPair.get(lstPairOffset);
				
				System.out.println(pageFramePair.getPage() + " => " + pageFramePair.getFrame());
			}
		}
	}
	
	private int hash(int value){
		return (10 * value + 5) % (_tableSize);
	}
}	
