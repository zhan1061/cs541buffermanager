package bufmgr;

import java.util.ArrayList;

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

		System.out.println("Created table.");
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

	public void setPageFrame(int page, int frame){
		// If the pair for page already exists, set the frame.
		PageFramePair pageFramePair = getPairForPage(page);

		if(pageFramePair != null){
			pageFramePair.setFrame(frame);
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

	private int hash(int value){
		return (10 * value + 5) % (_tableSize);
	}
}	
