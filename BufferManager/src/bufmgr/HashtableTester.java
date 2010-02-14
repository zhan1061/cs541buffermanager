package bufmgr;

public class HashtableTester{
	public static void main(String[] args){
		PageHashtable pageHashTable = new PageHashtable(23);

		// Add stuff
		for(int entry = 0; entry < 50; entry++){
			pageHashTable.setPageFrame(entry, entry*2);
		}

		// Retrieve stuff
		for(int entry = 0; entry < 50; entry++){
			System.out.println("Entry: " + entry + " => " + pageHashTable.getPageFrame(entry));
		}

		System.out.println("Entry: " + 100 + " => " + pageHashTable.getPageFrame(100));

		pageHashTable.removeMappingForPage(25);

		System.out.println("Entry: " + 25 + " => " + pageHashTable.getPageFrame(25));
	}
}
