package bufmgr;

import global.*;
import chainexception.ChainException;
import diskmgr.*;

public class Hello{
	public static void main(String[] args){
		BufMgr bufMgr = null;
		
		try{
			bufMgr = new BufMgr(512, "Clock");
			Page page = new Page();			
			PageId pageId = bufMgr.newPage(page, 10);
			
			System.out.println("First page id:" + pageId.pid);
			
			byte[] pageData = page.getpage();
			pageData[0] = (byte)'a';
			pageData[1] = (byte)'b';
			pageData[2] = (byte)'c';
			
			bufMgr.flushPage(pageId);
			
			System.out.println("Written to disk.");
			
			PageId pinPgId = new PageId();
			pinPgId.pid = 3;
			
			bufMgr.pinPage(pinPgId, page, false);
			
			byte[] readPageData = page.getpage();
			
			System.out.println("Read page");
			System.out.println((char)readPageData[0] + " " + (char)readPageData[1] + " " + (char)readPageData[2]);			
		}catch(ChainException chainException){
			chainException.printStackTrace();
		}
	}
}