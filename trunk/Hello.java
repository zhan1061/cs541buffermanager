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
		}catch(ChainException chainException){
			chainException.printStackTrace();
		}
	}
}