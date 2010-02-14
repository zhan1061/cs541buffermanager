package bufmgr;

import chainexception.ChainException;

public class PageNotInPoolException extends ChainException {
	public PageNotInPoolException(Exception e, String name){ 
		super(e, name);
	}
}
