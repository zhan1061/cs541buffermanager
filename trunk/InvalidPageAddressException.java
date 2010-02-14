package bufmgr;

import chainexception.*;
import diskmgr.*;

public class InvalidPageAddressException extends ChainException{
	public InvalidPageAddressException(Exception e, String name){ 
		super(e, name);
	}
}
