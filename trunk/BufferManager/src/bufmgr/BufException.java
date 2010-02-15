package bufmgr;
import chainexception.*;

public class BufException extends ChainException{

  public BufException(Exception e, String name)
  { super(e, name); }
 
}
