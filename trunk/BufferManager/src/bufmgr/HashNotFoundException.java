
package bufmgr;
import chainexception.*;

public class HashNotFoundException extends ChainException{

  public HashNotFoundException(Exception e, String name)
  { super(e, name); }
 
}
