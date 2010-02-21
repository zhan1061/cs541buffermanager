package heap;
import chainexception.*;

public class HFBufMgrException extends ChainException{


  public HFBufMgrException()
  {
     super();
  
  }

  public HFBufMgrException(Exception e, String name)
  {
    super(e, name);
  }
}