package heap;
import chainexception.*;

public class HFException extends ChainException{


  public HFException()
  {
     super();
  
  }

  public HFException(Exception e, String name)
  {
    super(e, name);
  }



}