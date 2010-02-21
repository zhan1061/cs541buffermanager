package heap;
import chainexception.*;

public class InvalidUpdateException extends ChainException{


  public InvalidUpdateException ()
  {
     super();
  }

  public InvalidUpdateException (Exception e, String name)
  {
    super(e, name);
  }

}
