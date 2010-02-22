package heap;
import chainexception.*;

public class SpaceNotAvailableException extends ChainException{


  public SpaceNotAvailableException()
  {
     super();
  
  }

  public SpaceNotAvailableException(Exception e, String name)
  {
    super(e, name);
  }

}
