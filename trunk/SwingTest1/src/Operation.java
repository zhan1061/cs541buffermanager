import java.io.Serializable;

public abstract class Operation implements Serializable{
	public abstract Transaction getParentTransaction();
}
