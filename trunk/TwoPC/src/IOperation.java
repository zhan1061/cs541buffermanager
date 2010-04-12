import java.io.Serializable;

public interface IOperation extends Serializable{
	public abstract Transaction getParentTransaction();
	public abstract int getTargetServerID();
	public abstract int getSourceServerID();
	public void setTargetServerID(int targetServerID);
}
