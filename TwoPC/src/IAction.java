import java.util.ArrayList;

public interface IAction {
	public ArrayList<IOperation> getOperations();
	public void setParentTransaction(Transaction parentTransaction);
}
