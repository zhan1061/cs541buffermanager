import java.util.ArrayList;

public interface IAction {
	public ArrayList<Operation> getOperations();
	public void setParentTransaction(Transaction parentTransaction);
}
