import java.io.Serializable;
import java.util.ArrayList;

public class GetBalanceAction implements IAction, Serializable {
	private AccountID _accountID;
	private Transaction _parentTransaction;
	private ArrayList<IOperation> _lstOperation = null;
	
	public GetBalanceAction(ArrayList lstArgs){
		// Just get the accountID from the argument list.
		_accountID = (AccountID)lstArgs.get(0);
		_parentTransaction = null; // This must be set by the enclosing transaction.
	}
	
	public GetBalanceAction(AccountID accountID){
		// Just get the accountID from the argument list.
		_accountID = accountID;		
	}
	
	public ArrayList<IOperation> getOperations(){
		if(_lstOperation == null){
			_lstOperation = new ArrayList<IOperation>();
			
			// This is just one read operation.
			ReadOperation readOperation1 = new ReadOperation(_accountID, _parentTransaction);
			_lstOperation.add(readOperation1);
		}
		
		return _lstOperation;
	}
	
	public String toString(){
		return "GetBalance(" + _accountID.toString() + ")";
	}

	@Override
	public void setParentTransaction(Transaction parentTransaction) {
		_parentTransaction = parentTransaction;		
	}
}
