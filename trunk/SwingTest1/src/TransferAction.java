import java.util.ArrayList;


public class TransferAction {
	private AccountID _fromAccountID;
	private AccountID _toAccountID;
	private Transaction _parentTransaction;
	private ArrayList<IOperation> _lstOperation = null;
	private double _amount = 0.0;
	
	public TransferAction(AccountID fromAccountID, AccountID toAccountID, double amount){
		// Just get the accountID from the argument list.
		_fromAccountID = fromAccountID;
		_toAccountID = toAccountID;
		_amount = amount;
	}
	
	public ArrayList<IOperation> getOperations(){
		if(_lstOperation == null){
			_lstOperation = new ArrayList<IOperation>();
			
			// This is just one increment operation.
			DecrementOperation operation1 = 
				new DecrementOperation(_fromAccountID, _amount, _parentTransaction);
			IncrementOperation operation2 = 
				new IncrementOperation(_toAccountID, _amount, _parentTransaction);
			
			_lstOperation.add(operation1);
			_lstOperation.add(operation2);
		}
		
		return _lstOperation;
	}
	
	public String toString(){
		return "Transfer(" + _fromAccountID.toString() + " -> " + _toAccountID.toString() + ")";
	}

	public void setParentTransaction(Transaction parentTransaction) {
		_parentTransaction = parentTransaction;		
	}
}
