import java.util.ArrayList;

public class DepositAction {
	private AccountID _accountID;
	private Transaction _parentTransaction;
	private ArrayList<IOperation> _lstOperation = null;
	private double _depositAmount = 0.0;
	
	public DepositAction(AccountID accountID, double depositAmount){
		// Just get the accountID from the argument list.
		_accountID = accountID;
		_depositAmount = depositAmount;
	}
	
	public ArrayList<IOperation> getOperations(){
		if(_lstOperation == null){
			_lstOperation = new ArrayList<IOperation>();
			
			// This is just one increment operation.
			IncrementOperation incrementOperation1 = 
				new IncrementOperation(_accountID, _depositAmount, _parentTransaction);
			
			_lstOperation.add(incrementOperation1);
		}
		
		return _lstOperation;
	}
	
	public String toString(){
		return "Deposit(" + _accountID.toString() + ")";
	}

	public void setParentTransaction(Transaction parentTransaction) {
		_parentTransaction = parentTransaction;		
	}
}
