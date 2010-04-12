import java.io.Serializable;
import java.util.ArrayList;

public class WithdrawAction implements IAction, Serializable{
	private AccountID _accountID;
	private Transaction _parentTransaction;
	private ArrayList<IOperation> _lstOperation = null;
	private double _withdrawAmount = 0.0;
	
	public WithdrawAction(AccountID accountID, double withdrawAmount){
		// Just get the accountID from the argument list.
		_accountID = accountID;
		_withdrawAmount = withdrawAmount;
	}
	
	public ArrayList<IOperation> getOperations(){
		if(_lstOperation == null){
			_lstOperation = new ArrayList<IOperation>();
			
			// This is just one increment operation.
			DecrementOperation decrementOperation1 = 
				new DecrementOperation(_accountID, _withdrawAmount, _parentTransaction);
			
			_lstOperation.add(decrementOperation1);
		}
		
		return _lstOperation;
	}
	
	public String toString(){
		return "Withdraw(" + _accountID.toString() + ")";
	}

	public void setParentTransaction(Transaction parentTransaction) {
		_parentTransaction = parentTransaction;		
	}
}
