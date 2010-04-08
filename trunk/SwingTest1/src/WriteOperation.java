
public abstract class WriteOperation implements IOperation {
	private AccountID _accountID;
//	private double _newBalance;
//	private Transaction _parentTransaction;
//	
//	public WriteOperation(AccountID accountID, double newBalance, Transaction parentTransaction){
//		_accountID = accountID;
//		_newBalance = newBalance;
//		_parentTransaction = parentTransaction;
//	}
//	
	public AccountID getAccountID(){
		return _accountID;
	}
	
	public double getOldBalance() {
		return 0.0;
	}
	
//	public double getNewBalance(){
//		return _newBalance;
//	}
//	
//	public String toString(){
//		return "Write " + _newBalance + " -> acc:" + _accountID.toString(); 
//	}
//	
//	public Transaction getParentTransaction(){
//		return _parentTransaction;
//	}
}
