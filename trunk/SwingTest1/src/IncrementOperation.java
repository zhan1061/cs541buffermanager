
public class IncrementOperation extends WriteOperation {
	private AccountID _accountID;
	private double _incrementAmount;
	private double _oldBalance;
	private Transaction _parentTransaction;
	
	public IncrementOperation(AccountID accountID, double incrementAmount, Transaction parentTransaction){
		_accountID = accountID;
		_incrementAmount = incrementAmount;
		_parentTransaction = parentTransaction;
	}
	
	public AccountID getAccountID(){
		return _accountID;
	}
	
	public double getNewBalance(){
		return _oldBalance + _incrementAmount;
	}
	
	public double getIncrementAmount(){
		return _incrementAmount;
	}
	
	@Override
	public double getOldBalance(){
		return _oldBalance;
	}
	
	public void setOldBalance(double oldBalance){
		_oldBalance = oldBalance;
	}
	
	public String toString(){
		return "Increment " + _incrementAmount + " -> acc:" + _accountID.toString(); 
	}
	
	public Transaction getParentTransaction(){
		return _parentTransaction;
	}

	@Override
	public int getTargetServerID() {
		return _accountID.getPeerID();
	}

	@Override
	public int getSourceServerID() {
		// Grab originating server ID from the parent transaction.
		return _parentTransaction.getOriginatingServerID();
	}

	@Override
	public void setTargetServerID(int targetServerID) {
		// TODO Auto-generated method stub
		
	}
}
