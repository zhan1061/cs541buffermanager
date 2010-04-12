
public class ReadOperation implements IOperation{
	private AccountID _accountID;
	private double _result;
	private Transaction _parentTransaction;
		
	public ReadOperation(AccountID accountID, Transaction parentTransaction){
		_accountID = accountID;
		_parentTransaction = parentTransaction;
	}
	
	public double getResult(){
		return _result;
	}
	
	public void setResult(double result){
		_result = result; 
	}
	
	public AccountID getAccountID(){
		return _accountID;
	}
	
	public String toString(){
		return "Read acc:" + _accountID.toString(); 
	}
	
	public Transaction getParentTransaction(){
		return _parentTransaction;
	}

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
