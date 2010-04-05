
public class ReadOperation extends Operation{
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
}
