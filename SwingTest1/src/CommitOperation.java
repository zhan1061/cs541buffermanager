
public class CommitOperation extends Operation {
	private AccountID _accountID;
	private Transaction _parenTransaction;
	
	public CommitOperation(AccountID accountID, Transaction parentTransaction){
		_accountID = accountID;
		_parenTransaction = parentTransaction;
	}
	
	public AccountID getAccountID(){
		return _accountID;
	}
	
	@Override
	public Transaction getParentTransaction() {
		return _parenTransaction;
	}
}
