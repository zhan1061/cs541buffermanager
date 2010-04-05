
public class AbortOperation extends Operation {
	private AccountID _accountID;
	private Transaction _parenTransaction;
	
	public AbortOperation(AccountID accountID, Transaction parentTransaction){
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
