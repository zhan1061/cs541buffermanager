
public class CreateAccountOperation implements IOperation {
	private int _peerID;
	private AccountID _resultAccountID;
	private Transaction _parentTransaction;
		
	/**
	 * CreateAccountOperations are only carried out in the context of the local site.
	 * @param parentTransaction
	 */
	public CreateAccountOperation(Transaction parentTransaction){
		_peerID = (Integer)GlobalState.get("localPeerID");
		_parentTransaction = parentTransaction;
	}
	
	public int getPeerID(){
		return _peerID;
	}
	
	public void setAccountID(AccountID accountID){
		_resultAccountID = accountID;
	}
	
	public AccountID getAccountID(){
		return _resultAccountID;
	}
	
	public String toString(){
		return "Create account"; 
	}
	
	public Transaction getParentTransaction(){
		return _parentTransaction;
	}

	@Override
	public int getTargetServerID() {
		return _peerID;
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
