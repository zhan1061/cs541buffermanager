
public class CommitOperation implements IOperation {
	private Transaction _parentTransaction;
	private int _targetServerID = -1;
	
	public CommitOperation(Transaction parentTransaction){
		_parentTransaction = parentTransaction;
	}
	
	@Override
	public Transaction getParentTransaction() {
		return _parentTransaction;
	}

	@Override
	public int getTargetServerID() {
		return _targetServerID;
	}

	@Override
	public int getSourceServerID() {
		// Grab originating server ID from the parent transaction.
		return _parentTransaction.getOriginatingServerID();
	}

	@Override
	public void setTargetServerID(int targetServerID) {
		_targetServerID = targetServerID;		
	}
}
