
public abstract class LogRecord {
	public static final int VOTEREQUEST_LOG = 1;
	public static final int START2PC_LOG = 2;
	public static final int VOTE_LOG = 3;
	public static final int COMMIT_LOG = 4;
	public static final int ABORT_LOG = 5;
		
	protected TransactionID _transactionID;
	protected int _recordType;	
	
	public TransactionID getTransactionID(){
		return _transactionID;
	}
	
	public int getRecordType(){
		return _recordType;
	}	
}
