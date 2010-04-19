
public class CommitLogRecord extends LogRecord {
	public CommitLogRecord(TransactionID transactionID){
		_transactionID = transactionID;
		_recordType = COMMIT_LOG;
	}
	
	public String toString(){
		return "CommitLogRecord for " + _transactionID.toString();
	}
}
