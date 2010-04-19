
public class AbortLogRecord extends LogRecord{
	public AbortLogRecord(TransactionID transactionID){
		_transactionID = transactionID;
		_recordType = ABORT_LOG;
	}
	
	public String toString(){
		return "AbortLogRecord for " + _transactionID.toString();
	}
}
