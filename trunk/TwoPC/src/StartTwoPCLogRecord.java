import java.util.ArrayList;


public class StartTwoPCLogRecord extends LogRecord {
	private ArrayList<Integer> _lstParticipant;
	
	public StartTwoPCLogRecord(TransactionID transactionID, ArrayList<Integer> lstParticipant){
		_recordType = START2PC_LOG;
		_transactionID = transactionID;
		_lstParticipant = lstParticipant;
	}
	
	public String toString(){
		return "Start-2PC for TID: " + _transactionID.toString();
	}
}
