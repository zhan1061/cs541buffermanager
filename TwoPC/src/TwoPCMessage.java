import java.io.Serializable;

public abstract class TwoPCMessage implements Serializable{
	public final static int VOTE_REQUEST_MESSAGE = 1;
	public final static int VOTE_REPLY_MESSAGE = 2;
	public final static int COMMIT_MESSAGE = 3;
	public final static int ABORT_MESSAGE = 4;
	public final static int STARTTWOPC_MESSAGE = 5;
	public final static int DECISION_REQUEST_MESSAGE = 6;
	public static final int DECISION_REPLY_MESSAGE = 7;
	public static final int ABORT_RECOVER_MESSAGE = 8;
	public static final int ABORT_COMMIT_MESSAGE = 9;
	public static final int START_CTP_MESSAGE = 10;
	public static final int WOUND_MESSAGE = 11;
	
	protected int _messageType;
	protected TransactionID _senderTransactionID;
	
	public int getMessageType(){
		return _messageType;
	}
	
	public TransactionID getSenderTransactionID(){
		return _senderTransactionID;		
	}
	
	public void setSenderTransactionID(TransactionID senderTransactionID){
		_senderTransactionID = senderTransactionID;
	}
}
