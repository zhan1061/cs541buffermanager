import java.io.Serializable;

public abstract class TwoPCMessage implements Serializable{
	public final static int VOTE_REQUEST_MESSAGE = 1;
	public final static int VOTE_REPLY_MESSAGE = 2;
	public final static int COMMIT_MESSAGE = 3;
	public final static int ABORT_MESSAGE = 4;
	public final static int STARTTWOPC_MESSAGE = 5;
	public final static int DECISION_REQUEST_MESSAGE = 6;
		
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
