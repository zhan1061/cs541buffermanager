
public class StartTwoPCMessage extends TwoPCMessage{
	private int _coordinatorID;
	
	public StartTwoPCMessage(int coordinatorID, TransactionID transactionID){
		_messageType = STARTTWOPC_MESSAGE;
		_senderTransactionID = transactionID;
		_coordinatorID = coordinatorID;
	}
	
	public int getCoordinatorID(){
		return _coordinatorID;		
	}
}
