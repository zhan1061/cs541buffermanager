
public class WoundMessage extends TwoPCMessage {
	public WoundMessage(TransactionID transactionID){
		_messageType = WOUND_MESSAGE;
		_senderTransactionID = transactionID;
	}
}
