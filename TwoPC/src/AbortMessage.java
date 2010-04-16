
public class AbortMessage extends TwoPCMessage {
	public AbortMessage(int participantID, TransactionID transactionID){
		_messageType = ABORT_MESSAGE;
		_senderTransactionID = transactionID;
	}
}
