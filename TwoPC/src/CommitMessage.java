
public class CommitMessage extends TwoPCMessage {
	public CommitMessage(int participantID, TransactionID transactionID){
		_messageType = COMMIT_MESSAGE;
		_senderTransactionID = transactionID;
	}
}
