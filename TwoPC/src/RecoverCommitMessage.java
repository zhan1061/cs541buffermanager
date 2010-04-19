
public class RecoverCommitMessage extends TwoPCMessage {
	public RecoverCommitMessage(TransactionID transactionID){
		_messageType = ABORT_COMMIT_MESSAGE;
		_senderTransactionID = transactionID;
	}
}
