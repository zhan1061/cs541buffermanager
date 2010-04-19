
public class RecoverAbortMessage extends TwoPCMessage {
	public RecoverAbortMessage(TransactionID transactionID){
		_messageType = ABORT_RECOVER_MESSAGE;
		_senderTransactionID = transactionID;
	}
}
