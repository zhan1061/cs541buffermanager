
public class StartCTPMessage extends TwoPCMessage{
	public StartCTPMessage(TransactionID transactionID){
		_messageType = START_CTP_MESSAGE;
		_senderTransactionID = transactionID;
	}
}
