
public class DecisionReplyMessage extends TwoPCMessage {
	public static final int ABORT_DECISION_REPLY = 1;
	public static final int COMMIT_DECISION_REPLY = 2;
		
	private int _decisionReplyType;
	private int _senderID;
	
	public DecisionReplyMessage(int senderID, int decisionReplyType, TransactionID transactionID){
		_messageType = DECISION_REPLY_MESSAGE;
		_decisionReplyType = decisionReplyType;
		_senderTransactionID = transactionID;
		_senderID = senderID;		
	}
	
	public int getDecisionReplyType(){
		return _decisionReplyType;
	}
	
	public int getSenderID(){
		return _senderID;
	}
}
