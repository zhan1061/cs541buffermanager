
public class DecisionRequestMessage extends TwoPCMessage {
	private int _participantID;
	
	public DecisionRequestMessage(int participantID, TransactionID transactionID){
		_messageType = DECISION_REQUEST_MESSAGE;
		_participantID = participantID;
		_senderTransactionID = transactionID;
	}
	
	public int getParticipantID(){
		return _participantID;
	}
}
