
public class DecisionRequestMessage extends TwoPCMessage {
	private int _participantID;
	
	public DecisionRequestMessage(int participantID){
		_messageType = DECISION_REQUEST_MESSAGE;
		_participantID = participantID;		
	}
	
	public int getParticipantID(){
		return _participantID;
	}
}
