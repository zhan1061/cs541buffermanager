
public class VoteReplyMessage extends TwoPCMessage {
	public static final int YES_VOTE = 1;
	public static final int NO_VOTE = 2;
	public static final int INVALID_VOTE = 0;
	
	private int _participantID;
	private int _vote;
	
	public VoteReplyMessage(int participantID, int vote, TransactionID transactionID){
		_messageType = VOTE_REPLY_MESSAGE;
		_participantID = participantID;
		_vote = vote;
		_senderTransactionID = transactionID;
	}
	
	public int getParticipantID(){
		return _participantID;
	}
	
	public int getVote(){
		return _vote;
	}
}
