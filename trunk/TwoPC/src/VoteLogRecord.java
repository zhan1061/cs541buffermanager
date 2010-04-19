import java.util.ArrayList;

public class VoteLogRecord extends LogRecord{
	public final static int VOTE_YES = 1;
	public final static int VOTE_NO = 2;
	
	private int _vote;
	private ArrayList<Integer> _lstParticipant;
	
	public VoteLogRecord(TransactionID transactionID, int vote, ArrayList<Integer> lstParticipant){
		_recordType = VOTE_LOG;
		_transactionID = transactionID;
		_vote = vote;
		_lstParticipant = lstParticipant;
	}
	
	public int getVote(){
		return _vote;
	}
	
	public ArrayList<Integer> getParticipants() {
		return _lstParticipant;
	}
	
	public String toString(){
		return "VoteLogRecord" + "(" + ((_vote == VOTE_YES)?"YES":"NO") + ")" + " for " + _transactionID.toString();
	}
}
