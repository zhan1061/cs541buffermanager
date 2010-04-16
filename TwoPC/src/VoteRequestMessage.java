import java.util.ArrayList;


public class VoteRequestMessage extends TwoPCMessage{
	private int _coordinatorID;
	private ArrayList<Integer> _lstParticipant;
	
	public VoteRequestMessage(int coordinatorID, ArrayList<Integer> lstParticipant, TransactionID transactionID){		
		_messageType = VOTE_REQUEST_MESSAGE;
		_coordinatorID = coordinatorID;
		_lstParticipant = lstParticipant;
		_senderTransactionID = transactionID;
	}

	public void setCoordinatorID(int _coordinatorID) {
		this._coordinatorID = _coordinatorID;
	}

	public int getCoordinatorID() {
		return _coordinatorID;
	}

	public void setParticipants(ArrayList<Integer> _lstParticipant) {
		this._lstParticipant = _lstParticipant;
	}

	public ArrayList<Integer> getParticipants() {
		return _lstParticipant;
	}
}
