import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Transaction implements Serializable{
	public static int COMMIT_COMPLETE = 1;
	public static int ABORT_COMPLETE = 2;
	public static int NOT_COMPLETE = 3;
	
	private ArrayList<IOperation> _lstOperations = null;
	private IAction _action = null;
	private TransactionID _transactionID = null;
	private int _peerID; // Will help us keep track of the originating peer.
	private boolean _bCompleted;
	private ArrayList <String> _lstActionResult = null;
	private int _completeType = NOT_COMPLETE;
	private int _totalTargetServers = 0;	
	
	public Transaction(int peerID, IAction action){
		_action = action;
		_action.setParentTransaction(this);
		_peerID = peerID;
		_transactionID = new TransactionID(_peerID);
		_lstOperations = _action.getOperations();
		_lstActionResult = new ArrayList<String>();
		_bCompleted = false;
		setTotalTargetServers(0);
	}
	
	/**
	 * Decomposes the corresponding operation into basic R/W operations based on
	 * the action type.
	 */
	public ArrayList<IOperation> getOperations(){
		return _lstOperations;
	}
	
	public TransactionID getTransactionID(){
		return _transactionID;
	}
	
	public void setTransactionID(TransactionID transactionID){
		_transactionID = transactionID;
	}
	
	public ArrayList <String> getActionResults(){
		return _lstActionResult;
	}
	
	public void setActionResults(ArrayList <String> lstActionResults){
		_lstActionResult = lstActionResults;
	}
	
	public void markComplete(int completeType){
		// Also make sure that the result list is complete.
		_completeType = completeType;
		_bCompleted = true;
	}
	
	public IAction getAction(){
		return _action;
	}
	
	public int getCompleteType(){
		return _completeType;		
	}
	
	public boolean isComplete(){
		return _bCompleted;
	}
	
	public String toString(){
		return _action.toString();
	}
	
	public boolean equals(Object object){
		if(object instanceof Transaction){
			Transaction transaction = (Transaction)object;
			return _transactionID.equals(transaction.getTransactionID());
		}else{
			return false;
		}
	}
	
	/**
	 * Returns the peerID of the peer/server where this transaction was created.
	 * @return
	 */
	public int getOriginatingServerID(){
		return _peerID;
	}

	public void setTotalTargetServers(int totalTargetServers) {
		this._totalTargetServers = totalTargetServers;
	}

	public int getTotalTargetServers() {
		return _totalTargetServers;
	}
}
