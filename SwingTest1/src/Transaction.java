import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Transaction implements Serializable{
	private ArrayList<Operation> _lstOperations = null;
	private IAction _action = null;
	private TransactionID _transactionID = null;
	private int _peerID; // Will help us keep track of the originating peer.
	private boolean _bCompleted;
	private ArrayList <String> _lstActionResult = null;
		
	public Transaction(int peerID, IAction action){
		_action = action;
		_action.setParentTransaction(this);
		_peerID = peerID;
		_transactionID = new TransactionID(_peerID);
		_lstOperations = _action.getOperations();
		_bCompleted = false;
	}
	
	/**
	 * Decomposes the corresponding operation into basic R/W operations based on
	 * the action type.
	 */
	public ArrayList<Operation> getOperations(){
		return _lstOperations;
	}
	
	public TransactionID getTransactionID(){
		return _transactionID;
	}
	
	public void markComplete(){
		// Also make sure that the result list is complete.
		_bCompleted = true;
	}
	
	public boolean isCompleted(){
		return _bCompleted;
	}
	
	public String toString(){
		return _action.toString();
	}
}
