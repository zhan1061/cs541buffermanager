import java.io.Serializable;


public class TransactionID implements Serializable{
	private int _peerID;
	private int _localTransactionNumber;
	private static int _nextLocalTransactionNumber;
	
	static{
		_nextLocalTransactionNumber = 0;
	}
	
	/**
	 * Creates a TID for site whose ID is given by peerID.
	 * @param peerID
	 */
	public TransactionID(int peerID){
		_peerID = peerID;
		_localTransactionNumber = _nextLocalTransactionNumber;
		_nextLocalTransactionNumber++;
	}
	
	public int getPeerID(){
		return _peerID;
	}
	
	public int getLocalTransactionNumber(){
		return _localTransactionNumber;
	}
	
	public String toString(){
		return "TID[" + _peerID + ":" + _localTransactionNumber + "]";
	}
	
	public boolean equals(Object object){
		if(!(object instanceof TransactionID)){
			return false;
		}else{
			TransactionID transactionID = (TransactionID)object;
			
			if(transactionID.getPeerID() == _peerID && 
					transactionID.getLocalTransactionNumber() == _localTransactionNumber){
				return true;
			}else{
				return false;
			}
		}
	}
}
