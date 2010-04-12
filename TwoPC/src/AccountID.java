import java.io.Serializable;

/**
 * Account information.
 * @author anurag
 *
 */
public class AccountID implements Serializable{
	private int _peerID;
	private int _localAccountNumber;
	
	public AccountID(int peerID, int localAccountNumber){
		_peerID = peerID;
		_localAccountNumber = localAccountNumber;
	}
	
	public int getPeerID(){
		return _peerID;
	}
	
	public int getLocalAccountNumber(){
		return _localAccountNumber;
	}
	
	public boolean equals(Object object){		
		if(object instanceof AccountID){
			AccountID accountID = (AccountID)object;
			
			if(_peerID == accountID.getPeerID() && _localAccountNumber == accountID.getLocalAccountNumber()){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}
	
	public String toString(){
		return "[" + _peerID + ":" + _localAccountNumber + "]";
	}
}
