import java.io.Serializable;


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
	
	public String toString(){
		return "[" + _peerID + ":" + _localAccountNumber + "]";
	}
}
