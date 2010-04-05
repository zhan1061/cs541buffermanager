
public class Peer {
	private String _peerName;
	private String _peerHostname;
	private int _peerPortNumber;
	private int _peerID;
	
	public Peer(String peerName, String peerHostname, int peerPortNumber){
		setPeerName(peerName);
		setPeerHostname(peerHostname);
		setPeerPortNumber(peerPortNumber);
	}
	
	public Peer(String peerName, String peerHostname, int peerPortNumber, int peerID){
		setPeerName(peerName);
		setPeerHostname(peerHostname);
		setPeerPortNumber(peerPortNumber);
		setPeerID(peerID);
	}
	
	public boolean equals(Object object){
		if(!(object instanceof Peer)){
			return false;
		}else{
			Peer testPeer = (Peer)object;
			
			if(testPeer.getPeerName().equals(_peerName)){
				return true;
			}else{
				return false;
			}
		}
	}

	public void setPeerName(String peerName) {
		this._peerName = peerName;
	}

	public String getPeerName() {
		return _peerName;
	}

	public void setPeerHostname(String peerHostname) {
		this._peerHostname = peerHostname;
	}

	public String getPeerHostname() {
		return _peerHostname;
	}

	public void setPeerPortNumber(int peerPortNumber) {
		this._peerPortNumber = peerPortNumber;
	}

	public int getPeerPortNumber() {
		return _peerPortNumber;
	}

	public void setPeerID(int _peerID) {
		this._peerID = _peerID;
	}

	public int getPeerID() {
		return _peerID;
	}
	
	
}
