
public class PeerEvent {
	public final static int PEER_ADDED = 1;
	public final static int PEER_REMOVED = 2;
	private Peer _peer;
	private int _eventType;
	
	public PeerEvent(int eventType, String peerName, String peerHostname, int peerPortNumber){
		_peer = new Peer(peerName, peerHostname, peerPortNumber);
		_eventType = eventType;		
	}
	
	public PeerEvent(int eventType, Peer peer){
		_peer = peer;
		_eventType = eventType;
	}
	
	public Peer getPeer(){
		return _peer;
	}
	
	public int getEventType(){
		return _eventType;
	}
}
