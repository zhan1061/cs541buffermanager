
public interface IPeerMonitor {
	public void addPeer(Peer peer) throws InvalidPeerException;
	
	public void removePeer(String peerName) throws InvalidPeerException;
	
	public void setPeerEventListener(IPeerEventListener peerEventListener);
	
	public void startMonitor();
}
