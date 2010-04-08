import java.util.Hashtable;


/**
 * A utility class that returns the peer object given its ID.
 * @author anurag
 *
 */
public class PeerIDKeyedMap {
	private static Hashtable<Integer, Peer> _htIDPeer;
	
	static{
		_htIDPeer = new Hashtable<Integer, Peer>();
	}
	
	/**
	 * Inserts the peer into the table. If the ID is found to exist,
	 * the operation is rejected and an exception is thrown.
	 * @param peer
	 * @throws InvalidPeerException
	 */
	public static void insertPeer(Peer peer) throws InvalidPeerException{
		if(_htIDPeer.containsKey(peer.getPeerID())){
			throw new InvalidPeerException("Duplicate peer ID. : " + peer.getPeerID() + ";" + peer.getPeerName());
		}else{
			_htIDPeer.put(peer.getPeerID(), peer);			
		}		
	}
	
	/**
	 * Returns the peer given the ID.
	 * @param peerID
	 * @return
	 * @throws InvalidPeerException
	 */
	public static Peer getPeer(int peerID) throws InvalidPeerException{
		if(_htIDPeer.containsKey(peerID) == false){
			throw new InvalidPeerException("Peer doesn't exist.");
		}else{
			return _htIDPeer.get(peerID);
		}
	}
}
