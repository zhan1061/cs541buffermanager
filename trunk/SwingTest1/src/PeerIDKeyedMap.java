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
			throw new InvalidPeerException("Duplicate peer ID.");
		}else{
			_htIDPeer.put(peer.getPeerID(), peer);			
		}		
	}
}
