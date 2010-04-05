import java.util.Hashtable;


/**
 * A utility class that returns the peer object given its name.
 * @author anurag
 *
 */
public class PeerNameKeyedMap {
	private static Hashtable<String, Peer> _htNamePeer;

	static{
		_htNamePeer = new Hashtable<String, Peer>();
	}

	/**
	 * Inserts the peer into the table. If the name is found to exist,
	 * the operation is rejected and an exception is thrown.
	 * @param peer
	 * @throws InvalidPeerException
	 */
	public static void insertPeer(Peer peer) throws InvalidPeerException{
		if(_htNamePeer.containsKey(peer.getPeerName())){
			throw new InvalidPeerException("Duplicate peer Name.");
		}else{
			_htNamePeer.put(peer.getPeerName(), peer);			
		}		
	}
}
