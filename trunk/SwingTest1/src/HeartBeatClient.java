import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class HeartBeatClient implements Runnable{
	Peer _peer = null;
	private int _heartBeatState = Constants.PEER_STATE_ALIVE;
	
	public HeartBeatClient(Peer peer){
		_peer = peer;
	}
	
	public Peer getPeer(){
		return _peer;
	}
	
	/**
	 * Issues an RMI call to the heart beat server running at the peer
	 * to see if it is still running.
	 * @param peer
	 * @return
	 */
	public boolean checkPeerStatus(Peer peer){
		try{
			Registry registry = LocateRegistry.getRegistry(peer.getPeerHostname(), peer.getPeerPortNumber());
			IHeartBeat remoteHeartBeatObject = (IHeartBeat)registry.lookup(peer.getPeerName() + "_HeartBeat");
			
			// areYouAlive is a blocking call. 
			if(remoteHeartBeatObject.areYouAlive()){
				_heartBeatState = Constants.PEER_STATE_ALIVE;
			}
			
			// A lack of exception is assumed to mean the server at the other
			// end is still alive.
			return true;
		}catch(Exception exception){
			_heartBeatState = Constants.PEER_STATE_DEAD;
			return false;
		}
	}
	
	public int getPeerState(){
		return _heartBeatState;
	}

	@Override
	public void run() {
		checkPeerStatus(_peer);
	}
}
