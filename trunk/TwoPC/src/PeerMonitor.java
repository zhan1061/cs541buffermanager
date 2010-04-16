import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Represents a pair of HeartBeatClient, lease time objects.
 * @author anurag
 *
 */
class ActiveEntity{
	private HeartBeatClient _heartBeatClient = null;
	private int _leaseCountRemaining = 0;
	private Peer _peer = null;
	private Thread _heartBeatClientThread = null; 
	
	public ActiveEntity(Peer peer){
		_peer = peer;
		_heartBeatClient = new HeartBeatClient(_peer);
		_leaseCountRemaining = Constants.INITIAL_LEASE_COUNT;
		_heartBeatClientThread = new Thread(_heartBeatClient);
		
		_heartBeatClientThread.start();
	}
	
	public boolean hasThreadTerminated(){
		return (_heartBeatClientThread.getState() == Thread.State.TERMINATED);
	}
	
	public void decrementLeaseCount(){
		_leaseCountRemaining--;
	}
	
	public int getLeaseCount(){
		return _leaseCountRemaining;		
	}
	
	public int getPeerState(){
		return _heartBeatClient.getPeerState();
	}
	
	public Peer getPeer(){
		return _peer;
	}
}

/**
 * Monitors peers which may be added through the UI.
 * @author anurag
 *
 */
public class PeerMonitor implements IPeerMonitor{
	private ArrayList<Peer> _lstPeer = null;
	private IPeerEventListener _peerEventListener = null;
	private HeartBeatClient _heartBeatClient = null;
	private Timer _clientLauncherTimer = null;
	private Timer _clientTesterTimer = null;
	private TimerTask _clientLauncherTimerTask = null; // Runs every 2s.
	private TimerTask _clientTesterTimerTask = null; // Runs every 1s.
	private ArrayList<ActiveEntity> _lstActiveEntity = null;
	private List<ActiveEntity> _synchlstActiveEntity = null;
	private List<Peer> _synchlstPeer = null;
	private Object _objActiveLstSynch = null;
	private Hashtable<Peer, Integer> _htPeerState = null;
	
	public PeerMonitor(){
		// Initialization.
		_lstPeer = new ArrayList<Peer>();
		_lstActiveEntity = new ArrayList<ActiveEntity>();
		_synchlstActiveEntity = Collections.synchronizedList(_lstActiveEntity);
		_synchlstPeer = Collections.synchronizedList(_lstPeer);
		_clientLauncherTimer = new Timer();
		_clientLauncherTimerTask = new ActiveEntityLauncherTimerTask();
		_clientTesterTimer = new Timer();
		_clientTesterTimerTask = new ActiveEntityTesterTimerTask();
		_objActiveLstSynch = new Object();
		_htPeerState = new Hashtable<Peer, Integer>();
	}
	
	public PeerMonitor(ArrayList<Peer> lstPeer) throws InvalidPeerException {
		// Initialization.
		_lstPeer = new ArrayList<Peer>();
		_lstActiveEntity = new ArrayList<ActiveEntity>();
		_synchlstActiveEntity = Collections.synchronizedList(_lstActiveEntity);
		_synchlstPeer = Collections.synchronizedList(_lstPeer);
		_clientLauncherTimer = new Timer();
		_clientLauncherTimerTask = new ActiveEntityLauncherTimerTask();
		_clientTesterTimer = new Timer();
		_clientTesterTimerTask = new ActiveEntityTesterTimerTask();
		_objActiveLstSynch = new Object();
		_htPeerState = new Hashtable<Peer, Integer>();
		
		// Add each peer from lstPeer.
		for(Peer peer : lstPeer){
			_synchlstPeer.add(peer);
			_htPeerState.put(peer, Constants.PEER_STATE_DEAD);
			PeerIDKeyedMap.getPeer(peer.getPeerID()).setPeerState(Constants.PEER_STATE_DEAD);
		}
	}
	
	public void startMonitor() {
		_clientLauncherTimer.scheduleAtFixedRate(_clientLauncherTimerTask, new Date(System.currentTimeMillis()), 
				Constants.MONITOR_INTERVAL_LAUNCHER_MILLIS);
		_clientTesterTimer.scheduleAtFixedRate(_clientTesterTimerTask, new Date(System.currentTimeMillis()),
				Constants.MONITOR_INTERVAL_TESTER_MILLIS);
	}	
	
	@Override
	/**
	 * Adds peer to peer list. Exception thrown if connetion cannot be
	 * established with the specified peer.
	 */
	public void addPeer(Peer peer) throws InvalidPeerException{		
		// Duplicate peer.
		if(_synchlstPeer.contains(peer)){
			// Do nothing if this is a duplicate peer. 
			return;
		}

		// Make a preliminary check to see if the peer is valid.
		HeartBeatClient heartBeatClient = new HeartBeatClient(peer);

		if(heartBeatClient.checkPeerStatus(peer)){			
			_synchlstPeer.add(peer);
			_htPeerState.put(peer, Constants.PEER_STATE_ALIVE);
			PeerIDKeyedMap.getPeer(peer.getPeerID()).setPeerState(Constants.PEER_STATE_ALIVE);
		}else{
			throw new InvalidPeerException("Peer is either non-existent, or has died.");
		}		
	}

	@Override
	/**
	 * Removes peer from peer list. Exception thrown if
	 * the peer doesn't exist.
	 */
	public void removePeer(String peerName) throws InvalidPeerException{
				
	}

	@Override
	public void setPeerEventListener(IPeerEventListener peerEventListener) {
		_peerEventListener = peerEventListener;		
	}
	
	/**
	 * TimerTask subclass that periodically adds ActiveEntities to
	 * the active entity list.
	 * @author anurag
	 *
	 */
	class ActiveEntityLauncherTimerTask extends TimerTask{

		@Override
		public void run() {			
			for (Peer peer : _synchlstPeer){
				ActiveEntity activeEntity = new ActiveEntity(peer);

				synchronized(_objActiveLstSynch){
					_synchlstActiveEntity.add(activeEntity);
				}
			}					
		}		
	}// ActiveEntityLauncherTimerTask ends.
	
	class ActiveEntityTesterTimerTask extends TimerTask{
		
		@Override
		public void run() {
			ArrayList<ActiveEntity> lstActiveEntityToDelete = new ArrayList<ActiveEntity>();
			
			synchronized(_objActiveLstSynch){
				for(ActiveEntity activeEntity : _synchlstActiveEntity){
					if(activeEntity.hasThreadTerminated()){
						if(activeEntity.getPeerState() == Constants.PEER_STATE_ALIVE){
							// Just remove this active entity since the peer is alive.
							lstActiveEntityToDelete.add(activeEntity);

							// If this peer went from being dead to being alive, log a status message.
							// Also, reflect the latest status in the state table.
							if(_htPeerState.get(activeEntity.getPeer()) == Constants.PEER_STATE_DEAD){
								_htPeerState.put(activeEntity.getPeer(), Constants.PEER_STATE_ALIVE);
								
								try {
									PeerIDKeyedMap.getPeer(activeEntity.getPeer().getPeerID()).setPeerState(Constants.PEER_STATE_ALIVE);
								} catch (InvalidPeerException e) {
									e.printStackTrace();
								}
								
//								System.out.println(activeEntity.getPeer().getPeerName() + " alive.");
								
								PeerEvent peerEventAdded = new PeerEvent(PeerEvent.PEER_ADDED, activeEntity.getPeer());
								_peerEventListener.peerAdded(peerEventAdded);
							}
							
						}else if(activeEntity.getPeerState() == Constants.PEER_STATE_DEAD){							
							// If this peer went from being alive to being dead, log a status message.
							// Also, reflect the latest status in the state table.
							if(_htPeerState.get(activeEntity.getPeer()) == Constants.PEER_STATE_ALIVE){
								_htPeerState.put(activeEntity.getPeer(), Constants.PEER_STATE_DEAD);
								
								try {
									PeerIDKeyedMap.getPeer(activeEntity.getPeer().getPeerID()).setPeerState(Constants.PEER_STATE_DEAD);
								} catch (InvalidPeerException e) {
									e.printStackTrace();
								}
								
								// Generate peer event.
								PeerEvent peerEventRemoved = new PeerEvent(PeerEvent.PEER_REMOVED, activeEntity.getPeer());
								_peerEventListener.peerRemoved(peerEventRemoved);							
							}
							
							lstActiveEntityToDelete.add(activeEntity);
						}
					}else{
						// Thread hasn't terminated yet.
						activeEntity.decrementLeaseCount();

						// Let's see if this guy has exhausted its lease.
						if(activeEntity.getLeaseCount() <= 0){
							System.out.println("Lease time exhausted... for " + activeEntity.getPeer().getPeerName());
							// If this peer went from being alive to being dead, log a status message.
							// Also, reflect the latest status in the state table.
							if(_htPeerState.get(activeEntity.getPeer()) == Constants.PEER_STATE_ALIVE){
								_htPeerState.put(activeEntity.getPeer(), Constants.PEER_STATE_DEAD);
								
								try {
									PeerIDKeyedMap.getPeer(activeEntity.getPeer().getPeerID()).setPeerState(Constants.PEER_STATE_DEAD);
								} catch (InvalidPeerException e) {
									e.printStackTrace();
								}
								
								// Generate peer event.
								PeerEvent peerEventRemoved = new PeerEvent(PeerEvent.PEER_REMOVED, activeEntity.getPeer());
								_peerEventListener.peerRemoved(peerEventRemoved);							
							}
									
							lstActiveEntityToDelete.add(activeEntity);
						}
					}
				}

				// Delete stuff from active entity list.
				for(ActiveEntity activeEntityToDelete : lstActiveEntityToDelete){
					_synchlstActiveEntity.remove(activeEntityToDelete);
				}
			}
		}
				
	}// ActiveEntityTesterTimerTask ends.
}
