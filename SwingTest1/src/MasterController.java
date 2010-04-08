import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;


public class MasterController implements IPeerEventListener, ITransactionEventListener, ISchedulerEventListener{
	IPeerMonitor _peerMonitor = null;
	IUserInteractionHandler _uiHandler = null;
	int _registryPort = 0;
	String _serviceName = "";
	
	public MasterController(String serviceName){
		_uiHandler = new ServerFrame("Server - " + serviceName);
		_uiHandler.addPeerEventListener(this);
		_serviceName = serviceName;
		
		GlobalState.set("uiHandler", _uiHandler);
		
		// Read the servers.txt file if it exists to get the list of peers.
		ArrayList<Peer> lstPeer = getPeersFromFile("servers.txt");
		
		try{
			_peerMonitor = new PeerMonitor(lstPeer);
			_peerMonitor.setPeerEventListener(this);			
		}catch(Exception exception){
			//_uiHandler.appendLog("Local HeartBeat service registered.");
		}	
		
		// Start HeartBeatServer.
//		try{
//			HeartBeatServer heartBeatServer = new HeartBeatServer(_serviceName, _registryPort);
//			
//			_uiHandler.appendLog("Local HeartBeat service registered.");
//		}catch(Exception exception){
//			// HeartBeat server start failure.
//			_uiHandler.appendLog("Error occurred while attempting to start HeartBeatServer: " + exception.getMessage());		
//		}
		
		Thread heartBeatServerThread = new Thread(new HeartBeatServerThread());
		Thread transactionManagerServerThread = new Thread(new TransactionManagerServerThread());
		Thread schedulerServerThread = new Thread(new SchedulerServerThread());
		
		heartBeatServerThread.start();
		transactionManagerServerThread.start();
		schedulerServerThread.start();
		
		// Start PeerMonitor.		
		_peerMonitor.startMonitor();
	}	
	
	/**
	 * Gets peers that form the peer network from configuration file.
	 * Also sets the local registry port number.
	 * @param filePath
	 * @return
	 */
	private ArrayList<Peer> getPeersFromFile(String filePath){
		ArrayList<Peer> lstPeer = new ArrayList<Peer>();
		
		try{
			FileReader fileReaderServers = new FileReader(filePath);
			BufferedReader brServers = new BufferedReader(fileReaderServers);
			String line = "";
			
			while((line = brServers.readLine()) != null){
				String[] arrServerLineComponent = line.trim().split(",");
				int peerID = Integer.parseInt(arrServerLineComponent[0].trim());
				String peerName = arrServerLineComponent[1].trim();
				String peerHostName = arrServerLineComponent[2].trim();
				int peerPortNumber = Integer.parseInt(arrServerLineComponent[3].trim());
				
				PeerIDKeyedMap.insertPeer(new Peer(peerName, peerHostName, peerPortNumber, peerID));
				PeerNameKeyedMap.insertPeer(new Peer(peerName, peerHostName, peerPortNumber, peerID));
				
				if(peerName.equals(_serviceName) == false){
					lstPeer.add(new Peer(peerName, peerHostName, peerPortNumber, peerID));
				}else{
					_registryPort = peerPortNumber;
					GlobalState.set("localPeerName", peerName);
					GlobalState.set("localPeerID", peerID);
				}
			}			
		}catch(Exception exception){
			this._uiHandler.appendLog("Error occurred while reading servers file: " + exception.getMessage());
		}
		
		return lstPeer;
	}
	
	@Override
	public void peerAdded(PeerEvent peerEvent) {
		// Dispatch event content to IPeerMonitor
		try{
			_peerMonitor.addPeer(peerEvent.getPeer());
			
			// Peer added successfully to monitor.
			_uiHandler.appendLog("Peer [" + peerEvent.getPeer().getPeerName() + "] added successfully.");
		}catch(InvalidPeerException invalidPeerException){
			_uiHandler.appendLog("Invalid peer");
		}
	}

	@Override
	public void peerRemoved(PeerEvent peerEvent) {
		// Peer removed.
		_uiHandler.appendLog("Peer [" + peerEvent.getPeer().getPeerName() + "] failed.");
	}

	class HeartBeatServerThread implements Runnable {
		@Override
		public void run() {
			// Start HeartBeatServer.
			try{
				HeartBeatServer heartBeatServer = new HeartBeatServer(_serviceName, _registryPort);
				
				_uiHandler.appendLog("Local HeartBeat service registered.");
			}catch(Exception exception){
				// HeartBeat server start failure.
				_uiHandler.appendLog("Error occurred while attempting to start HeartBeatServer: " + exception.getMessage());		
			}
		}
	}
	
	class TransactionManagerServerThread implements Runnable{
		@Override
		public void run() {
			// Start TransactionManagerServer.
			try{
				TransactionManagerServer transactionManagerServer = 
					new TransactionManagerServer(_serviceName, _registryPort,
							MasterController.this);
				
				_uiHandler.appendLog("Local TransactionManager service registered.");
			}catch(Exception exception){
				// TransactionManager server start failure.
				_uiHandler.appendLog("Error occurred while attempting to start TransactionManagerServer: " + exception.getMessage());		
			}
		}		
	}
	
	class SchedulerServerThread implements Runnable{
		@Override
		public void run() {
			// Start SchedulerServer.
			try{
				SchedulerServer schedulerServer = new SchedulerServer(_serviceName, _registryPort, 
						MasterController.this);
				
				_uiHandler.appendLog("Local Scheduler service registered.");
			}catch(Exception exception){
				// Scheduler server start failure.
				_uiHandler.appendLog("Error occurred while attempting to start SchedulerServer: " + exception.getMessage());		
			}
		}		
	}

	@Override
	public void transactionEventOccurred(TransactionEvent transactionEvent) {
		_uiHandler.appendLog(transactionEvent.getTransaction().toString()+ " - " + transactionEvent.getMessage());		
	}

	@Override
	public void schedulerEventOccurred(SchedulerEvent schedulerEvent) {
		_uiHandler.appendLog(schedulerEvent.getMessage());		
	}
}
