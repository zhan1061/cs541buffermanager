import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class HeartBeatServer {
	public HeartBeatServer(String serviceName, int registryPort) throws Exception{
		try{
			HeartBeat heartBeat = new HeartBeat();
			IHeartBeat remoteHeartBeatObj = (IHeartBeat) UnicastRemoteObject.exportObject(heartBeat, 0);
			String heartBeatServiceName = serviceName + "_HeartBeat";
			Registry registry = null;
			
			if(registryPort == 0){
				registry = LocateRegistry.getRegistry();
			}else{
				registry = LocateRegistry.getRegistry(registryPort);
			}
			
			registry.rebind(heartBeatServiceName, remoteHeartBeatObj);			
		}catch(Exception exception){
			exception.printStackTrace();
			throw exception;
		}
	}
}
