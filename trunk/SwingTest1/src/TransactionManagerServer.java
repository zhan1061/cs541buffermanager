import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class TransactionManagerServer {
	public TransactionManagerServer(String serviceName, int registryPort, 
			ITransactionEventListener transactionEventListener) throws Exception{
		try{
			TransactionManager transactionManager = new TransactionManager();
			ITransactionManager remoteTransactionManagerObj = (ITransactionManager) UnicastRemoteObject.exportObject(transactionManager, 0);
			String transactionManagerServiceName = serviceName + "_TransactionManager";
			Registry registry = null;
			
			transactionManager.setTransactionEventListener(transactionEventListener);
			
			if(registryPort == 0){
				registry = LocateRegistry.getRegistry();
			}else{
				registry = LocateRegistry.getRegistry(registryPort);
			}
			
			registry.rebind(transactionManagerServiceName, remoteTransactionManagerObj);			
		}catch(Exception exception){
			exception.printStackTrace();
			throw exception;
		}
	}
}
