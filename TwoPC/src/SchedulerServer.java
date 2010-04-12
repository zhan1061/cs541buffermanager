import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class SchedulerServer {
	public SchedulerServer(String serviceName, int registryPort, 
			ISchedulerEventListener schedulerEventListener) throws Exception{
		try{
			Scheduler scheduler = new Scheduler();
			IScheduler remoteSchedulerObj = (IScheduler) UnicastRemoteObject.exportObject(scheduler, 0);
			String schedulerServiceName = serviceName + "_Scheduler";
			Registry registry = null;
			
			scheduler.setSchedulerEventListener(schedulerEventListener);
			
			if(registryPort == 0){
				registry = LocateRegistry.getRegistry();
			}else{
				registry = LocateRegistry.getRegistry(registryPort);
			}
			
			registry.rebind(schedulerServiceName, remoteSchedulerObj);			
		}catch(Exception exception){
			exception.printStackTrace();
			throw exception;
		}
	}
}
