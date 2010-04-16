import java.rmi.RemoteException;

public class HeartBeat implements IHeartBeat {	
	@Override
	public boolean areYouAlive() throws RemoteException {
		// Take a while before responding.
		if(FailureEventMonitor.getFailureEventMonitor().getCurrentFailureState() == 
			FailureEvent.FAILURE_EVENT){

			throw new RemoteException("Simulated failure.");
		}
		
		return true;
	}
}
