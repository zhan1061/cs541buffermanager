import java.rmi.RemoteException;

public class HeartBeat implements IHeartBeat {	
	@Override
	public boolean areYouAlive() throws RemoteException {
		// Take a while before responding.
		try{
			Thread.sleep(3000);
		}catch(InterruptedException interruptedException){			
		}
		
		return true;
	}
}
