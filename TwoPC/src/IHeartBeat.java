import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IHeartBeat extends Remote{
	public boolean areYouAlive() throws RemoteException;
}
