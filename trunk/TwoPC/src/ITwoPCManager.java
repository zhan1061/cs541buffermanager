import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ITwoPCManager{
//	public void relayTwoPCMessage(TwoPCMessage twoPCMessage) throws RemoteException;
	public void registerControllerForTransaction(TransactionID transactionID, ITwoPCController twoPCController);
}
