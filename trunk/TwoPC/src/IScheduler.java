import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Hashtable;

public interface IScheduler extends Remote {
	public void schedule(IOperation operation) throws RemoteException, TransactionException;
	public ArrayList<String> getFinalTransactionResult(Transaction transaction) throws RemoteException;
	public Hashtable<Integer, Double> getLocalAccountDetails() throws RemoteException;
}
