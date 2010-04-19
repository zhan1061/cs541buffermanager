import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Hashtable;

public interface ITransactionManager extends Remote{
	// Remotely invocable methods.
	public Transaction createTransaction(IAction action) throws RemoteException;
	public void begin(Transaction transaction) throws RemoteException;
	public void operationComplete(IOperation operation) throws RemoteException;
	public boolean isComplete(Transaction transaction) throws RemoteException;
	public Transaction getTransactionState(Transaction transaction) throws RemoteException;
	public void deleteTransaction(Transaction transaction) throws RemoteException, TransactionException;
	public void wound(Transaction transaction) throws RemoteException;
	
	// Locally invocable methods.  
	public void commit(Transaction transaction) throws RemoteException;
	public void abort(Transaction transaction) throws RemoteException;
	public void executeOperation(IOperation operation) throws RemoteException, TransactionException;
	public Hashtable<Integer, Double> getAccountDetails(int peerID) throws RemoteException;
}
