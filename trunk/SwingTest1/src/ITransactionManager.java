import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ITransactionManager extends Remote{
	public Transaction createTransaction(IAction action) throws RemoteException;
	public void begin(Transaction transaction) throws RemoteException;
	public void commit(Transaction transaction) throws RemoteException;
	public void abort(Transaction transaction) throws RemoteException;
	public void operationComplete(Operation operation) throws RemoteException;
	public void executeOperation(Operation operation, 
			IOperationCompletedEventHandler operationCompletedEvent) throws RemoteException;
	public boolean isComplete(Transaction transaction) throws RemoteException;
}
