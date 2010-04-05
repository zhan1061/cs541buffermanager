import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Hashtable;


public class TransactionManager implements ITransactionManager{
	private Hashtable <Transaction, TransactionFrame> _htTransactionController;
	private ArrayList<Transaction> _lstActiveTransaction;
	
	public TransactionManager(){
		_htTransactionController = new Hashtable<Transaction, TransactionFrame>();
		_lstActiveTransaction = new ArrayList<Transaction>();
	}
	
	/**
	 * Creates a transaction encapsulating the desired action. Should be called
	 * by the client/account holder.
	 * @param action
	 * @return
	 * @throws RemoteException
	 */
	public Transaction createTransaction(IAction action) throws RemoteException{
		Transaction transaction = new Transaction((Integer)GlobalState.get("localPeerID"), action);
		return transaction;
	}
	
	@Override
	public void abort(Transaction transaction) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void begin(Transaction transaction) throws RemoteException {
		// Brings up the frame for the transaction.
		TransactionFrame transactionFrame = new TransactionFrame(transaction, this);		
		
		transactionFrame.setVisible(true);
		
		_htTransactionController.put(transaction, transactionFrame);
		_lstActiveTransaction.add(transaction);
	}

	@Override
	public void commit(Transaction transaction) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

	/**
	 * executeOperation goes ahead and schedules the operation
	 * at the scheduler. The scheduler may modify the operation
	 * by filling in results. This method should be called by the
	 * transaction controller (an object of type TransactionFrame).
	 */
	public void executeOperation(Operation operation,
			IOperationCompletedEventHandler operationCompletedEvent)
			throws RemoteException {
		
	}

	/**
	 * This method is supposed to be invoked by the client. Its side
	 * effect is that it will synchronize the result list of 'transaction'
	 * with the result list of its local copy.
	 */
	public boolean isComplete(Transaction transaction) throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * The operation argument will have the result (if any) of the operation. This
	 * method will be invoked by the scheduler. 
	 */
	public void operationComplete(Operation operation) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

}
