import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;


public class TransactionManager implements ITransactionManager, ITransactionEventGenerator, ITwoPCManager{
	private ComparisonTable <Transaction, TransactionFrame> _htTransactionController;
	private ArrayList<Transaction> _lstActiveTransaction;
	private ITransactionEventListener _transactionEventListener = null;
	private ComparisonTable<Transaction, Integer> _htTransactionCommitsReceived;
	private ComparisonTable<TransactionID, ITwoPCController> _htTransactionTwoPCController;
	
	public TransactionManager(){
		_htTransactionController = new ComparisonTable<Transaction, TransactionFrame>();
		_lstActiveTransaction = new ArrayList<Transaction>();
		_htTransactionCommitsReceived = new ComparisonTable<Transaction, Integer>();
		_htTransactionTwoPCController = new ComparisonTable<TransactionID, ITwoPCController>();
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
	public void begin(Transaction transaction) throws RemoteException {
		// Brings up the frame for the transaction.
		TransactionFrame transactionFrame = new TransactionFrame(transaction, this);		
		
		transactionFrame.setVisible(true);
		
		System.out.println("Adding to controller map: " + transaction.getTransactionID().toString());
		_htTransactionController.put(transaction, transactionFrame);
		
		// Remove duplicate transaction.
		if(_lstActiveTransaction.contains(transaction)){
			_lstActiveTransaction.remove(transaction);
		}
		
		_lstActiveTransaction.add(transaction);
	}

	@Override
	public void abort(Transaction transaction){
		// TODO Auto-generated method stub
		// Mark the transaction as complete.
		
	}

	@Override
	public void commit(Transaction transaction){
		// TODO Auto-generated method stub		
	}

	/**
	 * executeOperation goes ahead and schedules the operation
	 * at the scheduler. The scheduler may modify the operation
	 * by filling in results. This method should be called by the
	 * transaction controller (an object of type TransactionFrame).
	 * @throws TransactionException 
	 */
	public void executeOperation(IOperation operation) throws TransactionException{
		// From the operation, determine which site's scheduler
		// will need to be contacted. 
		try{
			int targetPeerID = operation.getTargetServerID();
			Peer peer = PeerIDKeyedMap.getPeer(targetPeerID);
			Registry registry = LocateRegistry.getRegistry(peer.getPeerHostname(), peer.getPeerPortNumber());
			IScheduler schedulerRemoteObj = (IScheduler)registry.lookup(peer.getPeerName() + "_Scheduler");
			
			schedulerRemoteObj.schedule(operation);			
		}catch(RemoteException remoteException){
			logTransactionEvent(operation.getParentTransaction(), remoteException.getMessage());
			throw new TransactionException(remoteException.getMessage());
//			remoteException.printStackTrace();
		} catch (NotBoundException e) {
			logTransactionEvent(operation.getParentTransaction(), e.getMessage());
			throw new TransactionException(e.getMessage());
//			e.printStackTrace();
		} catch (InvalidPeerException e) {
			logTransactionEvent(operation.getParentTransaction(), e.getMessage());
			throw new TransactionException(e.getMessage());
//			e.printStackTrace();
		}
	}
	
	/**
	 * If the transaction hasn't already committed, attempts to kill it.
	 * @throws RemoteException 
	 */
	public void wound(Transaction transaction) throws RemoteException{
		// Find the transaction in our active transaction list.
		// If it is not found, it means that has already completed. So
		// do nothing.
		// If it is, and its status is 'completed', then again, do nothing.
		for(Transaction activeTransaction : _lstActiveTransaction){
			if(activeTransaction.equals(transaction)){
				// Found transaction. Let's check the completion status.
				if(activeTransaction.isComplete() == false){
					if(_htTransactionTwoPCController.containsKey(transaction.getTransactionID())){
						// There exists a twoPC controller (coordinator or participant)
						// that's handling the transaction. Send a wound message to it.
						WoundMessage woundMessage = new WoundMessage(transaction.getTransactionID());
						
						relayTwoPCMessage(woundMessage);
					}else{
						// This transaction hasn't reached the 2PC stage yet.
						// We can wound this transaction. Get the corresponding controller.
						TransactionFrame transactionController = _htTransactionController.get(activeTransaction);

						transactionController.abort();
						// Close this controller and begin the transaction anew.
						transactionController.setVisible(false);
						activeTransaction.setTotalTargetServers(0);

						if(_htTransactionCommitsReceived.containsKey(activeTransaction)){
							// Reset the number of aborts received. This needs to be done
							// because a transaction with the same ID (in fact, the same transaction),
							// was just aborted.
							_htTransactionCommitsReceived.put(activeTransaction, 0);
						}

						// Create a copy of the active transaction.
						Transaction newTransaction = new Transaction(activeTransaction.getOriginatingServerID(), 
								activeTransaction.getAction());
						newTransaction.setTransactionID(activeTransaction.getTransactionID());					

						begin(newTransaction);
					}
				}
			}
		}
	}
	
	/**
	 * This method is supposed to be invoked by the client. Its side
	 * effect is that it will synchronize the result list of 'transaction'
	 * with the result list of its local copy.
	 */
	public boolean isComplete(Transaction transaction) throws RemoteException {
		for(Transaction activeTransaction : _lstActiveTransaction){
			if(activeTransaction.equals(transaction)){
//				System.out.println("Txn found.");
				if(activeTransaction.isComplete()){
					// Transfer results!!!
					transaction.markComplete(activeTransaction.getCompleteType());
					transaction.setActionResults(activeTransaction.getActionResults());
					
					for(String result : transaction.getActionResults()){
						System.out.println(result);
					}
					
//					System.out.println("Returning true!!!");
					
					return true;
				}else{
//					System.out.println("Returning false!!!");
					return false;
				}
			}
		}
		
		throw new RemoteException("Transaction object not found.");
	}
	
	public Transaction getTransactionState(Transaction transaction) throws RemoteException {
		for(Transaction activeTransaction : _lstActiveTransaction){
			if(activeTransaction.equals(transaction)){
				return activeTransaction;				
			}
		}		
		
		throw new RemoteException("Transaction object not found.");
	}
	
	public void deleteTransaction(Transaction transaction) throws TransactionException, RemoteException{
		// Removes this transaction from the list of active transaction.
		// If the transaction hasn't completed, a TransactionException will
		// be thrown.
		if(transaction.isComplete()){
			logTransactionEvent(transaction, "Cleaned up.");
			
			synchronized (_lstActiveTransaction) {
				_lstActiveTransaction.remove(transaction);			
			}			
		}else{
			throw new TransactionException("Attempt to delete incomplete transaction.");
		}
	}

	public Hashtable<Integer, Double> getAccountDetails(int peerID) throws RemoteException{
		try {
			Peer peer = PeerIDKeyedMap.getPeer(peerID);
			Registry registry = LocateRegistry.getRegistry(peer.getPeerHostname(), peer.getPeerPortNumber());
			IScheduler schedulerRemoteObj = (IScheduler)registry.lookup(peer.getPeerName() + "_Scheduler");
			
			return schedulerRemoteObj.getLocalAccountDetails();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * The operation argument will have the result (if any) of the operation. This
	 * method will be invoked by the scheduler. For this reason, mind the reference.
	 */
	public void operationComplete(IOperation operation) throws RemoteException {
		// Notify the IOperationCompletedEventHandler associated with this operation.
		IOperationCompletedEventHandler operationCompletedEventHandler = null;
		Transaction parentTransaction = operation.getParentTransaction();
		
		if(_htTransactionController.containsKey(parentTransaction)){
			operationCompletedEventHandler = _htTransactionController.get(parentTransaction);
			
			if(operation instanceof CommitOperation || operation instanceof AbortOperation){
				int targetPeerID = operation.getTargetServerID();
				Peer peer;
				
				System.out.println("TM - processing commit/abort.");
								
				try {
					peer = PeerIDKeyedMap.getPeer(targetPeerID);
					Registry registry = LocateRegistry.getRegistry(peer.getPeerHostname(), peer.getPeerPortNumber());
					IScheduler schedulerRemoteObj = (IScheduler)registry.lookup(peer.getPeerName() + "_Scheduler");
					
					operation.getParentTransaction().setActionResults(
							schedulerRemoteObj.getFinalTransactionResult(operation.getParentTransaction()));
					logTransactionEvent(operation.getParentTransaction(), "Results obtained.");
					
					synchronized(_lstActiveTransaction){
						for(Transaction activeTransaction : _lstActiveTransaction){
							if(activeTransaction.equals(operation.getParentTransaction())){
								// Add to the results of the active transaction.
								if(activeTransaction.getActionResults() != null){
									for(String result : operation.getParentTransaction().getActionResults()){
										activeTransaction.getActionResults().add(result);
									}
								}

								// Mark as complete only if all awaited commits/aborts have
								// been received.
								if(_htTransactionCommitsReceived.containsKey(activeTransaction) == false){
									_htTransactionCommitsReceived.put(activeTransaction, 1);
								}else{
									int commitsReceived = _htTransactionCommitsReceived.get(activeTransaction);
									_htTransactionCommitsReceived.put(activeTransaction, 
											commitsReceived + 1);
								}

								if(_htTransactionCommitsReceived.get(activeTransaction) == activeTransaction.getTotalTargetServers()){
									System.out.println("Received from : " + _htTransactionCommitsReceived.get(activeTransaction));
									activeTransaction.markComplete(operation.getParentTransaction().getCompleteType());

									// Remove entry from table: _htTransactionCommitsReceived
									//								_htTransactionCommitsReceived.remove(activeTransaction);
								}
							}
						}
					}
				} catch (Exception exception) {
					logTransactionEvent(operation.getParentTransaction(), "Results not obtained.");
				}
				
				if(_htTransactionCommitsReceived.get(parentTransaction) == 
					parentTransaction.getTotalTargetServers()){
					if(operation instanceof CommitOperation){
						parentTransaction.markComplete(Transaction.COMMIT_COMPLETE);
					}else{
						parentTransaction.markComplete(Transaction.ABORT_COMPLETE);
					}
				}
			}
			
			operationCompletedEventHandler.operationCompleted(operation);
		}else{
			System.out.println("No handler for " + operation.getParentTransaction().toString());
			System.out.println(operation.getParentTransaction().getTransactionID().toString());
		}
	}

	@Override
	public void setTransactionEventListener(
			ITransactionEventListener transactionEventListener) {
		_transactionEventListener = transactionEventListener;
	}

	private void logTransactionEvent(Transaction transaction, String message){
		if(_transactionEventListener != null){
			_transactionEventListener.transactionEventOccurred(new TransactionEvent(transaction, message));
		}
	}

	@Override
	/**
	 * Relays the message to it's two PC controller.
	 */
	public void relayTwoPCMessage(TwoPCMessage twoPCMessage)
			throws RemoteException {
		// If this site has 'failed', throw a RemoteException.
		if(FailureEventMonitor.getFailureEventMonitor().getCurrentFailureState() == FailureEvent.FAILURE_EVENT){
			throw new RemoteException("Simulated failure.");
		}
		
		if(_htTransactionTwoPCController.containsKey(twoPCMessage.getSenderTransactionID())){
			ITwoPCController twoPCController = 
				_htTransactionTwoPCController.get(twoPCMessage.getSenderTransactionID());
			
			twoPCController.processMessage(twoPCMessage);
		}else{
			// Start a participant. The reason why we know this is a participant
			// site is that if it had been a coordinator, the controller would already
			// have been registered when the message came in.
			ParticipantController participantController = 
				new ParticipantController(twoPCMessage.getSenderTransactionID(), this);
			
			participantController.setVisible(true);
			participantController.processMessage(twoPCMessage);
		}
	}

	@Override
	/**
	 * Registers a TwoPCController for a transaction.
	 */
	public void registerControllerForTransaction(TransactionID transactionID, ITwoPCController twoPCController) {
		_htTransactionTwoPCController.put(transactionID, twoPCController);
	}

	@Override
	public void simulateCommitForPeer(TransactionID transactionID, int peerID)
			throws RemoteException {
		// Lookup the transaction controller for this transaction and tell it to commit.
		// Not giving in to the urge to pull off a hack (:D).
		ArrayList<Transaction> lstTransaction = _htTransactionController.keys();
		
		for(Transaction transaction : lstTransaction){
			if(transaction.getTransactionID().equals(transactionID)){
				TransactionFrame transactionController = _htTransactionController.get(transaction);
				
				transactionController.commitPeer(peerID);
				
				break;
			}
		}
	}
	
	@Override
	public void simulateAbortForPeer(TransactionID transactionID, int peerID)
			throws RemoteException {
		// Lookup the transaction controller for this transaction and tell it to commit.
		// Not giving in to the urge to pull off a hack (:D).
		ArrayList<Transaction> lstTransaction = _htTransactionController.keys();
		
		for(Transaction transaction : lstTransaction){
			if(transaction.getTransactionID().equals(transactionID)){
				TransactionFrame transactionController = _htTransactionController.get(transaction);
				
				transactionController.abortPeer(peerID);
				
				break;
			}
		}
	}
}
