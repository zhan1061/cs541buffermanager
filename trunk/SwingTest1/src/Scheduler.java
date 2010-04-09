import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;


public class Scheduler implements IScheduler, ISchedulerEventGenerator {
	// The next two tables are for the use of R/W operaiton only.
	private ComparisonTable <AccountID, ArrayList<IOperation>> _htAccountLockWaitTable = new ComparisonTable<AccountID, ArrayList<IOperation>>();
	private ComparisonTable<AccountID, ArrayList<IOperation>> _htAccountOwnerTable = new ComparisonTable<AccountID, ArrayList<IOperation>>();
	private ArrayList<CreateAccountOperation> _lstAccountCatalogWaitList = new ArrayList<CreateAccountOperation>();
	private IOperation _accountCatalogOwnerOperation = null;
	private ComparisonTable<Transaction, ArrayList<IOperation>> _htCompletedOperationsTable = new ComparisonTable<Transaction, ArrayList<IOperation>>();
	private static int _lastAccountIDNumber = 0;
	private ArrayList<Transaction> _lstUnqueriedTransaction = new ArrayList<Transaction>();
	private ISchedulerEventListener _schedulerEventListener = null;
	private static DataManager _dataManager = null;
	private int _localPeerID;
	
	static{
		_dataManager = new DataManager((Integer)GlobalState.get("localPeerID"));
	}
	
	public Scheduler(){
		_localPeerID = (Integer)GlobalState.get("localPeerID");
		System.out.println("Scheduler created!!");
	}
	
	@Override
	public void schedule(IOperation operation) throws RemoteException, TransactionException {
		if(_lstUnqueriedTransaction.contains(operation.getParentTransaction()) == false){
			_lstUnqueriedTransaction.add(operation.getParentTransaction());
		}
		
		if(operation instanceof CreateAccountOperation){
			scheduleCreateAccountOperation((CreateAccountOperation)operation);
		}else if(operation instanceof CommitOperation){
			try{
				scheduleCommitOperation((CommitOperation)operation);
			}catch(Exception exception){
				logSchedulerEvent(exception.getMessage());
				exception.printStackTrace();
			}
		}else if(operation instanceof AbortOperation){
			try{
				scheduleAbortOperation((AbortOperation)operation);
			}catch(Exception exception){
				logSchedulerEvent(exception.getMessage());
				exception.printStackTrace();
			}
		}else{
			scheduleReadWriteOperation(operation);
		}
	}
	
	private ITransactionManager getTransactionManagerRemoteObj(IOperation operation){
		Transaction transaction = operation.getParentTransaction();
		int originatingServerID = transaction.getOriginatingServerID();
		
		try{
			Peer originatingPeer = PeerIDKeyedMap.getPeer(originatingServerID);
			Registry registry = LocateRegistry.getRegistry(originatingPeer.getPeerHostname(), originatingPeer.getPeerPortNumber());
			System.out.println(originatingPeer);
			ITransactionManager transactionManagerRemoteObject = 
				(ITransactionManager) registry.lookup(originatingPeer.getPeerName() + "_TransactionManager");
			
			return transactionManagerRemoteObject;
		}catch(Exception exception){
			logSchedulerEvent("Unable to communicate with remote TransactionManagerObject");
			exception.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * Removes and returns the first waiting operation in the lock wait table.
	 * Null is returned if there are not waiting operations.
	 * @return IOperation
	 */
	private IOperation removeFirstWaitingOperation(){
		IOperation waitingOperation = null;
		ArrayList<AccountID> lstAccountID = _htAccountLockWaitTable.keys();
		
		if(lstAccountID.isEmpty() == false){
			AccountID firstAccountID = lstAccountID.get(0);
			ArrayList<IOperation> lstWaitingOperation = _htAccountLockWaitTable.get(firstAccountID);
			
			// Get the first operation from the list of waiting operations.
			waitingOperation = lstWaitingOperation.remove(0);
			
			// If removing the operation causes the waiting list to become
			// empty, remove the account's entry from the wait table.
			if(lstWaitingOperation.isEmpty()){
				_htAccountLockWaitTable.remove(firstAccountID);
			}
		}
		
		return waitingOperation;
	}
	
	/**
	 * Remove all operations belonging to transaction from owner lists.
	 * @param transaction
	 */
	private void removeTransactionFromOwnerLists(Transaction transaction) {
		//for(ArrayList<IOperation> lstOwnerTransaction : _ht)
		ArrayList<AccountID> lstAccountID = _htAccountOwnerTable.keys();
		
		for(AccountID accountID : lstAccountID){
			ArrayList<IOperation> lstOperationToDelete = new ArrayList<IOperation>();
			ArrayList<IOperation> lstAccountOperation = _htAccountOwnerTable.get(accountID);
			
			for(IOperation operation : lstAccountOperation){
				if(operation.getParentTransaction().equals(transaction)){
					lstOperationToDelete.add(operation);					
				}
			}
			
			// Delete all operations that are in lstOperationToDelete
			for(IOperation operationToDelete : lstOperationToDelete){
				lstAccountOperation.remove(operationToDelete);
			}
		}
	}

	private void addToTransactionListOfCompletedOperations(Transaction transaction, IOperation operation){
		// Add this to the table of completed operations.
		if(_htCompletedOperationsTable.containsKey(transaction)){
			_htCompletedOperationsTable.get(transaction).add(operation);
		}else{
			ArrayList<IOperation> lstCompletedOperations = new ArrayList<IOperation>();
			lstCompletedOperations.add(operation);
			
			_htCompletedOperationsTable.put(transaction, lstCompletedOperations);
		}
	}
	
	/**
	 * Performs the appropriate write operation.
	 * @param operation
	 * @throws TransactionException 
	 */
	private void performWriteOperation(IOperation operation) throws TransactionException {
		// Now, write operations are further split up into increment and decrement operations.
		if(operation instanceof IncrementOperation){
			IncrementOperation incrementOperation = (IncrementOperation)operation;
			// Set the old balance.
			double[] readBalanceData = _dataManager.readBalance(_localPeerID, incrementOperation.getAccountID().getLocalAccountNumber());
			
			if(readBalanceData[0] == DataManager.SUCCESSFUL_OPERATION){
				incrementOperation.setOldBalance(readBalanceData[1]);				
				_dataManager.writeBalance(_localPeerID, incrementOperation.getAccountID().getLocalAccountNumber(), 
						incrementOperation.getNewBalance());
				
				// Also, add this to the transaction result list.
				Transaction parentTransaction = incrementOperation.getParentTransaction();
				parentTransaction.getActionResults().add("Balance: " + incrementOperation.getNewBalance());
			}else{
				// Also, add this to the transaction result list.
				Transaction parentTransaction = incrementOperation.getParentTransaction();
				parentTransaction.getActionResults().add("Unable to write new balance.");
				
				throw new TransactionException("Unable to write new balance.");
			}
		}else if(operation instanceof DecrementOperation){
			DecrementOperation decrementOperation = (DecrementOperation)operation;
			
			// Set the old balance.
			double[] readBalanceData = _dataManager.readBalance(_localPeerID, decrementOperation.getAccountID().getLocalAccountNumber());
			
			if(readBalanceData[0] == DataManager.SUCCESSFUL_OPERATION){
				decrementOperation.setOldBalance(readBalanceData[1]);
				_dataManager.writeBalance(_localPeerID, decrementOperation.getAccountID().getLocalAccountNumber(), 
						decrementOperation.getNewBalance());
				
				// Also, add this to the transaction result list.
				Transaction parentTransaction = decrementOperation.getParentTransaction();
				parentTransaction.getActionResults().add("Balance: " + decrementOperation.getNewBalance());
			}else{
				// Also, add this to the transaction result list.
				Transaction parentTransaction = decrementOperation.getParentTransaction();
				parentTransaction.getActionResults().add("Unable to write new balance.");
				
				throw new TransactionException("Unable to write new balance.");
			}	
		}				
	}

	/**
	 * Performs the read operation.
	 * @param operation
	 */
	private void performReadOperation(IOperation operation) throws TransactionException{
		ReadOperation readOperation = (ReadOperation) operation;
		
		double[] readBalanceData = _dataManager.readBalance(_localPeerID, readOperation.getAccountID().getLocalAccountNumber());
		
		if(readBalanceData[0] == DataManager.SUCCESSFUL_OPERATION){
			// Also, add this to the transaction result list.
			readOperation.setResult(readBalanceData[1]);
			
			Transaction parentTransaction = readOperation.getParentTransaction();
			parentTransaction.getActionResults().add("Balance: " + readOperation.getResult());
		}else{
			// Also, add this to the transaction result list.
			Transaction parentTransaction = readOperation.getParentTransaction();
			parentTransaction.getActionResults().add("Unable to get balance.");
			
			throw new TransactionException("Unable to get balance.");
		}
		
	}

	/**
	 * Returns true if operation conflicts with at least one of the operations in the list.
	 * @param operation
	 * @param lstOperation
	 * @return
	 */
	private boolean isOperationConflicting(IOperation operation, ArrayList<IOperation> lstOperation){
		for(IOperation testOperation : lstOperation){
			if(operation instanceof IncrementOperation || operation instanceof DecrementOperation){
				// Regardless of the test operation type, this is a conflict.
				if(operation.getParentTransaction().equals(testOperation.getParentTransaction()) == false){
					return true;
				}
			}else{
				if(testOperation instanceof IncrementOperation || testOperation instanceof DecrementOperation){
					if(operation.getParentTransaction().equals(testOperation.getParentTransaction()) == false){
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Roll back (in reverse order), all operations performed
	 * on behalf of this transaction.
	 * @param transaction
	 */
	private void rollbackTransaction(Transaction transaction){
		// Roll back completed operations belonging to this transaction.
		if(_htCompletedOperationsTable.containsKey(transaction)){
			ArrayList<IOperation> lstOperation = _htCompletedOperationsTable.get(transaction);
			int lstOperationOffset;
			
			// Roll operations back in reverse.
			for(lstOperationOffset = lstOperation.size() - 1; lstOperationOffset >= 0; lstOperationOffset--){
				IOperation operation = lstOperation.get(lstOperationOffset);
				
				if(operation.getClass().equals(IncrementOperation.class) ||
						operation.getClass().equals(DecrementOperation.class)){
					
					// Write the old balance.
					_dataManager.writeBalance(_localPeerID, 
							((WriteOperation)operation).getAccountID().getLocalAccountNumber(), 
							((WriteOperation)operation).getOldBalance());
				}else if(operation.getClass().equals(CreateAccountOperation.class)){
					// Delete the account.
					_dataManager.deleteAccount(_localPeerID, 
							((CreateAccountOperation)operation).getAccountID().getLocalAccountNumber());
				}
			}			
		}
	}
	
	/**
	 * Schedules an account creation operation.
	 * @param createAccountOperation
	 */
	private void scheduleCreateAccountOperation(CreateAccountOperation createAccountOperation){
		// Account creation should be handled slightly differently than other operations.
		if(_accountCatalogOwnerOperation == null && isLockOwnerTableEmpty()){
			// No one owns this lock yet. Assign the lock to this operation.
			_accountCatalogOwnerOperation = createAccountOperation;
			int newLocalAccountNumber = _dataManager.createAccount(_localPeerID);
			
			createAccountOperation.setAccountID(new AccountID(_localPeerID, newLocalAccountNumber));
			
			// Append a line to the Transaction result list.
			ArrayList<String> lstTransactionResult = createAccountOperation.getParentTransaction().getActionResults();
			lstTransactionResult.add("Created account:" + createAccountOperation.getAccountID().toString());
			
			try {
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(createAccountOperation);
				Transaction parentTransaction = createAccountOperation.getParentTransaction();
				
				transactionManager.operationComplete(createAccountOperation);				
				addToTransactionListOfCompletedOperations(parentTransaction, createAccountOperation);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			
			_lastAccountIDNumber++;
		}else{
			// Add this operation to the wait list.
			_lstAccountCatalogWaitList.add(createAccountOperation);
		}
	}
	
	private void scheduleCommitOperation(CommitOperation commitOperation) throws Exception{
		// Release all locks held by the parent transaction.
		Transaction parentTransaction = commitOperation.getParentTransaction();
		ITransactionManager transactionManager = getTransactionManagerRemoteObj(commitOperation);
		
		// Go thru owner lists and release locks.
		if(_accountCatalogOwnerOperation != null && 
				commitOperation.getParentTransaction().equals(_accountCatalogOwnerOperation.getParentTransaction())){
			// Commit immediately.
			
			try {
				commitOperation.getParentTransaction().markComplete(Transaction.COMMIT_COMPLETE);
				transactionManager.operationComplete(commitOperation);
				
				// Remove the entry from the uncommitted operations list.
				_htCompletedOperationsTable.remove(parentTransaction);
			} catch (RemoteException e) {
				throw new Exception("Unable to process commit.");
			}
			
			_accountCatalogOwnerOperation = null;
		}else{
			// Non-account-creation operation.
			// Release all locks held by operations of the commit operation's
			// parent transaction.
			removeTransactionFromOwnerLists(commitOperation.getParentTransaction());
			
			// Mark transaction as complete.
			commitOperation.getParentTransaction().markComplete(Transaction.COMMIT_COMPLETE);
			transactionManager.operationComplete(commitOperation);
			
			// Remove transactoin from completed operations table (we won't be
			// rolling back any operation for this transaction since it has committed).
			_htCompletedOperationsTable.remove(parentTransaction);
		}
		
		// If the wait list for the account catalog is not empty,
		// Pick and schedule the next create account operation. That is,
		// preference is always given to account creation operations.
		if(_lstAccountCatalogWaitList.isEmpty() == false){
			// Pull the next account creation operation in and schedule it.
			CreateAccountOperation createAccountOperation = 
				_lstAccountCatalogWaitList.remove(0);
			
			scheduleCreateAccountOperation(createAccountOperation);
		}else{
			// Grab the first operation from the first account in the lock wait table.		
			// Fairness be damned.
			IOperation firstWaitingOperation = removeFirstWaitingOperation();
			
			if(firstWaitingOperation != null){
				// Schedule this operation.
				scheduleReadWriteOperation(firstWaitingOperation);
			}
		}
	}
	
	/**
	 * Schedules an abort operation.
	 * @param operation
	 */
	private void scheduleAbortOperation(AbortOperation abortOperation) throws Exception{
		// Release all locks held by the parent transaction.
		Transaction parentTransaction = abortOperation.getParentTransaction();
		ITransactionManager transactionManager = getTransactionManagerRemoteObj(abortOperation);
		
		// Go thru owner lists and release locks.
		if(_accountCatalogOwnerOperation != null && 
				abortOperation.getParentTransaction().equals(_accountCatalogOwnerOperation.getParentTransaction())){
			// Abort immediately.
			try {
				parentTransaction.markComplete(Transaction.ABORT_COMPLETE);
				rollbackTransaction(parentTransaction);
				// Remove the entry from the uncommitted operations list.
				_htCompletedOperationsTable.remove(parentTransaction);
				transactionManager.operationComplete(abortOperation);				
			} catch (RemoteException e) {
				throw new Exception("Unable to process abort.");
			}
			
			_accountCatalogOwnerOperation = null;
		}else{
			// Non-account-creation operation.
			// Release all locks held by operations of the abort operation's
			// parent transaction.
			removeTransactionFromOwnerLists(parentTransaction);
			// Mark transaction as complete.
			parentTransaction.markComplete(Transaction.ABORT_COMPLETE);			
			rollbackTransaction(parentTransaction);
			// Remove transactoin from completed operations table.
			_htCompletedOperationsTable.remove(parentTransaction);
			transactionManager.operationComplete(abortOperation);
		}
		
		// If the wait list for the account catalog is not empty,
		// Pick and schedule the next create account operation. That is,
		// preference is always given to account creation operations.
		if(_lstAccountCatalogWaitList.isEmpty() == false){
			// Pull the next account creation operation in and schedule it.
			CreateAccountOperation createAccountOperation = 
				_lstAccountCatalogWaitList.remove(0);
			
			scheduleCreateAccountOperation(createAccountOperation);
		}else{
			// Grab the first operation from the first account in the lock wait table.		
			// Fairness be damned.
			IOperation firstWaitingOperation = removeFirstWaitingOperation();
			
			if(firstWaitingOperation != null){
				// Schedule this operation.
				scheduleReadWriteOperation(firstWaitingOperation);
			}
		}	
	}
	
	/**
	 * Schedules a R/W operation. The lock handling logic for both types
	 * will be different.
	 * @throws TransactionException 
	 */
	private void scheduleReadWriteOperation(IOperation operation) throws TransactionException{
		if(_accountCatalogOwnerOperation != null || _lstAccountCatalogWaitList.isEmpty() == false){
			// An account creation operation is waiting/running. We wait for it to finish.
			// Add operation to the wait-list for its account.
			AccountID targetAccountID = null;

			if(operation instanceof ReadOperation){
				targetAccountID = ((ReadOperation)operation).getAccountID();
			}else if(operation instanceof WriteOperation){
				targetAccountID = ((WriteOperation)operation).getAccountID();
			}

			ArrayList<IOperation> lstWaitListForAccount = null;
			
			if(_htAccountLockWaitTable.containsKey(targetAccountID) == false){
				_htAccountLockWaitTable.put(targetAccountID, new ArrayList<IOperation>());
			}
			
			lstWaitListForAccount = _htAccountLockWaitTable.get(targetAccountID);
			lstWaitListForAccount.add(operation);
		}else{
			AccountID targetAccountID = null;
			
			if(operation instanceof ReadOperation){
				targetAccountID = ((ReadOperation)operation).getAccountID();
			}else if(operation instanceof WriteOperation){
				targetAccountID = ((WriteOperation)operation).getAccountID();
			}
			
			// Check if there's already a conflicting owner for the account.
			ArrayList<IOperation> lstOwnerListForAccount = _htAccountOwnerTable.get(targetAccountID);
			
			if(lstOwnerListForAccount != null && isOperationConflicting(operation, lstOwnerListForAccount)){
				// Add operation to the wait list for the target account.
				ArrayList<IOperation> lstWaitListForAccount = null;
				
				if(_htAccountLockWaitTable.containsKey(targetAccountID) == false){
					_htAccountLockWaitTable.put(targetAccountID, new ArrayList<IOperation>());
				}
				
				lstWaitListForAccount = _htAccountLockWaitTable.get(targetAccountID);
				
				lstWaitListForAccount.add(operation);
			}else{
				// Add this operation to the list of owners.
				if(lstOwnerListForAccount == null){
					lstOwnerListForAccount = new ArrayList<IOperation>();
					_htAccountOwnerTable.put(targetAccountID, lstOwnerListForAccount);
				}
				
				lstOwnerListForAccount.add(operation);
				
				// Perform the operation.
				if(operation instanceof ReadOperation){
					performReadOperation(operation);
				}else if(operation instanceof WriteOperation){
					performWriteOperation(operation);
				}
				
				// Mark operation as complete.
				try {
					ITransactionManager transactionManager = getTransactionManagerRemoteObj(operation);
					Transaction parentTransaction = operation.getParentTransaction();
					
					transactionManager.operationComplete(operation);
					
					// Add to list of completed operations for the transaction.
					addToTransactionListOfCompletedOperations(parentTransaction, operation);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
	}	
	
	private boolean isLockOwnerTableEmpty(){
		ArrayList<AccountID> lstAccountID = _htAccountOwnerTable.keys();
		
		for(AccountID accountID : lstAccountID){			
			if(_htAccountOwnerTable.containsKey(accountID)){
				ArrayList<IOperation> lstOwnerOperations = _htAccountOwnerTable.get(accountID);
				
				if(lstOwnerOperations.isEmpty() == false){
					// There is some owner.
					return false;
				}
			}
		}
		
		return true;
	}

	@Override
	public ArrayList<String> getFinalTransactionResult(Transaction transaction)
			throws RemoteException {
		if(_lstUnqueriedTransaction.contains(transaction) && transaction.isComplete()){
			int transactionOffset = _lstUnqueriedTransaction.indexOf(transaction);
			
			return _lstUnqueriedTransaction.get(transactionOffset).getActionResults();
		}
		
		return null;
	}

	private void logSchedulerEvent(String message){
		if(_schedulerEventListener != null){
			_schedulerEventListener.schedulerEventOccurred(new SchedulerEvent(message));
		}
	}
	
	@Override
	public void setSchedulerEventListener(
			ISchedulerEventListener schedulerEventListener) {
		_schedulerEventListener = schedulerEventListener;
	}
}
