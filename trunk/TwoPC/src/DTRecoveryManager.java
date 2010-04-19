import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

/**
 * Handles distributed recovery.
 * @author anurag
 *
 */
public class DTRecoveryManager {
	private ComparisonTable<TransactionID, ArrayList<LogRecord>> _htTransactionLogs;
	private ArrayList<Integer> _lstParticipant;
	private int _coordinatorID;
	private int _localPeerID;
	
	public DTRecoveryManager(){
		_htTransactionLogs = new ComparisonTable<TransactionID, ArrayList<LogRecord>>();
		_localPeerID = (Integer)GlobalState.get("localPeerID");
		buildTransactionLogsTable();
	}
	
	public void printDTLog(){
		for(LogRecord logRecord : DTLog.getLog()){
			System.out.println(logRecord.toString());
		}
	}	
	
	/**
	 * Goes over the DTLog to build a table mapping logs for a certain transaction
	 * against the transaction itself. 
	 */
	private void buildTransactionLogsTable(){
		ArrayList<TransactionID> lstCompletedTransaction = new ArrayList<TransactionID>();
		
		for(LogRecord logRecord : DTLog.getLog()){
			ArrayList<LogRecord> lstTransactionLog = null;
			
			if(_htTransactionLogs.containsKey(logRecord.getTransactionID())){
				lstTransactionLog = _htTransactionLogs.get(logRecord.getTransactionID());
			}else{
				lstTransactionLog = new ArrayList<LogRecord>();
				
				_htTransactionLogs.put(logRecord.getTransactionID(), lstTransactionLog);
			}
			
			lstTransactionLog.add(logRecord);
			
			if(logRecord.getRecordType() == LogRecord.ABORT_LOG || 
					logRecord.getRecordType() == LogRecord.COMMIT_LOG){
				// This transaction has completed - i.e., either committed or aborted.
				lstCompletedTransaction.add(logRecord.getTransactionID());
			}
		}
		
		pruneTransactionLogTable();
	}
	
	/**
	 * Removes transactions in list from the transaction log table. 
	 * @param lstTransactionToRemove
	 */
	private void removeTransactionFromTable(ArrayList<TransactionID> lstTransactionToRemove){
		for(TransactionID transactionID : lstTransactionToRemove){
			_htTransactionLogs.remove(transactionID);
		}
	}
	
	private void pruneTransactionLogTable(){
		ArrayList<TransactionID> lstTransactionID = _htTransactionLogs.keys();
		ArrayList<TransactionID> lstTransactionToDelete = new ArrayList<TransactionID>();
		
		for(TransactionID transactionID : lstTransactionID){
			ArrayList<LogRecord> lstLogRecord = _htTransactionLogs.get(transactionID);
			int lastCommitAbortLogRecordOffset = -1; 
			int lstLogRecordOffset;
			int searchStartOffset = lstLogRecord.size() - 1;
			
			if(lstLogRecord.get(searchStartOffset).getRecordType() == LogRecord.ABORT_LOG ||
					lstLogRecord.get(searchStartOffset).getRecordType() == LogRecord.COMMIT_LOG){
				// We ignore the last log if it's a commit/abort log.
				searchStartOffset--;
			}
			
			for(lstLogRecordOffset = searchStartOffset; lstLogRecordOffset >= 0; lstLogRecordOffset--){
				LogRecord logRecord = lstLogRecord.get(lstLogRecordOffset);
				
				if(logRecord.getRecordType() == LogRecord.ABORT_LOG || logRecord.getRecordType() == LogRecord.COMMIT_LOG){
					lastCommitAbortLogRecordOffset = lstLogRecordOffset;
					
					break;
				}
			}
			
			if(lastCommitAbortLogRecordOffset != -1 && lastCommitAbortLogRecordOffset < lstLogRecord.size() - 1){
				// Remove everything in the list from the beginning to where the last commit/abort log record was found.
				int recordsDeleted = 0;

				while(recordsDeleted <= lastCommitAbortLogRecordOffset){
					lstLogRecord.remove(0);

					recordsDeleted++;
				}
			}
			
			if(lstLogRecord.isEmpty()){
				lstTransactionToDelete.add(transactionID);
			}
		}
		
		// Remove TransactionIDs with empty logs.
		for(TransactionID transactionID : lstTransactionToDelete){
			_htTransactionLogs.remove(transactionID);
		}
	}
	
	/**
	 * Recovers transaction identified by transactionID
	 * @param transactionID
	 */
	public void recoverTransaction(TransactionID transactionID) throws TransactionException{
		ArrayList<LogRecord> lstTransactionLogs = _htTransactionLogs.get(transactionID);
		
		if(lstTransactionLogs != null && lstTransactionLogs.isEmpty() == false){
			boolean bStartTwoPCLogFound = recordTypeExists(lstTransactionLogs, LogRecord.START2PC_LOG);
			boolean bCommitLogFound = recordTypeExists(lstTransactionLogs, LogRecord.COMMIT_LOG);
			boolean bAbortLogFound = recordTypeExists(lstTransactionLogs, LogRecord.ABORT_LOG);
			
			if(bStartTwoPCLogFound){
				// The coordinator was hosted at this host.
				if(bCommitLogFound || bAbortLogFound){
					// Decision was reached before failure. Do nothing.
					System.out.println("DTRM: Decision made before failure. Nothing to do.");
				}else{
					// Decide to abort. Also, send aborts to all participants.
					RecoverAbortMessage recoverAbortMessage = new RecoverAbortMessage(transactionID);
					
					try{
						System.out.println("DTRM: Aborting at coordinator.");
						getTransactionManagerRemoteObj(_localPeerID).relayTwoPCMessage(recoverAbortMessage);
					}catch(Exception exception){
						throw new TransactionException(exception.getMessage());
					}
				}
			}else{
				// This host acted as a participant.
				if(bCommitLogFound || bAbortLogFound){
					// Decision was reached before failure. Do nothing.
					System.out.println("DTRM: Decision made before failure. Nothing to do.");
				}else{
					// Decide to abort. Also, send aborts to all participants.
					boolean bYesVoted = false;
					
					for(LogRecord logRecord : lstTransactionLogs){
						if(logRecord.getRecordType() == LogRecord.VOTE_LOG){
							VoteLogRecord voteLogRecord = (VoteLogRecord)logRecord;
							
							if(voteLogRecord.getVote() == VoteLogRecord.VOTE_YES){
								bYesVoted = true;
								
								break;
							}
						}
					}
					
					if(bYesVoted == false){
						// Participant had either voted no, or not voted at all.
						// In either case, abort unilaterally.
						RecoverAbortMessage recoverAbortMessage = new RecoverAbortMessage(transactionID);

						try{
							System.out.println("DTRM: Aborting at participant ID: " + _localPeerID);
							getTransactionManagerRemoteObj(_localPeerID).relayTwoPCMessage(recoverAbortMessage);
						}catch(Exception exception){
							throw new TransactionException(exception.getMessage());
						}
					}else{
						// Participant was in uncertainty period. Invoke CTP.
						StartCTPMessage startCTPMessage = new StartCTPMessage(transactionID);
						
						try{
							System.out.println("DTRM: Initiating CTP at participant ID: " + _localPeerID);
							getTransactionManagerRemoteObj(_localPeerID).relayTwoPCMessage(startCTPMessage);
						}catch(Exception exception){
							throw new TransactionException(exception.getMessage());
						}
					}
				}
			}			
		}else{
			// The participant logged nothing. So no action was taken for this
			// particular transaction and it can thus be aborted.
			RecoverAbortMessage recoverAbortMessage = new RecoverAbortMessage(transactionID);

			try{
				System.out.println("DTRM: Aborting at participant ID: " + _localPeerID);
				getTransactionManagerRemoteObj(_localPeerID).relayTwoPCMessage(recoverAbortMessage);
			}catch(Exception exception){
				throw new TransactionException(exception.getMessage());
			}
		}
	}
	
	private ArrayList<Integer> getParticipantsFromLogs(ArrayList<LogRecord> lstLogRecord){
		// The participant list can be found in start2PC and vote logs.
		for(LogRecord logRecord : lstLogRecord){
			if(logRecord.getRecordType() == LogRecord.START2PC_LOG){
				return ((StartTwoPCLogRecord)logRecord).getParticipants();
			}else if(logRecord.getRecordType() == LogRecord.VOTEREQUEST_LOG){
				return ((VoteLogRecord)logRecord).getParticipants();
			}
		}
		
		return null;
	}
	
	private ITransactionManager getTransactionManagerRemoteObj(int peerID){
		try{
			Peer originatingPeer = PeerIDKeyedMap.getPeer(peerID);
			Registry registry = LocateRegistry.getRegistry(originatingPeer.getPeerHostname(), originatingPeer.getPeerPortNumber());
			ITransactionManager transactionManagerRemoteObject = 
				(ITransactionManager) registry.lookup(originatingPeer.getPeerName() + "_TransactionManager");
			
			return transactionManagerRemoteObject;
		}catch(Exception exception){
			exception.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * Returns true if a log record of type logRecordType exists in the list of log records.
	 * @param lstLogRecord
	 * @return
	 */
	private boolean recordTypeExists(ArrayList<LogRecord> lstLogRecord, int logRecordType){
		for(LogRecord logRecord : lstLogRecord){
			if(logRecord.getRecordType() == logRecordType){
				return true;
			}
		}
		
		return false;
	}
}
