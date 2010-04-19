import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.RemoteException;
import java.util.ArrayList;

import javax.swing.*;

/**
 * A graphical representation of a transaction in execution. 
 */
public class TransactionFrame extends JFrame implements IOperationCompletedEventHandler {
	private Transaction _transaction;
	private ITransactionManager _transactionManager;
	private JList _lstBoxOperations;
	private DefaultListModel _lstModel;
	private JButton _btnNext;
	private JButton _btnCommit;
	private JButton _btnAbort;
	private JButton _btnStartTwoPC;
	private JScrollPane _scpOperations;
	private JPanel _pnlButtons;
	private int _currentOperationIndex;
	private ArrayList<Integer> _lstTargetServerID = null;
	private ITwoPCController _twoPCController = null;
	private int _localPeerID = -1;
	private boolean _bOperationsStarted = false;
	
	public TransactionFrame(Transaction transaction, ITransactionManager transactionManager){
		super("Transaction - " + transaction.toString());
		
		// Set graphical properties
		setSize(300, 200);
		setLayout(new FlowLayout());
		
		_lstModel = new DefaultListModel();
		_lstBoxOperations = new JList(_lstModel);
		_scpOperations = new JScrollPane(_lstBoxOperations);
		
		_btnNext = new JButton("Execute");
		_btnNext.addActionListener(new TransactionButtonEventHandler());
		_btnNext.setEnabled(true);
		
		_btnCommit = new JButton("Commit");
		_btnCommit.addActionListener(new TransactionButtonEventHandler());
		_btnCommit.setEnabled(false);
		
		_btnAbort = new JButton("Abort");
		_btnAbort.addActionListener(new TransactionButtonEventHandler());
		_btnAbort.setEnabled(true);
		
		_btnStartTwoPC = new JButton("Start 2PC");
		_btnStartTwoPC.addActionListener(new TransactionButtonEventHandler());
		_btnStartTwoPC.setEnabled(false);
		
		_pnlButtons = new JPanel();
		
		_pnlButtons.add(_btnNext);
		_pnlButtons.add(_btnStartTwoPC);
//		_pnlButtons.add(_btnCommit);
//		_pnlButtons.add(_btnAbort);
		
		add(_scpOperations);
		add(_pnlButtons);
		
		// Set non-graphical properties.
		_transaction = transaction;
		_transactionManager = transactionManager;
		_currentOperationIndex = 0;
		
		if(GlobalState.containsKey("localPeerID")){
			_localPeerID = (Integer)GlobalState.get("localPeerID");		
			this.setTitle("Coordinator - Site:" + _localPeerID);
		}
		
		populateOperationList();
		_lstTargetServerID = new ArrayList<Integer>();
		
		_lstBoxOperations.setSelectedIndex(_currentOperationIndex);
	}
	
	private void populateOperationList(){
		for(IOperation operation : _transaction.getOperations()){
			_lstModel.addElement(operation);
		}
	}

	/**
	 * Aborts the encapsulated transaction. It sends the abort
	 * command to every scheduler that was in communication during the
	 * transaction.
	 */
	public void abort(){
		try {
			// We may have to generate multiple abort operations
			// if we talked to multiple schedulers during a transaction.
			for(Integer targetServerID : _lstTargetServerID){
				AbortOperation abortOperation = new AbortOperation(_transaction);
				
				abortOperation.setTargetServerID(targetServerID);
				
				try{
					_transactionManager.executeOperation(abortOperation);
				}catch(TransactionException transactionException){
					// Do nothing if abort fails.
				}
			}
			
			_btnCommit.setEnabled(false);
			_btnAbort.setEnabled(false);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Aborts the encapsulated transaction. It sends the abort
	 * command to the participant's scheduler.
	 */
	public void abortPeer(int peerID){
		try {
			// We may have to generate multiple abort operations
			// if we talked to multiple schedulers during a transaction.
			if(_lstTargetServerID.contains(new Integer(peerID))){
				AbortOperation abortOperation = new AbortOperation(_transaction);

				abortOperation.setTargetServerID(peerID);

				try{
					_transactionManager.executeOperation(abortOperation);
				}catch(TransactionException transactionException){
					// Do nothing if abort fails.
				}

				_btnCommit.setEnabled(false);
				_btnAbort.setEnabled(false);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	public void commit(){
		try {
			// We may have to generate multiple commit operations
			// if we talked to multiple schedulers during a transaction.
			for(Integer targetServerID : _lstTargetServerID){
				CommitOperation commitOperation = new CommitOperation(_transaction);
				
				commitOperation.setTargetServerID(targetServerID);
				_transactionManager.executeOperation(commitOperation);
			}
			
			_btnCommit.setEnabled(false);
			_btnAbort.setEnabled(false);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (TransactionException e) {
			// Try to abort.
			// We may have to generate multiple commit operations
			// if we talked to multiple schedulers during a transaction.
			for(Integer targetServerID : _lstTargetServerID){
				AbortOperation abortOperation = new AbortOperation(_transaction);
				
				abortOperation.setTargetServerID(targetServerID);
				
				try{
					_transactionManager.executeOperation(abortOperation);
				}catch(Exception exception){
					// Do nothing if abort fails.
				}
			}
		}
	}
	
	/**
	 * Commits the encapsulated transaction. It sends the abort
	 * command to the participant's scheduler.
	 */
	public void commitPeer(int peerID){
		if(_lstTargetServerID.contains(new Integer(peerID))){
			try {			
				CommitOperation commitOperation = new CommitOperation(_transaction);

				commitOperation.setTargetServerID(peerID);
				_transactionManager.executeOperation(commitOperation);			

				_btnCommit.setEnabled(false);
				_btnAbort.setEnabled(false);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (TransactionException e) {
				// Try to abort.
				// We may have to generate multiple commit operations
				// if we talked to multiple schedulers during a transaction.			
				AbortOperation abortOperation = new AbortOperation(_transaction);

				abortOperation.setTargetServerID(peerID);

				try{
					_transactionManager.executeOperation(abortOperation);
				}catch(Exception exception){
					// Do nothing if abort fails.
				}			
			}
		}
	}
	
	@Override
	public void operationCompleted(IOperation operation) {
		System.out.println(operation.toString() + " - completed.");
		_currentOperationIndex++;
		
		if(_currentOperationIndex < _lstModel.getSize()){
			_btnNext.setEnabled(true);
			_btnAbort.setEnabled(true);
			
			// Highlight the next operation.
			_lstBoxOperations.setSelectedIndex(_currentOperationIndex);
		}else if (_currentOperationIndex == _lstModel.getSize()){
			_btnCommit.setEnabled(true);
			_btnAbort.setEnabled(true);
			_btnStartTwoPC.setEnabled(true);
			_btnNext.setEnabled(false);
		}
	}
	
	public Transaction getTransaction(){
		return _transaction;
	}
	
	public void processTwoPCMessage(TwoPCMessage twoPCMessage){
		// Pass the message on to the associated TwoPCController.
		if(_twoPCController == null){
			// If the TwoPCController is null when the message is
			// received, this must necessarily be the participant site.
//			_twoPCController = new ParticipantController(_transaction, lstParticipant, twoPCManager)
		}
	}
	
	class TransactionButtonEventHandler implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent event) {
			if(event.getActionCommand().equals("Execute")){
				// Ask the TM to execute the next operation.
				try {
					IOperation operationToExecute = _transaction.getOperations().get(_currentOperationIndex);
					
					// If the target server ID isn't already known, add it to our list.
					if(_lstTargetServerID.contains(operationToExecute.getTargetServerID()) == false){
						_lstTargetServerID.add(operationToExecute.getTargetServerID());
						_transaction.setTotalTargetServers(_lstTargetServerID.size());
					}
					
					try{
						_transactionManager.executeOperation(operationToExecute);
						_btnNext.setEnabled(false);
						_btnAbort.setEnabled(false);						
						_bOperationsStarted = true;
					}catch(TransactionException transactionException){
						if(_bOperationsStarted){
							// Try to abort.
							// We may have to generate multiple commit operations
							// if we talked to multiple schedulers during a transaction.
							for(Integer targetServerID : _lstTargetServerID){
								AbortOperation abortOperation = new AbortOperation(_transaction);

								abortOperation.setTargetServerID(targetServerID);

								try{
									_transactionManager.executeOperation(abortOperation);
								}catch(Exception exception){
									// Do nothing if abort fails.
								}
							}
						}
					}
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}else if(event.getActionCommand().equals("Commit")){
				try {
					// We may have to generate multiple commit operations
					// if we talked to multiple schedulers during a transaction.
					for(Integer targetServerID : _lstTargetServerID){
						CommitOperation commitOperation = new CommitOperation(_transaction);
						
						commitOperation.setTargetServerID(targetServerID);
						_transactionManager.executeOperation(commitOperation);
					}
					
					_btnCommit.setEnabled(false);
					_btnAbort.setEnabled(false);
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (TransactionException e) {
					// Try to abort.
					// We may have to generate multiple commit operations
					// if we talked to multiple schedulers during a transaction.
					for(Integer targetServerID : _lstTargetServerID){
						AbortOperation abortOperation = new AbortOperation(_transaction);
						
						abortOperation.setTargetServerID(targetServerID);
						
						try{
							_transactionManager.executeOperation(abortOperation);
						}catch(Exception exception){
							// Do nothing if abort fails.
						}
					}
				}
			}else if(event.getActionCommand().equals("Abort")){
				try {
					// We may have to generate two multiple commit operations
					// if we talked to multiple schedulers during a transaction.
					for(Integer targetServerID : _lstTargetServerID){
						AbortOperation abortOperation = new AbortOperation(_transaction);
						
						abortOperation.setTargetServerID(targetServerID);
						
						try{
							_transactionManager.executeOperation(abortOperation);
						}catch(TransactionException transactionException){
							// Do nothing it abort fails.
						}
					}
					
					_btnCommit.setEnabled(false);
					_btnAbort.setEnabled(false);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}else if(event.getActionCommand().equals("Start 2PC")){
				// Prepare the list of participants.
				ArrayList<Integer> lstParticipant = new ArrayList<Integer>();
				
				for(Integer targetServerID : _lstTargetServerID){
					if(lstParticipant.contains(targetServerID) == false && targetServerID != _localPeerID){
						lstParticipant.add(targetServerID);
//						System.out.println("Adding participant: " + targetServerID);
					}					
				}
				
				// 2PC can only be started once. So disable the button.
				_btnStartTwoPC.setEnabled(false);
				
				CoordinatorController coordinatorController = 
					new CoordinatorController(_transaction.getTransactionID(), lstParticipant, 
							(ITwoPCManager)_transactionManager);
				
				coordinatorController.setVisible(true);
			}
		}
		
	}
}
