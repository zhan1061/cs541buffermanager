import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimerTask;
import java.util.Timer;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class ParticipantController extends JFrame implements ITwoPCController, IFailureEventListener{
	private final int VOTE_REQUEST_TRIES = 20;
	private static final int TIMER_INTERVAL = 500;
	private static final int CTP_MESSAGE_INTERVAL = 1000;
	// Transaction states.
	private static final int TWOPC_STARTED = 0;
	private static final int NO_VOTE_SENT = 1;
	private static final int YES_VOTE_SENT = 1;
	private static final int DECISION_RECEIVED = 2;
	private static final int COMMITTED = 3;
	private static final int ABORTED = 4;
	private static final int VOTE_REQUESTED = 5;
	
	private JButton _btnVoteYes;
	private JButton _btnVoteNo;
	private JButton _btnCommit;
	private JButton _btnAbort;
	private JTextArea _txtStatus;
	private JScrollPane _jspStatus;
	private JPanel _pnlVote;
	private JPanel _pnlCommitAbort;
	private JPanel _pnlStatus;
	
	private int _localPeerID = -1;
	private ArrayList<Integer> _lstParticipant; 
	private ITwoPCManager _twoPCManager;
	private TransactionID _transactionID;
	private int _coordinatorID;
	private boolean _bVoteRequestReceived = false;
	private int _voteRequestWaitTries; // Each try is executed after 500 ms.
	private Timer _decisionWaitTimer;
	private TimerTask _decisionWaitTimerTask;
	private Timer _ctpTimer;
	private TimerTask _ctpTimerTask;
	private int _transactionState;
	private int _voteCast = VoteReplyMessage.INVALID_VOTE;
	private ArrayList<DecisionReplyMessage> _lstDecisionReplyMessage;
	private boolean _bTransactionWounded;
	
	/**
	 * The controller is also registered with the TwoPCManager.
	 * @param transactionID
	 * @param twoPCManager
	 */
	public ParticipantController(TransactionID transactionID, ITwoPCManager twoPCManager){
		super("Participant");

		// Build UI.
		setSize(550, 450);		
		setLayout(new FlowLayout());
		
		_pnlVote = new JPanel(new FlowLayout());
		_pnlCommitAbort = new JPanel(new FlowLayout());
		_pnlStatus = new JPanel();
		
		_btnVoteYes = new JButton("Vote Yes");
		_btnVoteYes.addActionListener(new ControllerButtonEventHandler());
		_btnVoteYes.setEnabled(false);
		_pnlVote.add(_btnVoteYes);
		
		_btnVoteNo = new JButton("Vote No");
		_btnVoteNo.addActionListener(new ControllerButtonEventHandler());
		_btnVoteNo.setEnabled(false);
		_pnlVote.add(_btnVoteNo);
		
		_pnlVote.setBorder(BorderFactory.createLineBorder(Color.black));
		
		_btnCommit = new JButton("Commit");
		_btnCommit.addActionListener(new ControllerButtonEventHandler());
		_btnCommit.setEnabled(false);
		_pnlCommitAbort.add(_btnCommit);
		
		_btnAbort = new JButton("Abort");
		_btnAbort.addActionListener(new ControllerButtonEventHandler());
		_btnAbort.setEnabled(false);
		_pnlCommitAbort.add(_btnAbort);
		
		_pnlCommitAbort.setBorder(BorderFactory.createLineBorder(Color.black));
		
		_txtStatus = new JTextArea();
		_jspStatus = new JScrollPane(_txtStatus);
		_jspStatus.setPreferredSize(new Dimension(450, 300));
		_pnlStatus.add(_jspStatus);
		
		this.add(_pnlVote);
		this.add(_pnlCommitAbort);
		this.add(_pnlStatus);		
		
		_twoPCManager = twoPCManager;
		_transactionID = transactionID;
		_transactionState = TWOPC_STARTED;
		
		if(GlobalState.containsKey("localPeerID")){
			_localPeerID = (Integer)GlobalState.get("localPeerID");		
			this.setTitle("Participant - Site:" + _localPeerID);
			logTwoPCEvent("Participating in 2PC for transaction with TID " + _transactionID.toString());
		}else{
			logTwoPCEvent("Error - The local peer ID was not available in GlobalState table.");			
		}
		
		_lstParticipant = new ArrayList<Integer>();
		_lstDecisionReplyMessage = new ArrayList<DecisionReplyMessage>();
		
		initialize();
	}
	
	private void initialize(){
		_twoPCManager.registerControllerForTransaction(_transactionID, this);
		FailureEventMonitor.getFailureEventMonitor().registerFailureEventListener(this);
	}
	
	private ITransactionManager getTransactionManagerRemoteObj(int peerID){
		try{
			Peer originatingPeer = PeerIDKeyedMap.getPeer(peerID);
			Registry registry = LocateRegistry.getRegistry(originatingPeer.getPeerHostname(), originatingPeer.getPeerPortNumber());
			System.out.println(originatingPeer);
			ITransactionManager transactionManagerRemoteObject = 
				(ITransactionManager) registry.lookup(originatingPeer.getPeerName() + "_TransactionManager");
			
			return transactionManagerRemoteObject;
		}catch(Exception exception){
			logTwoPCEvent(exception.getMessage());
			exception.printStackTrace();
		}
		
		return null;
	}
	
	@Override
	/**
	 * Processes a message based on its type.
	 */
	public void processMessage(TwoPCMessage twoPCMessage) {		
		switch(twoPCMessage.getMessageType()){
		case TwoPCMessage.STARTTWOPC_MESSAGE:
			// Get the coordinator ID out of this message.
			StartTwoPCMessage startTwoPCMessage = (StartTwoPCMessage)twoPCMessage;
			_coordinatorID = startTwoPCMessage.getCoordinatorID();
			
			logTwoPCEvent("2PC started.");
			
			break;
		case TwoPCMessage.VOTE_REQUEST_MESSAGE:
			// This message signals the start of 2PC for the participant.
			VoteRequestMessage voteRequestMessage = (VoteRequestMessage)twoPCMessage;
//			_coordinatorID = voteRequestMessage.getCoordinatorID();
			_lstParticipant = voteRequestMessage.getParticipants();			
			// Set variable indicating the vote-request has been received.
			_bVoteRequestReceived = true;
			_transactionState = VOTE_REQUESTED;
			
			try {
				logTwoPCEvent("Received vote request from " + 
						_coordinatorID + ":" + PeerIDKeyedMap.getPeer(_coordinatorID));
			} catch (InvalidPeerException e) {
				e.printStackTrace();
			}
			
			// Enable the vote buttons.
			if(_bTransactionWounded == true){
				_btnVoteYes.setEnabled(false);
			}else{
				_btnVoteYes.setEnabled(true);
			}
			
			_btnVoteNo.setEnabled(true);
			
			break;
		case TwoPCMessage.COMMIT_MESSAGE:
			// This check ensures that if we receive multiple COMMIT/ABORT messages (as part of CTP, for instance),
			// we only process them once.
			if(_transactionState != DECISION_RECEIVED && _transactionState != COMMITTED && _transactionState != ABORTED){
				_transactionState = DECISION_RECEIVED;
				logTwoPCEvent("Received COMMIT from coordinator.");
				
				// Cancel the decision wait timer.
				// We can only commit if the 'Commit' button is clicked.
				if(_decisionWaitTimerTask != null){
					_decisionWaitTimerTask.cancel();
				}
				
				if(_ctpTimerTask != null){
					_ctpTimerTask.cancel();
					_ctpTimerTask = null;
				}
				
				// The log to commit won't be written until the 'Commit' button
				// is pressed.
				_btnCommit.setEnabled(true);
			}
			
			break;
		case TwoPCMessage.ABORT_MESSAGE:
			// This check ensures that if we receive multiple COMMIT/ABORT messages (as part of CTP, for instance),
			// we only process them once.
			if(_transactionState != DECISION_RECEIVED && _transactionState != COMMITTED && _transactionState != ABORTED){
				_transactionState = DECISION_RECEIVED;
				logTwoPCEvent("Received ABORT from coordinator.");
				
				// Cancel the decision wait timer.
				if(_decisionWaitTimerTask != null){
					_decisionWaitTimerTask.cancel();
				}
				
				// Cancel the CTP timer if it is running.
				if(_ctpTimerTask != null){
					_ctpTimerTask.cancel();
					_ctpTimerTask = null;
				}
				
				// We can abort here without further user-input.			
				DTLog.getLog().add(new AbortLogRecord(_transactionID));
				logTwoPCEvent("Aborting.");
				abort();			
			}
			
			break;
		case TwoPCMessage.ABORT_RECOVER_MESSAGE:
			// This check ensures that if we receive multiple COMMIT/ABORT messages (as part of CTP, for instance),
			// we only process them once.
			if(_transactionState != DECISION_RECEIVED && _transactionState != COMMITTED && _transactionState != ABORTED){
				_transactionState = DECISION_RECEIVED;
				logTwoPCEvent("Received ABORT from recovery manager.");
				// Cancel the decision wait timer.
				if(_decisionWaitTimerTask != null){
					_decisionWaitTimerTask.cancel();
				}
				
				if(_ctpTimerTask != null){
					_ctpTimerTask.cancel();
					_ctpTimerTask = null;
				}
				
				// We can abort here without further user-input.			
				DTLog.getLog().add(new AbortLogRecord(_transactionID));
				logTwoPCEvent("Aborting.");
				abort();			
			}
			
			break;
		case TwoPCMessage.DECISION_REQUEST_MESSAGE:
			DecisionRequestMessage decisionRequestMessage = (DecisionRequestMessage)twoPCMessage;
			
			try {
				logTwoPCEvent("Received DECISION_REQ from " + 
						decisionRequestMessage.getParticipantID() + ":" + 
						PeerIDKeyedMap.getPeer(decisionRequestMessage.getParticipantID()).getPeerName());
			} catch (InvalidPeerException e) {
				e.printStackTrace();
			}
			
			if(_voteCast != VoteReplyMessage.YES_VOTE || _transactionState == ABORTED){
				// I haven't voted yes yet or I've aborted already. Reply with
				// an ABORT message.
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(decisionRequestMessage.getParticipantID());
				DecisionReplyMessage decisionReplyMessage = new DecisionReplyMessage(_localPeerID, DecisionReplyMessage.ABORT_DECISION_REPLY, _transactionID);
				
				try {
					logTwoPCEvent("Sending abort message (in reply to DECISION_REQUEST) to " + 
							decisionRequestMessage.getParticipantID() + ":" + PeerIDKeyedMap.getPeer(decisionRequestMessage.getParticipantID()).getPeerName());
					transactionManager.relayTwoPCMessage(decisionReplyMessage);						
				} catch (Exception exception) {
					logTwoPCEvent(exception.getMessage());
					exception.printStackTrace();
				}
			}else if(_transactionState == COMMITTED){
				//  I've committed by now. You can too.
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(decisionRequestMessage.getParticipantID());
				DecisionReplyMessage decisionReplyMessage = new DecisionReplyMessage(_localPeerID, DecisionReplyMessage.COMMIT_DECISION_REPLY, _transactionID);
				
				try {
					logTwoPCEvent("Sending commit message (in reply to DECISION_REQUEST) to " + 
							decisionRequestMessage.getParticipantID() + ":" + PeerIDKeyedMap.getPeer(decisionRequestMessage.getParticipantID()).getPeerName());
					transactionManager.relayTwoPCMessage(decisionReplyMessage);						
				} catch (Exception exception) {
					logTwoPCEvent(exception.getMessage());
					exception.printStackTrace();
				}
			}else{
				// I'm in the uncertainty period too. 
			}
			
			break;
		case TwoPCMessage.DECISION_REPLY_MESSAGE:
			if(_transactionState != COMMITTED && _transactionState != ABORTED){
				// Essentially, we only process the first decision reply message.
				DecisionReplyMessage decisionReplyMessage = (DecisionReplyMessage)twoPCMessage;

				try {
					logTwoPCEvent("Decision reply message received from " + decisionReplyMessage.getSenderID() + ":" + 
							PeerIDKeyedMap.getPeer(decisionReplyMessage.getSenderID()).getPeerName());
				} catch (InvalidPeerException e) {					
				}				

				// We can cancel the CTP timer since we've received a decision reply.
				if(_ctpTimerTask != null){
					_ctpTimerTask.cancel();
					_ctpTimerTask = null;
				}
				
				synchronized(_lstDecisionReplyMessage){
					if(_lstDecisionReplyMessage.isEmpty()){
						_lstDecisionReplyMessage.add(decisionReplyMessage);

						// Decide what to do based on the decision type.
						switch(decisionReplyMessage.getDecisionReplyType()){
						case DecisionReplyMessage.COMMIT_DECISION_REPLY:
							DTLog.getLog().add(new CommitLogRecord(_transactionID));
							logTwoPCEvent("Wrote commit to log.");
							commit();
							_btnCommit.setEnabled(false);
							_btnAbort.setEnabled(false);
							
							break;
						case DecisionReplyMessage.ABORT_DECISION_REPLY:
							DTLog.getLog().add(new AbortLogRecord(_transactionID));
							logTwoPCEvent("Aborting.");
							abort();
							_btnCommit.setEnabled(false);
							_btnAbort.setEnabled(false);

							break;
						}
					}
				}
			}
			
			break;
		case TwoPCMessage.START_CTP_MESSAGE:
			// Start the CTP task
			if(_ctpTimerTask != null){
				_ctpTimerTask.cancel();
			}
			
			_ctpTimer = new Timer();
			_ctpTimerTask = new CTPTimerTask();
			_ctpTimer.schedule(_ctpTimerTask, new Date(System.currentTimeMillis()), TIMER_INTERVAL);
			
			break;
		case TwoPCMessage.WOUND_MESSAGE:
			_bTransactionWounded = true;
			
			logTwoPCEvent("Transaction " + twoPCMessage.getSenderTransactionID().toString() + " wounded.");
			
			// Disable the commit and vote yes buttons.
			_btnVoteYes.setEnabled(false);
			_btnCommit.setEnabled(false);
			
			break;		
		}
	}
	
	private void commit(){
		ITransactionManager transactionManager = getTransactionManagerRemoteObj(_coordinatorID);
		
		try{
			// We ask the coordinator to commit for us. Hence 'simulateCommit'.
			transactionManager.simulateCommitForPeer(_transactionID, _localPeerID);
			logTwoPCEvent("Committed transaction " + _transactionID.toString());
			_transactionState = COMMITTED;
		}catch(RemoteException remoteException){
			logTwoPCEvent("Error while committing transaction " + _transactionID.toString());
			remoteException.printStackTrace();
		}
	}
	
	private void abort(){
		ITransactionManager transactionManager = getTransactionManagerRemoteObj(_coordinatorID);
		
		try{
			// We ask the coordinator to abort for us. Hence 'simulateAbort'.
			transactionManager.simulateAbortForPeer(_transactionID, _localPeerID);
			logTwoPCEvent("Aborted.");
			_transactionState = ABORTED;
		}catch(RemoteException remoteException){
			logTwoPCEvent("Error while committing transaction " + _transactionID.toString());
			remoteException.printStackTrace();
		}
	}
	
	private void logTwoPCEvent(String log){
		_txtStatus.append(log + "\n");
	}
	
	/**
	 * Initiates the Cooperative Termination Protocol.
	 */
	private void startCTP(){
		logTwoPCEvent("Initiating Cooperative Termination Protocol.");
		
		// Send decision request messages to all participants.
		for(Integer participantID : _lstParticipant){
			if(participantID != _localPeerID){
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(participantID);
				DecisionRequestMessage decisionRequestMessage = new DecisionRequestMessage(_localPeerID, _transactionID);

				try {
					logTwoPCEvent("Sending decision request message to " + 
							participantID + ":" + PeerIDKeyedMap.getPeer(participantID).getPeerName());
					transactionManager.relayTwoPCMessage(decisionRequestMessage);						
				} catch (Exception exception) {
					// Any participants who're down aren't expected to reply anyway.
				}
			}
		}
		
		// Send decision request to the coordinator as well.
		DecisionRequestMessage decisionRequestMessage = new DecisionRequestMessage(_localPeerID, _transactionID);
		
		try {
			logTwoPCEvent("Sending decision request message to " + 
					_coordinatorID + ":" + PeerIDKeyedMap.getPeer(_coordinatorID).getPeerName());
			getTransactionManagerRemoteObj(_coordinatorID).relayTwoPCMessage(decisionRequestMessage);						
		} catch (Exception exception) {
			// Any participants who're dow aren't expected to reply anyway.
		}
	}
	
	class ControllerButtonEventHandler implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent e) {
			if(e.getActionCommand().equals("Vote Yes")){
				// Send a 'Yes' vote to the coordinator.
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(_coordinatorID);
				VoteReplyMessage voteReplyMessage = new VoteReplyMessage(_localPeerID, VoteReplyMessage.YES_VOTE, _transactionID);
				
				// Write 'yes' to DTLog.
				DTLog.getLog().add(new VoteLogRecord(_transactionID, VoteLogRecord.VOTE_YES, _lstParticipant));
				logTwoPCEvent("Wrote yes to log.");
				
				// Send the 'YES' vote.
				try{
					_transactionState = YES_VOTE_SENT;
					_voteCast = VoteReplyMessage.YES_VOTE;
					
					// Get the decision wait timer going.
					_decisionWaitTimerTask = new DecisionWaitTimerTask(VOTE_REQUEST_TRIES);
					_decisionWaitTimer = new Timer();
					_decisionWaitTimer.schedule(_decisionWaitTimerTask, new Date(System.currentTimeMillis()), TIMER_INTERVAL);
					
					// Disable the 'Abort' button since we've voted yes.
					_btnAbort.setEnabled(false);
					// Disabe the voting buttons.
					_btnVoteYes.setEnabled(false);
					_btnVoteNo.setEnabled(false);
					
					transactionManager.relayTwoPCMessage(voteReplyMessage);
					logTwoPCEvent("Sent yes vote to coordinator.");
				}catch(Exception exception){
					logTwoPCEvent("Error sending vote - " + exception.getMessage());
					exception.printStackTrace();
				}
			}else if(e.getActionCommand().equals("Vote No")){
				// Send a 'Yes' vote to the coordinator.
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(_coordinatorID);
				VoteReplyMessage voteReplyMessage = new VoteReplyMessage(_localPeerID, VoteReplyMessage.NO_VOTE, _transactionID);
				
				// Write abort log and abort.
				DTLog.getLog().add(new AbortLogRecord(_transactionID));
				logTwoPCEvent("Wrote abort to log.");
				abort();
				
				// Send the 'NO' vote.
				try{
					_voteCast = VoteReplyMessage.NO_VOTE;
					
					// Disable all buttons.
					_btnVoteYes.setEnabled(false);
					_btnVoteNo.setEnabled(false);
					_btnCommit.setEnabled(false);
					_btnAbort.setEnabled(false);
					
					transactionManager.relayTwoPCMessage(voteReplyMessage);
					logTwoPCEvent("Sent no vote to coordinator.");
				}catch(Exception exception){
					logTwoPCEvent("Error sending vote - " + exception.getMessage());
					exception.printStackTrace();
				}
			}else if(e.getActionCommand().equals("Commit")){
				// Write commit log and commit.
				DTLog.getLog().add(new CommitLogRecord(_transactionID));
				logTwoPCEvent("Wrote commit to log.");
				commit();
				_btnCommit.setEnabled(false);
			}
		}		
	}// ControllerButtonEventHandler ends.
	
	class DecisionWaitTimerTask extends TimerTask{
		private int _maxTries;
		
		public DecisionWaitTimerTask(int maxTries){
			_maxTries = maxTries;
		}
		
		@Override
		public void run() {
			
			if(_maxTries == 0){
				logTwoPCEvent("Decision wait timeout expired.");
				cancel();
				
				// Start the CTP task
				if(_ctpTimerTask != null){
					_ctpTimerTask.cancel();
				}
				
				_ctpTimer = new Timer();
				_ctpTimerTask = new CTPTimerTask();
				_ctpTimer.schedule(_ctpTimerTask, new Date(System.currentTimeMillis()), CTP_MESSAGE_INTERVAL);
			}
				
			_maxTries--;			
		}		
	}// DecisionWaitTimerTask ends.
	
	class CTPTimerTask extends TimerTask{
		public CTPTimerTask(){
		
		}
		
		@Override
		public void run() {
			startCTP();					
		}		
	}// CTPTimerTask ends.

	@Override
	public void failureEventOccurred(FailureEvent failureEvent) {
		if(failureEvent.getFailureEventType() == FailureEvent.FAILURE_EVENT){
			// Disable all buttons.
			_btnAbort.setEnabled(false);
			_btnCommit.setEnabled(false);
			_btnVoteNo.setEnabled(false);
			_btnVoteYes.setEnabled(false);

			// Shut down the decision timer.
			if(_decisionWaitTimerTask != null){
				_decisionWaitTimerTask.cancel();
			}
			
			logTwoPCEvent("Site failed.");
		}else if (failureEvent.getFailureEventType() == FailureEvent.RECOVERY_EVENT){
			DTRecoveryManager dtRecoveryManager = new DTRecoveryManager();
			
			try{
				dtRecoveryManager.recoverTransaction(_transactionID);
				logTwoPCEvent("Site recovered.");
			}catch(Exception exception){
				logTwoPCEvent(exception.getMessage());
			}			
		}
	}
}
