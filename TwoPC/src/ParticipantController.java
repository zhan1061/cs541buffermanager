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
	private int _transactionState;
	private int _voteCast = VoteReplyMessage.INVALID_VOTE;
	
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
		case TwoPCMessage.VOTE_REQUEST_MESSAGE:
			// This message signals the start of 2PC for the participant.
			VoteRequestMessage voteRequestMessage = (VoteRequestMessage)twoPCMessage;
			_coordinatorID = voteRequestMessage.getCoordinatorID();
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
			_btnVoteYes.setEnabled(true);
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
				_decisionWaitTimerTask.cancel();
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
				_decisionWaitTimerTask.cancel();
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
				AbortMessage abortMessage = new AbortMessage(decisionRequestMessage.getParticipantID(), _transactionID);

				try {
					logTwoPCEvent("Sending abort message (in reply to DECISION_REQUEST) to " + 
							decisionRequestMessage.getParticipantID() + ":" + PeerIDKeyedMap.getPeer(decisionRequestMessage.getParticipantID()).getPeerName());
					transactionManager.relayTwoPCMessage(abortMessage);						
				} catch (Exception exception) {
					logTwoPCEvent(exception.getMessage());
					exception.printStackTrace();
				}
			}else if(_transactionState == COMMITTED){
				//  I've committed by now. You can too.
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(decisionRequestMessage.getParticipantID());
				CommitMessage abortMessage = new CommitMessage(decisionRequestMessage.getParticipantID(), _transactionID);

				try {
					logTwoPCEvent("Sending commit message (in reply to DECISION_REQUEST) to " + 
							decisionRequestMessage.getParticipantID() + ":" + PeerIDKeyedMap.getPeer(decisionRequestMessage.getParticipantID()).getPeerName());
					transactionManager.relayTwoPCMessage(abortMessage);						
				} catch (Exception exception) {
					logTwoPCEvent(exception.getMessage());
					exception.printStackTrace();
				}
			}else{
				// I'm in the uncertainty period too. 
			}
			
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
					transactionManager.relayTwoPCMessage(voteReplyMessage);
					logTwoPCEvent("Sent yes vote to coordinator.");
					
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
					transactionManager.relayTwoPCMessage(voteReplyMessage);
					logTwoPCEvent("Sent no vote to coordinator.");
					
					_transactionState = NO_VOTE_SENT;
					_voteCast = VoteReplyMessage.NO_VOTE;
					
					// Disable all buttons.
					_btnVoteYes.setEnabled(false);
					_btnVoteNo.setEnabled(false);
					_btnCommit.setEnabled(false);
					_btnAbort.setEnabled(false);
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
				
				this.cancel();
				
				// TODO: Initiate CTP.
				logTwoPCEvent("Initiating Cooperative Termination Protocol.");
			}
				
			_maxTries--;			
		}		
	}// DecisionWaitTimerTask ends.

	@Override
	public void failureEventOccurred(FailureEvent failureEvent) {
		if(failureEvent.getFailureEventType() == FailureEvent.FAILURE_EVENT){
			// Disable all buttons.
			_btnAbort.setEnabled(false);
			_btnCommit.setEnabled(false);
			_btnVoteNo.setEnabled(false);
			_btnVoteYes.setEnabled(false);

			logTwoPCEvent("Site failed.");
		}else if (failureEvent.getFailureEventType() == FailureEvent.RECOVERY_EVENT){
			logTwoPCEvent("Site recovered.");
		}
	}
}
