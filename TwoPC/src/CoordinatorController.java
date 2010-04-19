import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.TimerTask;
import java.util.Timer;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.border.Border;

public class CoordinatorController extends JFrame implements ITwoPCController, IFailureEventListener{
	private static final int MAX_TRIES = 20;
	private static final int TIMER_INTERVAL = 500;
	private static final int TWOPC_STARTED = 0;
	private static final int NO_VOTE_SENT = 1;
	private static final int YES_VOTE_SENT = 1;
	private static final int DECISION_RECEIVED = 2;
	private static final int COMMITTED = 3;
	private static final int ABORTED = 4;
	private static final int VOTE_REQUESTED = 5;
	
	private JButton _btnVoteRequest;
//	private JButton _btnStart2PC;
	private JButton _btnVoteYesLocally;
	private JButton _btnVoteNoLocally;
	private JButton _btnCommit;
	private JButton _btnAbort;
	private JPanel _pnlStatus;
	private JScrollPane _jspStatus;
	private JPanel _pnlButtons;
	private JPanel _pnlVoteLocally;
	private JPanel _pnlCommitAbort;
	private JTextArea _txtStatus;
	
	private int _localPeerID = -1;
	private ArrayList<Integer> _lstParticipant; 
	private ArrayList<Integer> _lstVoteReplyPendingParticipants;
	private ITwoPCManager _twoPCManager;
	private TransactionID _transactionID;
	private Hashtable<Integer, VoteReplyMessage> _htParticipantVotingReplies;
	private TimerTask _voteReplyTimerTask;
	private Timer _voteReplyTimer;
	private int _transactionState;
	private boolean _bTransactionWounded;
	
	/**
	 * The controller is also registered with the TwoPCManager.
	 * @param transactionID
	 * @param twoPCManager
	 */
	public CoordinatorController(TransactionID transactionID, ArrayList<Integer> lstParticipant, ITwoPCManager twoPCManager){
		super("Coordinator");
		
		// Build UI.
		setSize(550, 450);		
		setLayout(new FlowLayout());
		
		_pnlStatus = new JPanel();
		_pnlButtons = new JPanel(new FlowLayout());
		_pnlCommitAbort = new JPanel(new FlowLayout());
		_pnlVoteLocally = new JPanel(new FlowLayout());
		
		_btnVoteRequest = new JButton("Request Vote");
		_btnVoteRequest.addActionListener(new ControllerButtonEventHandler());
		_btnVoteRequest.setEnabled(false);
		_pnlButtons.add(_btnVoteRequest);
		
//		_btnStart2PC = new JButton("Start 2PC");
//		_btnStart2PC.addActionListener(new ControllerButtonEventHandler());
//		_btnStart2PC.setEnabled(false);
//		_pnlButtons.add(_btnStart2PC);
		
		_btnVoteYesLocally = new JButton("Vote Yes Locally");
		_btnVoteYesLocally.addActionListener(new ControllerButtonEventHandler());
		_btnVoteYesLocally.setEnabled(false);
		_pnlVoteLocally.add(_btnVoteYesLocally);
		
		_btnVoteNoLocally = new JButton("Vote No Locally");
		_btnVoteNoLocally.addActionListener(new ControllerButtonEventHandler());
		_btnVoteNoLocally.setEnabled(false);
		_pnlVoteLocally.add(_btnVoteNoLocally);
		
		_pnlVoteLocally.setBorder(BorderFactory.createLineBorder(Color.black));
		
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
		
		this.add(_pnlButtons);
//		this.add(_pnlVoteLocally);
		this.add(_pnlCommitAbort);
		this.add(_pnlStatus);		
		
		_twoPCManager = twoPCManager;
		_transactionID = transactionID;
		
		if(GlobalState.containsKey("localPeerID")){
			_localPeerID = (Integer)GlobalState.get("localPeerID");		
			this.setTitle("Coordinator - Site:" + _localPeerID);
		}else{
			logTwoPCEvent("Error - The local peer ID was not available in GlobalState table.");			
		}
		
		_lstParticipant = new ArrayList<Integer>();
		
		if(lstParticipant != null){
			for(Integer participantID : lstParticipant){
				if(participantID != _localPeerID){
					// The local peer ID is assumed to be the coordinator if the
					// coordinator controller is running.
					_lstParticipant.add(participantID);
				}
			}
		}
		
		_lstVoteReplyPendingParticipants = new ArrayList<Integer>();
		_htParticipantVotingReplies = new Hashtable<Integer, VoteReplyMessage>();
		
		initialize();
	}
	
	private void initialize(){
		_twoPCManager.registerControllerForTransaction(_transactionID, this);
		_btnVoteRequest.setEnabled(true);
		FailureEventMonitor.getFailureEventMonitor().registerFailureEventListener(this);
		_bTransactionWounded = false;
		
		StartTwoPCMessage startTwoPCMessage = new StartTwoPCMessage(_localPeerID, _transactionID);
		
		for(Integer participantID : _lstParticipant){					
			ITransactionManager transactionManager = getTransactionManagerRemoteObj(participantID);
			
			try {
				_lstVoteReplyPendingParticipants.add(participantID);
				logTwoPCEvent("Signalling start of 2PC to " + 
						participantID + ":" + PeerIDKeyedMap.getPeer(participantID).getPeerName());
				transactionManager.relayTwoPCMessage(startTwoPCMessage);						
			} catch (Exception exception) {
				logTwoPCEvent(exception.getMessage());
				exception.printStackTrace();
			}
		}
	}
	
	@Override
	/**
	 * Processes a TwoPCMessage. The processing depends on the type
	 * of the message.
	 */
	public void processMessage(TwoPCMessage twoPCMessage) {
		switch(twoPCMessage.getMessageType()){
		case TwoPCMessage.VOTE_REPLY_MESSAGE:			
			// Vote reply message received from a participant.
			// Remove the sender from the list of pending voters.
			// Also, store the vote in table.
			VoteReplyMessage voteReplyMessage = (VoteReplyMessage)twoPCMessage;
			
			_lstVoteReplyPendingParticipants.remove(new Integer(voteReplyMessage.getParticipantID()));			
			_htParticipantVotingReplies.put(voteReplyMessage.getParticipantID(), voteReplyMessage);
			
			try {
				logTwoPCEvent("Received vote reply (" + 
						((voteReplyMessage.getVote() == VoteReplyMessage.NO_VOTE)?("NO"):("YES")) + ") " + "from " + 
						voteReplyMessage.getParticipantID() + ":" + PeerIDKeyedMap.getPeer(voteReplyMessage.getParticipantID()));
			} catch (InvalidPeerException e) {
				e.printStackTrace();
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
			
			if(_transactionState == COMMITTED){
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
			}else if(_transactionState == ABORTED){
				// I've aborted already. Reply with an ABORT message.
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
			}
			
			break;
		case TwoPCMessage.ABORT_RECOVER_MESSAGE:
			// Send the ABORT message to all participants. Also, attempt to commit locally.
			abortAll();
			
			// At this point, all buttons should be disabled.
			_btnCommit.setEnabled(false);
			_btnAbort.setEnabled(false);
			
			break;
		case TwoPCMessage.WOUND_MESSAGE:
			_bTransactionWounded = true;
			
			logTwoPCEvent("Transaction " + twoPCMessage.getSenderTransactionID().toString() + " wounded.");
			
			// Disable the commit button.
			_btnCommit.setEnabled(false);
			
			break;
		}
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
	
	private void logTwoPCEvent(String log){
		_txtStatus.append(log + "\n");
	}
	
	class ControllerButtonEventHandler implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent e) {
			if(e.getActionCommand().equals("Request Vote")){
				// Send a vote request to every participant.
				VoteRequestMessage voteRequestMessage = new VoteRequestMessage(_localPeerID, _lstParticipant, _transactionID);
				_lstVoteReplyPendingParticipants = new ArrayList<Integer>();
				
				for(Integer participantID : _lstParticipant){					
					ITransactionManager transactionManager = getTransactionManagerRemoteObj(participantID);
					
					try {
						_lstVoteReplyPendingParticipants.add(participantID);
						logTwoPCEvent("Sending vote request to " + 
								participantID + ":" + PeerIDKeyedMap.getPeer(participantID).getPeerName());
						transactionManager.relayTwoPCMessage(voteRequestMessage);						
					} catch (Exception exception) {
						logTwoPCEvent(exception.getMessage());
						exception.printStackTrace();
					}
				}
				
				// Log start-2PC to DTLog.
				DTLog.getLog().add(new StartTwoPCLogRecord(_transactionID, _lstParticipant));
				
				// Start the vote reply timer.
				_voteReplyTimerTask = new VoteReplyTimerTask(MAX_TRIES);
				_voteReplyTimer = new Timer();
				_voteReplyTimer.schedule(_voteReplyTimerTask, new Date(System.currentTimeMillis()), TIMER_INTERVAL);
				
				_btnVoteRequest.setEnabled(false);
			}else if(e.getActionCommand().equals("Commit")){
				// Send the COMMIT message to all participants. Also, attempt to commit locally.
				DTLog.getLog().add(new CommitLogRecord(_transactionID));
				
				for(Integer participantID : _lstParticipant){					
					ITransactionManager transactionManager = getTransactionManagerRemoteObj(participantID);
					CommitMessage commitMessage = new CommitMessage(participantID, _transactionID);
					
					try {
						logTwoPCEvent("Sending commit message to " + 
								participantID + ":" + PeerIDKeyedMap.getPeer(participantID).getPeerName());
						transactionManager.relayTwoPCMessage(commitMessage);
						
						// Test. Simulate coordinator going down after sending first commit out.
//						FailureEventMonitor.getFailureEventMonitor().triggerFailureEvent(new FailureEvent(FailureEvent.FAILURE_EVENT));
//						break;
						// Test ends.
					} catch (Exception exception) {
						logTwoPCEvent(exception.getMessage());
						exception.printStackTrace();
					}
				}
				
				// Send it to self.
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(_localPeerID);
				
				try {
					logTwoPCEvent("Sending commit message to self.");
					transactionManager.simulateCommitForPeer(_transactionID, _localPeerID);
					
					_transactionState = COMMITTED;					
				} catch (Exception exception) {
					logTwoPCEvent(exception.getMessage());
					exception.printStackTrace();
				}				
				
				// Disable the termination buttons.
				_btnCommit.setEnabled(false);
				_btnAbort.setEnabled(false);
			}else if(e.getActionCommand().equals("Abort")){
				// Send the ABORT message to all participants. Also, attempt to commit locally.
				abortAll();
				
				// At this point, all buttons should be disabled.
				_btnCommit.setEnabled(false);
				_btnAbort.setEnabled(false);
			}
		}		
	}
	
	/**
	 * Send abort messages to participants who voted 'Yes'.
	 */
	private void abortAll(){
		// Send the ABORT message to all participants. Also, attempt to commit locally.
		DTLog.getLog().add(new AbortLogRecord(_transactionID));
		boolean bAbortFailed = false;
		
		for(Integer participantID : _lstParticipant){
			// Send aborts only to those who voted YES.
			VoteReplyMessage voteReplyMessage = _htParticipantVotingReplies.get(new Integer(participantID));
			
			if(voteReplyMessage == null || voteReplyMessage.getVote() == VoteReplyMessage.YES_VOTE){
				ITransactionManager transactionManager = getTransactionManagerRemoteObj(participantID);
				AbortMessage abortMessage = new AbortMessage(participantID, _transactionID);

				try {
					logTwoPCEvent("Sending abort message to " + 
							participantID + ":" + PeerIDKeyedMap.getPeer(participantID).getPeerName());
					transactionManager.relayTwoPCMessage(abortMessage);						
				} catch (Exception exception) {
					logTwoPCEvent(exception.getMessage());
					exception.printStackTrace();
					
					bAbortFailed = true;
				}
			}
		}
		
		// Send it to self.
		ITransactionManager transactionManager = getTransactionManagerRemoteObj(_localPeerID);
		
		try {
			logTwoPCEvent("Sending abort message to self.");
			transactionManager.simulateAbortForPeer(_transactionID, _localPeerID);
		} catch (Exception exception) {
			logTwoPCEvent(exception.getMessage());
			exception.printStackTrace();
			
			bAbortFailed = true;
		}
		
		if(bAbortFailed == false){
			_transactionState = ABORTED;
		}
	}
	
	class VoteReplyTimerTask extends TimerTask{		
		private int _maxTries;
		
		public VoteReplyTimerTask(int maxTries){
			_maxTries = maxTries;
		}
		
		@Override
		public void run() {
			// Check if all vote replies have arrived.
			if(_lstVoteReplyPendingParticipants.isEmpty()){
				// All replies have arrived. We can cancel the timer.
				this.cancel();
				
				logTwoPCEvent("All voting replies received.");
				
				// Enable the buttons that'll allow the coordinator to vote locally.
				_btnVoteYesLocally.setEnabled(true);
				_btnVoteNoLocally.setEnabled(true);
								
				// If all replies were 'Yes', enabel the Commit button
				if(areAllParticipantVotesYes()){
					if(_bTransactionWounded == true){
						// Since the transaction has been wounded, the only
						// option open to the coordinator should be to abort.
						_btnCommit.setEnabled(false);
					}else{
						_btnCommit.setEnabled(true);
					}
					
					_btnAbort.setEnabled(true);					
				}else{
					_btnCommit.setEnabled(false);
					_btnAbort.setEnabled(true);
				}
			}
			
			if(_maxTries == 0){
				// Cancel the timer.
				this.cancel();
				
				// Timeout on voting replies has expired.
				logTwoPCEvent("Vote reply timeout expired.");				
				
				// Timeout action for vote-reply. Send aborts to yes voters.
				abortAll();
			}
			
			_maxTries--;			
		}

		private boolean areAllParticipantVotesYes() {
			Enumeration<Integer> participantIDs = _htParticipantVotingReplies.keys();
			
			while(participantIDs.hasMoreElements()){
				int participantID = participantIDs.nextElement();
				VoteReplyMessage voteReplyMessage = (VoteReplyMessage)_htParticipantVotingReplies.get(participantID);
				
				if(voteReplyMessage.getVote() == VoteReplyMessage.NO_VOTE){
					return false;
				}
			}
			
			return true;
		}		
	}

	@Override
	public void failureEventOccurred(FailureEvent failureEvent) {
		if(failureEvent.getFailureEventType() == FailureEvent.FAILURE_EVENT){
			// Disable all buttons. Closing the window is left to the user.
			_btnAbort.setEnabled(false);
			_btnCommit.setEnabled(false);
			_btnVoteNoLocally.setEnabled(false);
			_btnVoteRequest.setEnabled(false);
			_btnVoteYesLocally.setEnabled(false);
			
			// Cancel the vote reply timer.
			if(_voteReplyTimerTask != null){
				_voteReplyTimerTask.cancel();
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
