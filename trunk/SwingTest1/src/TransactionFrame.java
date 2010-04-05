import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

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
	private JScrollPane _scpOperations;
	private JPanel _pnlButtons;
	private int _currentOperationIndex;
		
	public TransactionFrame(Transaction transaction, ITransactionManager transactionManager){
		super("Transaction - " + transaction.toString());
		
		// Set graphical properties
		setSize(300, 200);
		setLayout(new FlowLayout());
		
		_lstModel = new DefaultListModel();
		_lstBoxOperations = new JList(_lstModel);
		_scpOperations = new JScrollPane(_lstBoxOperations);
		_btnNext = new JButton("Next");
		_btnCommit = new JButton("Commit");
		_btnAbort = new JButton("Abort");
		_pnlButtons = new JPanel();
		
		_pnlButtons.add(_btnNext);
		_pnlButtons.add(_btnCommit);
		_pnlButtons.add(_btnAbort);
		
		add(_scpOperations);
		add(_pnlButtons);
		
		// Set non-graphical properties.
		_transaction = transaction;
		_transactionManager = transactionManager;
		_currentOperationIndex = 0;
		
		populateOperationList();
		
		_lstBoxOperations.setSelectedIndex(_currentOperationIndex);		
	}
	
	private void populateOperationList(){
		for(Operation operation : _transaction.getOperations()){
			_lstModel.addElement(operation);
		}
	}

	@Override
	public void operationCompleted(Operation operation) {
		System.out.println(operation.toString() + " - completed.");		
	}
	
	public Transaction getTransaction(){
		return _transaction;
	}
	
	class TransactionButtonEventHandler implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent arg0) {
						
		}
		
	}
}
