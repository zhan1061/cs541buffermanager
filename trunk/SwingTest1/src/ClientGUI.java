import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

public class ClientGUI extends JPanel {

  private JTextField[] fields;
  private JTextField response;
  boolean canConnect = false;
  String serverName;
  String hostName;
  String port;
  boolean busy = false;
  private ArrayList <String> lastActionResult = new ArrayList<String>();  
  
  public ClientGUI(String[] labels, int[] widths) {
    super(new BorderLayout());
    final JTabbedPane tab = new JTabbedPane();
    
    JPanel connectPanel = new JPanel(new GridLayout(2,3));
    JPanel depositPanel = new JPanel(new GridLayout(2,2));
    JPanel withdrawPanel = new JPanel(new GridLayout(2,2));
    JPanel transferPanel = new JPanel(new GridLayout(3,1));
    JPanel balancePanel = new JPanel(new GridLayout(3,1));
    JPanel createAccountPanel = new JPanel(new GridLayout());
    
    
    JPanel labelPanel = new JPanel(new GridLayout(labels.length, 1));
    JPanel fieldPanel = new JPanel(new GridLayout(labels.length, 1));
    JPanel responseFieldPanel = new JPanel(new GridLayout(1, 1));
    JPanel buttonsPanel = new JPanel(new GridLayout(1, 2));
    connectPanel.add(labelPanel, BorderLayout.WEST);
    connectPanel.add(fieldPanel, BorderLayout.CENTER);
    connectPanel.add(responseFieldPanel, BorderLayout.EAST);
    connectPanel.add(buttonsPanel, BorderLayout.SOUTH);

//    add(labelPanel, BorderLayout.WEST);
//    add(fieldPanel, BorderLayout.CENTER);
//    add(responseFieldPanel, BorderLayout.EAST);
//    
    fields = new JTextField[labels.length];
    response = new JTextField();
    response.setColumns(20);
    for (int i = 0; i < labels.length; i += 1) {
      fields[i] = new JTextField();
      if (i < widths.length)
        fields[i].setColumns(widths[i]);

      JLabel lab = new JLabel(labels[i], JLabel.RIGHT);
      lab.setLabelFor(fields[i]);

      labelPanel.add(lab);
      JPanel p = new JPanel(new FlowLayout(FlowLayout.LEFT));
      p.add(fields[i]);
      fieldPanel.add(p);
      
    }
    JButton query = new JButton("Query");
    JButton reset = new JButton("Reset");
    buttonsPanel.add(query);
    buttonsPanel.add(reset);
    query.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        System.out.println(getText(0) + " " + getText(1) + ". " + getText(2));
        if(("".equals(fields[0].getText().trim()))||("".equals(fields[1].getText().trim()))||("".equals(fields[2].getText().trim())) )
        {
        	response.setText("plz enter all the connection details...");
        }
        else {
        canConnect = true;
        serverName = fields[0].getText().trim();
        hostName = fields[1].getText().trim();
    	port = fields[2].getText().trim();
    	response.setText("all the connection details present. thanks");
        }
        
      }
    });
    
    reset.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
        	for (int i =0; i <fields.length; i++)
        	{
        		fields[i].setText("");
        	}
        	serverName = "";
        	hostName = "";
        	port = "";
        	canConnect = false;
        	response.setText("cleared details...");
        	
        }
      });
    responseFieldPanel.add(response);
    tab.addTab("Connect To", connectPanel);
////////////////////////////////////////////////
    JPanel createAccountButtonsPanel = new JPanel(new GridLayout(2, 2));
    JButton createAccountButton = new JButton("Create new account");
    
//    final JTextField createAccountResponse = new JTextField();
//    createAccountResponse.setColumns(20);
//    
    final JTextArea createAccountResponse = new JTextArea(null, 5, 20);
    createAccountResponse.setLineWrap(true);
    JScrollPane createAccountResponseScroll = new JScrollPane(createAccountResponse);
    createAccountResponseScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);

    createAccountButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
          //create a transaction obj
        System.out.println("accnt create button clicked");
        
        if(!canConnect)
        {
                createAccountResponse.append("goto Connect to tab and enter connection details first\n");
        }
        else if(busy == true){
                createAccountResponse.append("try later... system executing another transaction currently\n");
        }
        else{
                tab.setVisible(false);
                busy = true;
                createAccountResponse.append("trying to create new account...\n");
                
                Registry registry;
                try {
                        registry = LocateRegistry.getRegistry(hostName, Integer.parseInt(port));
                        ITransactionManager remoteTransactionManagerObject = 
                                (ITransactionManager)registry.lookup(serverName + "_TransactionManager");
                        
                        CreateAccountAction createAccountAction = new CreateAccountAction();
                        Transaction txn1 = remoteTransactionManagerObject.createTransaction(createAccountAction);
                        remoteTransactionManagerObject.begin(txn1);
                        //poll to see if txn is complete or not.
                        boolean bTransactionCompleted = false;
                        
                        while(true)
                        {
                                bTransactionCompleted = remoteTransactionManagerObject.isComplete(txn1);
                                
                                if(bTransactionCompleted){
                                        txn1 = remoteTransactionManagerObject.getTransactionState(txn1);                                
                                        remoteTransactionManagerObject.deleteTransaction(txn1);
                                        
                                        break;
                                }
                                
                                try{
                                        Thread.sleep(500);
                                }catch(Exception interruptedException){                 
                                        interruptedException.printStackTrace();
                                        System.out.println("Sleep problem.");
                                }
                        }
                        
                        if(txn1.getCompleteType() == Transaction.COMMIT_COMPLETE){
                                lastActionResult = txn1.getActionResults();
                                String result[] = new String[lastActionResult.size()];
                                result = lastActionResult.toArray(result);
                                createAccountResponse.append("Result of this transaction:\n");
                                for (String i: result)
                                {
                                        createAccountResponse.append(i +"\n");  
                                }
                        }else{
                                createAccountResponse.append("Transaction aborted.\n");
                        }
                } catch (Exception ex) {
                        // TODO Auto-generated catch block
                        ex.printStackTrace();
                }
                busy = false;
                tab.setVisible(true);           
        }//end of else
      }
    });
    createAccountButtonsPanel.add(createAccountButton,BorderLayout.WEST);
    
    
    createAccountButtonsPanel.add(createAccountResponseScroll,BorderLayout.EAST);
    createAccountPanel.add(createAccountButtonsPanel, BorderLayout.CENTER);
    
    tab.addTab("Create Account", createAccountPanel);
    ////////////////////////////////////////////////
    JPanel depositDetailsPanel = new JPanel(new GridLayout(1, 2));
    JPanel depositLabelPanel = new JPanel(new GridLayout(2, 1));
    JPanel depositFieldPanel = new JPanel(new GridLayout(2, 1));

    JTextField d_accountNoField = new JTextField();
    JTextField d_amountField = new JTextField();
    d_accountNoField.setColumns(20);
    d_amountField.setColumns(20);
    
    JLabel d_accountNoLabel = new JLabel("Enter Account No.", JLabel.RIGHT);
    d_accountNoLabel.setLabelFor(d_accountNoField);
    JLabel d_ammountLabel = new JLabel("Enter amount to deposit", JLabel.RIGHT);
    d_ammountLabel.setLabelFor(d_amountField);
    
    depositLabelPanel.add(d_accountNoLabel,BorderLayout.NORTH);
    depositLabelPanel.add(d_ammountLabel,BorderLayout.SOUTH);
    depositFieldPanel.add(d_accountNoField,BorderLayout.NORTH);
    depositFieldPanel.add(d_amountField,BorderLayout.SOUTH);
    depositDetailsPanel.add(depositLabelPanel,BorderLayout.WEST);
    depositDetailsPanel.add(depositFieldPanel,BorderLayout.EAST);
    
    JButton depositButton = new JButton("Deposit Amount");
    depositButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        System.out.println("deposit amt button clicked");
        if(!canConnect)
        {
        	depositResponse.append("goto Connect to tab and enter connection details first\n");
        }
        else if(busy == true){
        	depositResponse.append("try later... system executing another transaction currently\n");
        }
//        else if(){
//        	//for checking if accnt no's branch = connect to branch
//        }
        else{
        	tab.setVisible(false);
        	busy = true;
        	depositResponse.append("trying to deposit:$$"+ d_amountField.getText()+ "into account:" +d_accountNoField.getText() +"\n");
        	
        	Registry registry;
    		try {
    			registry = LocateRegistry.getRegistry(hostName, Integer.parseInt(port));
    			ITransactionManager remoteTransactionManagerObject = 
    				(ITransactionManager)registry.lookup(serverName + "_TransactionManager");
    			
    			String a = d_accountNoField.getText().trim();
    			String b[] = a.split(":");
    			AccountID aid = new AccountID(Integer.parseInt(b[0]),Integer.parseInt(b[1]));
    			DepositAction depositAction = new DepositAction(aid,Double.parseDouble(d_amountField.getText().trim()));
    			Transaction txn1 = remoteTransactionManagerObject.createTransaction(depositAction);
    			remoteTransactionManagerObject.begin(txn1);
    			//poll to see if txn is complete or not.
    			boolean bTransactionCompleted = false;
    			while(true)
                {
                        bTransactionCompleted = remoteTransactionManagerObject.isComplete(txn1);
                        
                        if(bTransactionCompleted){
                                txn1 = remoteTransactionManagerObject.getTransactionState(txn1);                                
                                remoteTransactionManagerObject.deleteTransaction(txn1);
                                
                                break;
                        }
                        
                        try{
                                Thread.sleep(500);
                        }catch(Exception interruptedException){                 
                                interruptedException.printStackTrace();
                                System.out.println("Sleep problem.");
                        }
                }
                
                if(txn1.getCompleteType() == Transaction.COMMIT_COMPLETE){
                        lastActionResult = txn1.getActionResults();
                        String result[] = new String[lastActionResult.size()];
                        result = lastActionResult.toArray(result);
                        depositResponse.append("Result of this transaction:\n");
                        for (String i: result)
                        {
                        	depositResponse.append(i +"\n");  
                        }
                }else{
                	depositResponse.append("Transaction aborted.\n");
                }
    		} catch (Exception ex) {
    			// TODO Auto-generated catch block
    			ex.printStackTrace();
    		}
    		busy = false;
    		tab.setVisible(true);        	
        }//end of else
      }
    });
    depositPanel.add(depositDetailsPanel,BorderLayout.WEST);
    depositPanel.add(depositButton,BorderLayout.SOUTH);
    
    JTextField depositResponse = new JTextField();
    depositResponse.setColumns(20);
    depositPanel.add(depositResponse,BorderLayout.EAST);
    tab.addTab("Deposit $$", depositPanel);
        
    //////////////////////////////////////////////////
    JPanel withdrawDetailsPanel = new JPanel(new GridLayout(1, 2));
    JPanel withdrawLabelPanel = new JPanel(new GridLayout(2, 1));
    JPanel withdrawFieldPanel = new JPanel(new GridLayout(2, 1));

    JTextField w_accountNoField = new JTextField();
    JTextField w_amountField = new JTextField();
    w_accountNoField.setColumns(20);
    w_amountField.setColumns(20);
    
    JLabel w_accountNoLabel = new JLabel("Enter Account No.", JLabel.RIGHT);
    w_accountNoLabel.setLabelFor(w_accountNoField);
    JLabel w_ammountLabel = new JLabel("Enter amount to withdraw", JLabel.RIGHT);
    w_ammountLabel.setLabelFor(w_amountField);
    
    withdrawLabelPanel.add(w_accountNoLabel,BorderLayout.NORTH);
    withdrawLabelPanel.add(w_ammountLabel,BorderLayout.SOUTH);
    withdrawFieldPanel.add(w_accountNoField,BorderLayout.NORTH);
    withdrawFieldPanel.add(w_amountField,BorderLayout.SOUTH);
    withdrawDetailsPanel.add(withdrawLabelPanel,BorderLayout.WEST);
    withdrawDetailsPanel.add(withdrawFieldPanel,BorderLayout.EAST);
    
    JButton withdrawButton = new JButton("Withdraw Amount");
    withdrawButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        System.out.println("withdraw amt button clicked");
        if(!canConnect)
        {
        	withdrawResponse.append("goto Connect to tab and enter connection details first\n");
        }
        else if(busy == true){
        	withdrawResponse.append("try later... system executing another transaction currently\n");
        }
//        else if(){
//        	//for checking if accnt no's branch = connect to branch
//        }
        else{
        	tab.setVisible(false);
        	busy = true;
        	withdrawResponse.append("trying to withdraw:$$"+ w_amountField.getText()+ "from account:" +w_accountNoField.getText() +"\n");
        	
        	Registry registry;
    		try {
    			registry = LocateRegistry.getRegistry(hostName, Integer.parseInt(port));
    			ITransactionManager remoteTransactionManagerObject = 
    				(ITransactionManager)registry.lookup(serverName + "_TransactionManager");
    			
    			String a = w_accountNoField.getText().trim();
    			String b[] = a.split(":");
    			AccountID aid = new AccountID(Integer.parseInt(b[0]),Integer.parseInt(b[1]));
    			WithdrawAction withdrawAction = new WithdrawAction(aid,Double.parseDouble(w_amountField.getText().trim()));
    			Transaction txn1 = remoteTransactionManagerObject.createTransaction(withdrawAction);
    			remoteTransactionManagerObject.begin(txn1);
    			//poll to see if txn is complete or not.
    			boolean bTransactionCompleted = false;
    			while(true)
                {
                        bTransactionCompleted = remoteTransactionManagerObject.isComplete(txn1);
                        
                        if(bTransactionCompleted){
                                txn1 = remoteTransactionManagerObject.getTransactionState(txn1);                                
                                remoteTransactionManagerObject.deleteTransaction(txn1);
                                
                                break;
                        }
                        
                        try{
                                Thread.sleep(500);
                        }catch(Exception interruptedException){                 
                                interruptedException.printStackTrace();
                                System.out.println("Sleep problem.");
                        }
                }
                
                if(txn1.getCompleteType() == Transaction.COMMIT_COMPLETE){
                        lastActionResult = txn1.getActionResults();
                        String result[] = new String[lastActionResult.size()];
                        result = lastActionResult.toArray(result);
                        withdrawResponse.append("Result of this transaction:\n");
                        for (String i: result)
                        {
                        	withdrawResponse.append(i +"\n");  
                        }
                }else{
                	withdrawResponse.append("Transaction aborted.\n");
                }
    		} catch (Exception ex) {
    			// TODO Auto-generated catch block
    			ex.printStackTrace();
    		}
    		busy = false;
    		tab.setVisible(true);        	
        }//end of else
        
      }
    });
    withdrawPanel.add(withdrawDetailsPanel,BorderLayout.WEST);
    withdrawPanel.add(withdrawButton,BorderLayout.SOUTH);
    
    JTextField withdrawResponse = new JTextField();
    withdrawResponse.setColumns(20);
    withdrawPanel.add(withdrawResponse,BorderLayout.EAST);

    tab.addTab("Withdraw $$", withdrawPanel);
/////////////////////////////check balance///////////////////////////
    JPanel balanceDetailsPanel = new JPanel(new GridLayout(1, 2));
    
    final JTextField bal_accountNoField = new JTextField();
    bal_accountNoField.setColumns(20);
    
    JLabel bal_accountNoLabel = new JLabel("Enter Account No.", JLabel.RIGHT);
    bal_accountNoLabel.setLabelFor(bal_accountNoField);
    
    balanceDetailsPanel.add(bal_accountNoLabel,BorderLayout.WEST);
    balanceDetailsPanel.add(bal_accountNoField,BorderLayout.EAST);
   
    final JTextArea balanceResponse = new JTextArea(null, 5, 20);
    balanceResponse.setLineWrap(true);
    JScrollPane balanceResponseScroll = new JScrollPane(balanceResponse);
    balanceResponseScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);

    JButton balanceButton = new JButton("Check Balance");
    balanceButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        System.out.println("balance button clicked");
        if(!canConnect)
        {
        	balanceResponse.append("goto Connect to tab and enter connection details first\n");
        }
        else if(busy == true){
        	balanceResponse.append("try later... system executing another transaction currently\n");
        }
//        else if(){
//        	//for checking if accnt no's branch = connect to branch
//        }
        else{
        	tab.setVisible(false);
        	busy = true;
        	balanceResponse.append("trying to query balance in account:" +bal_accountNoField.getText() +"\n");
        	
        	Registry registry;
    		try {
    			registry = LocateRegistry.getRegistry(hostName, Integer.parseInt(port));
    			ITransactionManager remoteTransactionManagerObject = 
    				(ITransactionManager)registry.lookup(serverName + "_TransactionManager");
    			
    			String a = bal_accountNoField.getText().trim();
    			String b[] = a.split(":");
    			AccountID aid = new AccountID(Integer.parseInt(b[0]),Integer.parseInt(b[1]));
    			GetBalanceAction balanceAction = new GetBalanceAction(aid);
    			Transaction txn1 = remoteTransactionManagerObject.createTransaction(balanceAction);
    			remoteTransactionManagerObject.begin(txn1);
    			//poll to see if txn is complete or not.
    			boolean bTransactionCompleted = false;
    			while(true)
                {
                        bTransactionCompleted = remoteTransactionManagerObject.isComplete(txn1);
                        
                        if(bTransactionCompleted){
                                txn1 = remoteTransactionManagerObject.getTransactionState(txn1);                                
                                remoteTransactionManagerObject.deleteTransaction(txn1);
                                
                                break;
                        }
                        
                        try{
                                Thread.sleep(500);
                        }catch(Exception interruptedException){                 
                                interruptedException.printStackTrace();
                                System.out.println("Sleep problem.");
                        }
                }
                
                if(txn1.getCompleteType() == Transaction.COMMIT_COMPLETE){
                        lastActionResult = txn1.getActionResults();
                        String result[] = new String[lastActionResult.size()];
                        result = lastActionResult.toArray(result);
                        balanceResponse.append("Result of this transaction:\n");
                        for (String i: result)
                        {
                        	balanceResponse.append(i +"\n");  
                        }
                }else{
                	balanceResponse.append("Transaction aborted.\n");
                }
    		} catch (Exception ex) {
    			// TODO Auto-generated catch block
    			ex.printStackTrace();
    		}
    		busy = false;
    		tab.setVisible(true);        	
        }//end of else
       
      }
    });
    balancePanel.add(balanceDetailsPanel,BorderLayout.WEST);
    balancePanel.add(balanceButton,BorderLayout.WEST);
    
    balancePanel.add(balanceResponseScroll,BorderLayout.WEST);
    tab.addTab("Check Balance", balancePanel);
    //////////////////////////transfer///////////////////////////
    JPanel transferDetailsPanel = new JPanel(new GridLayout(1, 2));
    JPanel transferLabelPanel = new JPanel(new GridLayout(3, 1));
    JPanel transferFieldPanel = new JPanel(new GridLayout(3, 1));

    JTextField t_fromaccountNoField = new JTextField();
    JTextField t_toaccountNoField = new JTextField();
    JTextField t_amountField = new JTextField();
    t_fromaccountNoField.setColumns(20);
    t_fromaccountNoField.setColumns(20);
    t_amountField.setColumns(20);
    
    JLabel t_fromaccountNoLabel = new JLabel("From Account No.", JLabel.RIGHT);
    t_fromaccountNoLabel.setLabelFor(t_fromaccountNoField);
    JLabel t_toaccountNoLabel = new JLabel("To Account No.", JLabel.RIGHT);
    t_toaccountNoLabel.setLabelFor(t_toaccountNoField);
    JLabel t_ammountLabel = new JLabel("Enter amount to transfer", JLabel.RIGHT);
    t_ammountLabel.setLabelFor(t_amountField);
    
    transferLabelPanel.add(t_fromaccountNoLabel,BorderLayout.NORTH);
    transferLabelPanel.add(t_toaccountNoLabel,BorderLayout.NORTH);
    transferLabelPanel.add(t_ammountLabel,BorderLayout.NORTH);
    transferFieldPanel.add(t_fromaccountNoField,BorderLayout.NORTH);
    transferFieldPanel.add(t_toaccountNoField,BorderLayout.NORTH);
    transferFieldPanel.add(t_amountField,BorderLayout.NORTH);
    transferDetailsPanel.add(transferLabelPanel,BorderLayout.WEST);
    transferDetailsPanel.add(transferFieldPanel,BorderLayout.EAST);
    
    JButton transferButton = new JButton("transfer Amount");
    transferButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        System.out.println("transfer amt button clicked");
      }
    });
    transferPanel.add(transferDetailsPanel,BorderLayout.WEST);
    transferPanel.add(transferButton,BorderLayout.WEST);
    
    JTextField transferResponse = new JTextField();
    transferResponse.setColumns(20);
    transferPanel.add(transferResponse,BorderLayout.WEST);
    tab.addTab("Transfer $$", transferPanel);
/////////////////////////////////////////////////////////////////////
    
    add(tab);
  }

  public String getText(int i) {
    return (fields[i].getText());
  }

  public static void main(String[] args) {
	String[] labels = { "Server Name", "Host Name", "Port Number"};
    int[] widths = { 20, 20, 10 };
    
    final ClientGUI form = new ClientGUI(labels, widths);

    JFrame connectFrame = new JFrame("Client GUI");
    connectFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    connectFrame.getContentPane().add(form, BorderLayout.NORTH);
//    JPanel p = new JPanel();
//    connectFrame.getContentPane().add(p, BorderLayout.SOUTH);
    connectFrame.pack();
    connectFrame.setVisible(true);
  }
}


           
         