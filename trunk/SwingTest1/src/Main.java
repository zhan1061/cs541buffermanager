import java.awt.Frame;
import javax.swing.JFrame;


public class Main {
	public static void main(String[] args){
//		JFrame buttonTest = new ButtonTest("Button Test", 400, 200);
//		JFrame serverUI = new ServerFrame("Server");
//		
//		serverUI.setVisible(true);
		if(args.length != 1){
			System.out.println("Usage: java Main <service_name>");
		}else{		
//			int registryPort = Integer.parseInt(args[1]); 
			MasterController masterController = new MasterController(args[0]);
		}
		
//		TransactionFrame transactionFrame = new TransactionFrame(new Transaction(1, new GetBalanceAction(new AccountID(1,1))), null);
//		
//		transactionFrame.setVisible(true);
	}
}
