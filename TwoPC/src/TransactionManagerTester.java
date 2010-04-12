import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class TransactionManagerTester {
	public static void main(String[] args){
		// arg1: peer service name to connect to.
		if(args.length != 1){
			System.out.println("Usage: java TransactionManagerTester <servicename>");
		}
		
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry("localhost", 1099);
			ITransactionManager remoteTransactionManagerObject = 
				(ITransactionManager)registry.lookup(args[0] + "_TransactionManager");
			
			// Let us try create an account.
			CreateAccountAction createAccountAction = new CreateAccountAction();
			Transaction txn1 = remoteTransactionManagerObject.createTransaction(createAccountAction);
			remoteTransactionManagerObject.begin(txn1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
