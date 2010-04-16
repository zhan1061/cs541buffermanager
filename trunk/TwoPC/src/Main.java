import java.awt.Frame;
import java.util.ArrayList;

import javax.swing.JFrame;

public class Main {
	public static void main(String[] args){
		if(args.length != 1){
			System.out.println("Usage: java Main <service_name>");
		}else{		
			MasterController masterController = new MasterController(args[0]);
		}
		
//		JFrame coordinatorController = new CoordinatorController(new ArrayList<Integer>(), null);
//		coordinatorController.setVisible(true);
		
//		JFrame participantController = new ParticipantController(new TransactionID(3), null);
//		participantController.setVisible(true);
	}
}
