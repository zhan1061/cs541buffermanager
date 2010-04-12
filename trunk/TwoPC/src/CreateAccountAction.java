import java.io.Serializable;
import java.util.ArrayList;


public class CreateAccountAction implements IAction, Serializable{
	private Transaction _parentTransaction;
	private ArrayList<IOperation> _lstOperation = null;
	
	public CreateAccountAction(){
		
	}
	
	@Override
	public ArrayList<IOperation> getOperations() {
		if(_lstOperation == null){
			_lstOperation = new ArrayList<IOperation>();

			// This is just one read operation.
			CreateAccountOperation createAccountOperation = new CreateAccountOperation(_parentTransaction); 
			_lstOperation.add(createAccountOperation);
		}
		
		return _lstOperation; 
	}

	@Override
	public void setParentTransaction(Transaction parentTransaction) {
		_parentTransaction = parentTransaction;		
	}
}
