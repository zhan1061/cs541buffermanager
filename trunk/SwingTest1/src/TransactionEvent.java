
public class TransactionEvent {
	private Transaction _transaction;
	private String _message;
	
	public TransactionEvent(Transaction transaction, String message){
		_transaction = transaction;
		_message = message;
	}
	
	public Transaction getTransaction(){
		return _transaction;
	}
	
	public String getMessage(){
		return _message;
	}
}
