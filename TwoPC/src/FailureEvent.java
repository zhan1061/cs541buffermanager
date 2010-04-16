
public class FailureEvent {
	public final static int RECOVERY_EVENT = 1;
	public final static int FAILURE_EVENT = 2;
	
	private int _failureEventType;
	
	public FailureEvent(int failureEventType){
		_failureEventType = failureEventType;
	}
	
	public int getFailureEventType(){
		return _failureEventType;
	}
}
