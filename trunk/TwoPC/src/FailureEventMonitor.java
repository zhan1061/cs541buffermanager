import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class FailureEventMonitor {
	private final static int TIMER_INTERVAL = 500;
	
	private static FailureEventMonitor _failureEventMonitor = null;
	private ArrayList<IFailureEventListener> _lstFailureEventListener = null;
	private int _currentFailureState;
	
	protected FailureEventMonitor(){
		_lstFailureEventListener = new ArrayList<IFailureEventListener>();
	}
	
	public static FailureEventMonitor getFailureEventMonitor(){
		if(_failureEventMonitor == null){
			_failureEventMonitor = new FailureEventMonitor();			
		}
		
		return _failureEventMonitor;
	}
	
	/**
	 * Register failureEventListener as a listener for FailureEvents.
	 * @param failureEventListener
	 */
	public void registerFailureEventListener(IFailureEventListener failureEventListener){
		if(_lstFailureEventListener.contains(failureEventListener) == false){
			_lstFailureEventListener.add(failureEventListener);
		}
	}
	
	/**
	 * Informs all registered IFailureEventListeners that 
	 * a failure (FAILURE/RECOVERY) event has occurred. 
	 * @param failureEvent
	 */
	public void triggerFailureEvent(FailureEvent failureEvent){
		// Set the current failure state.
		_currentFailureState = failureEvent.getFailureEventType();
		
		// Inform all FailureEventListeners of the event.
		for(IFailureEventListener failureEventListener : _lstFailureEventListener){
			failureEventListener.failureEventOccurred(failureEvent);
		}
	}
	
	public int getCurrentFailureState(){
		return _currentFailureState;		
	}
}
