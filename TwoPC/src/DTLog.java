import java.util.ArrayList;


/**
 * Encapsulates the distributed transaction log.
 * @author anurag
 *
 */
public class DTLog {
	private static ArrayList<LogRecord> _lstLogRecord;
	
	static{
		_lstLogRecord = new ArrayList<LogRecord>();
	}
	
	public static ArrayList<LogRecord> getLog(){
		return _lstLogRecord;
	}
}
