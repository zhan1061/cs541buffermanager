import java.util.Hashtable;


public class GlobalState {
	private static Hashtable<String, Object> _htGlobalTable = new Hashtable<String, Object>();
	
	public static Hashtable<String, Object> getGlobalTable(){
		return _htGlobalTable;
	}
	
	public static Object get(String key){
		if(_htGlobalTable.containsKey(key)){
			return _htGlobalTable.get(key);
		}else{
			return null;
		}
	}
	
	public static void set(String key, Object value){
		_htGlobalTable.put(key, value);
	}
}
