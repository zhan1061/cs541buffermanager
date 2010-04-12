import java.util.ArrayList;


class Pair<K,V>{
	private K _keyObject;
	private V _valueObject;
	
	public Pair(K keyObject, V valueObject){
		_keyObject = keyObject;
		_valueObject = valueObject;
	}
	
	public K getKey(){
		return _keyObject;
	}
	
	public V getValue(){
		return _valueObject;
	}
	
	public void setValue(V value){
		_valueObject = value;
	}
	
	public void setKey(K key){
		_keyObject = key;
	}
}

/**
 * A very simple table at performs key comparisons using the equals method.
 * The downside is that this makes it much slower than a conventional Hashtable.
 * @author anurag
 *
 * @param <K>
 * @param <V>
 */
public class ComparisonTable <K,V> {
	private ArrayList<Pair<K,V>> _lstPair = new ArrayList<Pair<K,V>>();
	
	public void put(K key, V value){
		// Iterate through the list to find the right key. If it isn't found,
		// add it.
		boolean bFoundKey = false;
		
		for(Pair<K,V> pair : _lstPair){
			if(pair.getKey().equals(key)){
				pair.setValue(value);
				bFoundKey = true;
				
				break;
			}
		}
		
		if(bFoundKey == false){
			_lstPair.add(new Pair(key, value));
		}
	}
	
	public V get(K key){
		for(Pair<K,V> pair : _lstPair){
			if(pair.getKey().equals(key)){
				return pair.getValue();
			}
		}
		
		// Key not found.
		return null;
	}
	
	public boolean containsKey(K key){
		for(Pair<K,V> pair : _lstPair){
			if(pair.getKey().equals(key)){
				return true;
			}
		}
		
		// Key not found.
		return false;	
	}
	
	public ArrayList<K> keys(){
		ArrayList<K> lstKey = new ArrayList<K>();
		
		for(Pair<K,V> pair : _lstPair){
			lstKey.add(pair.getKey());
		}
		
		return lstKey;
	}
	
	public V remove(K key){
		for(Pair<K,V> pair : _lstPair){
			if(pair.getKey().equals(key)){
				V vToReturn = pair.getValue();
				_lstPair.remove(pair);
				
				return vToReturn;
			}
		}
		
		// Key not found.
		return null;
	}
}
