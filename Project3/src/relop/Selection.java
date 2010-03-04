package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
	private Iterator _iter = null;
	private Predicate[] _arrPredicates = null;
	private boolean _bOpen = false;
	private Tuple _savedTuple = null;
	
	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		_iter = iter;
		_arrPredicates = preds;
		
		// Reset the internal iterator's position.
		// Inherit internal iterator's schema.
		this.schema = _iter.schema;
		_iter.restart();
		_bOpen = true;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// Reset the internal iterator's position.
		_iter.restart();
		_bOpen = true;
		_savedTuple = null;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return _bOpen;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		_iter.close();
		_bOpen = false;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if(_savedTuple != null){
			// We have a saved tuple, so this iterator is not empty.
			return true;
		}else{
			// Look for a tuple satisfying predicates.
			if(_iter.hasNext() == false){
				return false;
			}else{
				boolean bFoundTuple = false;
				
				while(_iter.hasNext()){
					Tuple tuple = _iter.getNext();
					boolean bPredicatesSatisfied = false;
					
					// For each predicate, make checks.
					for(Predicate predicate : _arrPredicates){
						if(predicate.evaluate(tuple) == true){
							bPredicatesSatisfied = true;
							
							break;
						}
					}
					
					if(bPredicatesSatisfied){
						// All predicates are satisfied by tuple.
						_savedTuple = tuple;
						bFoundTuple = true;
						
						break;
					}
				}
				
				// At this point, if a qualifying tuple exists in the internal
				// iterator, it is stored in _savedTuple.
				return bFoundTuple;
			}
		}
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() throws IllegalStateException{
		if(_savedTuple != null){
			Tuple tuple = _savedTuple;
			// Unset _savedTuple since it's been consumed
			_savedTuple = null;
			
			return tuple;
		}else{
			// Look for a qualifying tuple.
			while(_iter.hasNext()){
				Tuple tuple = _iter.getNext();
				boolean bPredicatesSatisfied = false;
				
				// For each predicate, make checks.
				for(Predicate predicate : _arrPredicates){
					if(predicate.evaluate(tuple) == true){
						bPredicatesSatisfied = true;
						
						break;
					}
				}
				
				if(bPredicatesSatisfied){
					// All predicates are satisfied by tuple.
					// Return it.
					return tuple;
				}
			}
			
			// If control has reached this point, no qualifying tuple was found.
			throw new IllegalStateException("No qualifying tuple found.");
		}
	}

} // public class Selection extends Iterator
