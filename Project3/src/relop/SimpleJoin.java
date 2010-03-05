package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
	private Iterator _leftIter = null;
	private Iterator _rightIter = null;
	private Predicate[] _arrPredicates = null;
	private boolean _bOpen = false;
	private Tuple _savedTuple = null;
	private Tuple _currentLeftTuple = null;
	private Tuple _currentRightTuple = null;
	
	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		_leftIter = left;
		_rightIter = right;
		_arrPredicates = preds;
		// Reset the internal iterators' position.
		// The new schema should be a union of the schemas of the
		// joining relations.
		this.schema = Schema.join(_leftIter.schema, _rightIter.schema);
		
		_leftIter.restart();
		_rightIter.restart();
		_bOpen = true;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);			
		System.out.println("Join");
		
		_leftIter.explain(depth + 1);
		_rightIter.explain(depth + 1);		
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// Reset the internal iterator's position.
		_leftIter.restart();
		_rightIter.restart();
		_bOpen = true;
		_savedTuple = null;
		_currentLeftTuple = null;
		_currentRightTuple = null;
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
		_savedTuple = null;
		_leftIter.close();
		_rightIter.close();
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
			if(_leftIter.hasNext() == false){
				return false;
			}else{
				// At this point in the control flow, we are assured
				// that there is at least one tuple in each of the
				// left and the right iterators. 
				if(_currentLeftTuple == null || _rightIter.hasNext() == false){
					if(_leftIter.hasNext() == false){
						// Reached the end of the left relation.
						return false;
					}else{
						// Grab the next tuple of the left relation.
						_currentLeftTuple = _leftIter.getNext();
					}
				}
					
				while(true){
					if(_rightIter.hasNext() == false){
						_rightIter.restart();
					}
					
					while(_rightIter.hasNext()){
						// Join the two tuples and test for the join condition.
						// The predicate's field arguments are relative to the
						// combined tuple.
						_currentRightTuple = _rightIter.getNext();
						Tuple combinedTuple = Tuple.join(_currentLeftTuple, _currentRightTuple, schema);
						boolean bAllPredicatesSatisfied = true;

						// Check all predicates for satisfaction.
						for(Predicate predicate : _arrPredicates){
							if(predicate.evaluate(combinedTuple) == false){
								bAllPredicatesSatisfied = false;

								break;
							}
						}

						if(bAllPredicatesSatisfied == true){
							_savedTuple = combinedTuple;

							return true;
						}
					}
					
					if(_leftIter.hasNext() == false){
						break;
					}else{
						// Grab the next tuple of the left relation.
						_currentLeftTuple = _leftIter.getNext();
					}
				}
				
				return false;
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
			// Look for a tuple satisfying predicates.
			if(_leftIter.hasNext() == false){
				throw new IllegalStateException("No qualifying tuple found.");
			}else{
				// At this point in the control flow, we are assured
				// that there is at least one tuple in each of the
				// left and the right iterators. 
				if(_currentLeftTuple == null || _rightIter.hasNext() == false){
					if(_leftIter.hasNext() == false){
						// Reached the end of the left relation.
						throw new IllegalStateException("No qualifying tuple found.");
					}else{
						// Grab the next tuple of the left relation.
						_currentLeftTuple = _leftIter.getNext();
					}
				}
					
				while(true){
					if(_rightIter.hasNext() == false){
						_rightIter.restart();
					}
					
					while(_rightIter.hasNext()){
						// Join the two tuples and test for the join condition.
						// The predicate's field arguments are relative to the
						// combined tuple.
						_currentRightTuple = _rightIter.getNext();
						Tuple combinedTuple = Tuple.join(_currentLeftTuple, _currentRightTuple, schema);
						boolean bAllPredicatesSatisfied = true;

						// Check all predicates for satisfaction.
						for(Predicate predicate : _arrPredicates){
							if(predicate.evaluate(combinedTuple) == false){
								bAllPredicatesSatisfied = false;

								break;
							}
						}

						if(bAllPredicatesSatisfied == true){
							return combinedTuple;
						}
					}
					
					if(_leftIter.hasNext() == false){
						break;
					}else{
						// Grab the next tuple of the left relation.
						_currentLeftTuple = _leftIter.getNext();
					}
				}
				
				// If control has reached this point, no qualifying tuple was found.
				throw new IllegalStateException("No qualifying tuple found.");
			}
		}
	}

} // public class SimpleJoin extends Iterator
