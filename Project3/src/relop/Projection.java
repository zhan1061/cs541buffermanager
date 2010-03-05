package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	
	Integer[] fields;
	Iterator iter;
	int[] mapping;

  /**
   * Constructs a projection, given the underlying iator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
	  this.iter = iter;
	  this.fields = fields;

	  schema = new Schema(fields.length);
	  mapping= new int[fields.length];

	  for(int i=0;i< fields.length; i++ ){
		  String fieldName = iter.schema.fieldName(fields[i]);
		  int fieldLength = iter.schema.fieldLength(fields[i]);
		  int fieldType = iter.schema.fieldType(fields[i]);

		  schema.initField(i, fieldType,fieldLength, fieldName);
		  mapping[i] = fields[i];
	  }
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Projection Op...");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	   return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  Tuple projTuple = new Tuple(schema);
	  int numOfFields = schema.getCount();
	  Tuple origTuple = iter.getNext();

	  for(int i=0;i<numOfFields;i++){
		  projTuple.setField(i, origTuple.getField(mapping[i]));
	  }

      return projTuple;
  }

} // public class Projection extends Iterator
