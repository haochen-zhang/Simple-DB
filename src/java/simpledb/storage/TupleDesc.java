package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private final List<TDItem> tdItems = new ArrayList<>();
    private final Type[] types;
    private final String[] fields;
    private final int numFields;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public Type getFieldType() {
            return this.fieldType;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // TODO: some code goes here
        return this.tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // TODO: some code goes here
        this.numFields = typeAr.length;
        this.types = typeAr.clone();
        this.fields = fieldAr.clone();
        for (int i = 0; i < this.numFields; i++) {
            this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // TODO: some code goes here
        this.numFields = typeAr.length;
        this.types = typeAr.clone();
        String[] emptyArray = new String[this.numFields];
        Arrays.fill(emptyArray, null);
        this.fields = emptyArray;
        for (int i = 0; i < this.numFields; i++) {
            this.tdItems.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // TODO: some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // TODO: some code goes here
        return this.tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // TODO: some code goes here
        return this.tdItems.get(i).fieldType;
    }

    public Type[] getTypes() {
        return this.types;
    }

    public String[] getFieldsNames() {
        return this.fields;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        // TODO: some code goes here
        if (name == null) {
            throw new NoSuchElementException("null is not a valid field name");
        }
        for (int i = 0; i < this.numFields; i++) {
            if (name.equals(this.tdItems.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException(String.format("%s is not a valid field name", name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // TODO: some code goes here
//        return this.tdItems.stream().map(TDItem::getFieldType).mapToInt(Type::getLen).sum();
        int sum = 0;
        for(int i = 0; i < tdItems.size(); ++i) {
            sum += tdItems.get(i).fieldType.getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // TODO: some code goes here
        Type[] newTypes = Stream.concat(Arrays.stream(td1.types), Arrays.stream(td2.types)).toArray(Type[]::new);
        String[] newFields = Stream.concat(Arrays.stream(td1.fields), Arrays.stream(td2.fields)).toArray(String[]::new);
        return new TupleDesc(newTypes, newFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // TODO: some code goes here
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        return Arrays.equals(((TupleDesc) o).fields, this.fields)
                && Arrays.equals(((TupleDesc) o).types, this.types)
                && ((TupleDesc) o).numFields == this.numFields;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // TODO: some code goes here
        return this.tdItems.stream().map(TDItem::toString).collect(Collectors.joining(", "));
    }
}
