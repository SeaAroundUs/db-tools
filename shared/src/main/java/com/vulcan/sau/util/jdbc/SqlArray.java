package com.vulcan.sau.util.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Map;

public class SqlArray<T> implements Array {
    private T[] array = null;
    
    private SqlType sqlType = null;
    
    public SqlArray( T[] array ) {
        this.array = array;

        if ( array != null ) {
            sqlType = SqlType.valueOf(array.getClass().getComponentType());
        }
    }
    
    public SqlArray(long[] array) {
        
    }

    public SqlArray( T[] array, int type ) {
        this(array);
        
        sqlType = SqlType.valueOf(type);
    }
    
    
    public T[] getArray() {
        return array;
    }
    
    public Object getArray( long index, int count ) {
        throw new UnsupportedOperationException("This method is not supported.");
    }
    
    public Object getArray(long index, int count, Map<String,Class<?>> map) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    public Object getArray(Map<String,Class<?>> map) {
        throw new UnsupportedOperationException("This method is not supported.");
    }
    
    public int getBaseType() {
        return sqlType.getType();
    }

    public String getBaseTypeName() {
        return sqlType.getTypeName();
    }

    public ResultSet getResultSet() {
        throw new UnsupportedOperationException("This method is not supported.");
    }
    
    public ResultSet getResultSet( long index, int count ) {
        throw new UnsupportedOperationException("This method is not supported.");
    }
    
    public ResultSet getResultSet(long index, int count, Map<String,Class<?>> map) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    public ResultSet getResultSet(Map<String,Class<?>> map) {
        throw new UnsupportedOperationException("This method is not supported.");
    }
    
    public String toString()
    {
        if ( array != null ) {
            SimpleDateFormat format = null;
            
            if ( sqlType.getFormat() != null ) {
                format = new SimpleDateFormat( sqlType.getFormat() );
            }
            
            //  Removed until MySQL supports arrays.
            //  StringBuffer strBuf = new StringBuffer("{");
            StringBuffer strBuf = new StringBuffer();

            for ( int i = 0; i < array.length; i++ ) {
                if ( format != null ) { 
                  strBuf.append(i == 0 ? "" : ",").append(format.format(array[i]));
                }
                else {
                  strBuf.append(i == 0 ? "" : ",").append(array[i]);
                }
            }
        
            //  Removed until MySQL supports arrays.
            //  strBuf.append("}");

            return strBuf.toString( );
        }
        else {
            return null;
        }
    }
    
    public void free() {
    }
}
