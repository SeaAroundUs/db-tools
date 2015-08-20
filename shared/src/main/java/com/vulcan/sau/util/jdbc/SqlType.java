package com.vulcan.sau.util.jdbc;

import java.math.BigInteger;
import java.security.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

public enum SqlType {
    BIGINT( BigInteger.class, Types.BIGINT, "numeric" ),
    DATE( Date.class, Types.DATE, "date", "''yyyy-MM-dd''" ),
    DOUBLE( Double.class, Types.DOUBLE, "float8" ),
    FLOAT( Float.class, Types.REAL, "float4" ),
    INTEGER( Integer.class, Types.INTEGER, "int4" ),
    LONG( Long.class, Types.BIGINT, "int8" ),
    SHORT( Short.class, Types.SMALLINT, "int2" ),
    TEXT( String.class, Types.VARCHAR, "text" ),
    CALENDAR( Calendar.class, Types.TIMESTAMP, "timestamp", "''yyyy-MM-dd hh:mm:ss zzz''" ),
    TIMESTAMP( Timestamp.class, Types.TIMESTAMP, "timestamp", "''yyyy-MM-dd hh:mm:ss zzz''" ),
    VARCHAR( String.class, Types.VARCHAR, "varchar" );
 
    //@SuppressWarnings("unchecked")
	protected Class typeClass = null;
    protected String typeName = null;
    protected Integer type    = null;
    protected String format   = null;
        
    //@SuppressWarnings("unchecked")
	SqlType( Class typeClass, Integer type, String typeName ) {
        this.typeClass = typeClass;
        this.typeName = typeName;
        this.type = type;
    }
        
    //@SuppressWarnings("unchecked")
	SqlType( Class typeClass, Integer type, String typeName, String format ) {
        this.typeClass = typeClass;
        this.typeName = typeName;
        this.type = type;
        this.format = format;
    }
        
    //@SuppressWarnings("unchecked")
	public Class getTypeClass() {
        return typeClass;
    }
    
    public Integer getType() {
        return type;
    }
    
    public String getTypeName() {
        return typeName;
    }
    
    public String getFormat() {
        return format;
    }
    
    @SuppressWarnings("unchecked")
    public static SqlType valueOf( Class checkClass ) {
        SqlType result = null;
        
        if ( ! String.class.equals( checkClass ) ) {
            for ( SqlType sqlType : values() ) {
                if ( sqlType.typeClass.isAssignableFrom( checkClass ) ) {
                    result = sqlType;
                }
            }
        }
        else {
            result = VARCHAR;
        }
            
        return result;
    }
        
    public static SqlType valueOf( Integer typeInt ) {
        SqlType result = null;
            
        for ( SqlType sqlType : values() ) {
            if ( sqlType.type.equals( typeInt ) ) {
                result = sqlType;
            }
        }
            
        return result;
    }
}
