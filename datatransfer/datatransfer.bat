SET MAVEN_OPTS=-Xmx2048M -Djava.library.path=D:/jdbc/sqljdbc_4.0/auth/x64
 
REM mvn exec:java -Dactive.routes=datatransfer -Djava.library.path=D:/jdbc/sqljdbc_4.0/auth/x64
mvn exec:java -Dactive.routes=datatransfer 