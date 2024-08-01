# apache-spark-processor-city

# Execute the command below
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED --add-exports=java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED" -Djavax.net.ssl.trustStore=/home/programmer/desenvolvimento/testes/apache-spark-processor-city/ -Djavax.net.ssl.keyStore=/home/programmer/desenvolvimento/testes/apache-spark-processor-city

keytool -genkey -keyalg RSA -v -keystore elastic.jks -alias elastic-key-pair

keytool -genkey -keyalg RSA -v -keystore elastic.jks -alias elastic-key-pair
keytool -genkey -keyalg RSA -v -keystore elastic.jks -alias elastic-second-key-pair

keytool -importkeystore -srckeystore elastic.jks -destkeystore keystore.p12 -srcstoretype jks -deststoretype pkcs12

openssl pkcs12 -in keystore.p12 -out elastic.pem

keytool -exportcert -alias elastic-key-pair -keystore elastic.jks -rfc -file elastic-key-pair-cert.pem
https://www.baeldung.com/java-keystore-convert-to-pem-format

keytool -list -v -keystore elastic.jks
 