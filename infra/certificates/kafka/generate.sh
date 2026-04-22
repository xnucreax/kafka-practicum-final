openssl req -new \
    -newkey rsa:2048 \
    -keyout certs/kafka.key \
    -out certs/kafka.csr \
    -config kafka-cert.cnf \
    -nodes

openssl x509 -req \
    -days 3650 \
    -in certs/kafka.csr \
    -CA ../ca/certs/ca.crt \
    -CAkey ../ca/certs/ca.key \
    -CAcreateserial \
    -out certs/kafka.crt \
    -extfile kafka-cert.cnf \
    -extensions v3_req

openssl pkcs12 -export \
    -in certs/kafka.crt \
    -inkey certs/kafka.key \
    -chain \
    -CAfile ../ca/certs/ca.pem \
    -name kafka \
    -out certs/kafka.p12 \
    -password pass:sslkey_password

keytool -importkeystore \
    -deststorepass keystore_password \
    -destkeystore stores/kafka.keystore.jks \
    -srckeystore certs/kafka.p12 \
    -deststoretype JKS \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass sslkey_password

keytool -import \
    -file ../ca/certs/ca.crt \
    -alias ca \
    -keystore stores/kafka.truststore.jks \
    -storepass truststore_password \
    -noprompt
