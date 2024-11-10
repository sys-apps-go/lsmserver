go run lsmserver &

go run client.go \
    -server_addr=localhost:50051 \
    -num_users=500 \
    -ops_per_user=1000 \
    -key_range=100 \
    -value_range=1000 \
    -sleep_millis=500
