# Toy-KV

api:

get
put
delete
scan -> Iterator


.db file
version (4 bytes), keys_len(4 bytes), keys_data(VAR), value_len(4 bytes), value_data(VAR)

.pos file

/engine/store
/engine/db
/engine/

/transport/