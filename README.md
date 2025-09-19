# KVDB
Custom Key Value Database in Rust
Loosely based on BitCask

Intended to minimize use of external packages

## To Do:
- [X] Add rebuild memstore function for complete rebuild
- [ ] Add hint file 
- [ ] Review Lengths of each segment
- [ ] Add deletion function
- [ ] Add update function
- [ ] Add CRC
- [ ] Build Application Interface
- [ ] Build IPC interface
- [ ] Remove external packages (serde, config)


The in-memory key is made of:

| name | datatype | description |
| --- | --- | --- |
| key | string | key as string |
| file_id | u8 | int that specifies the datafile |
| timestamp | u32 | seconds from unix epoch |
| vsz | u32 | length of the paired value |
| voffset | u32 | index for the start of the value data in the file |


The datafile is built of:

| name | datatype | description |
| --- | --- | --- |
| timestamp | u32 | seconds from unix epoch |
| ksz | u32 | length of the key |
| vsz | u32 | length of the paired value |
| key | str | key string |
| value | str | value string |




