# KVDB
Custom Key Value Database in Rust
Loosely based on BitCask

Intended to minimize use of external packages


CRC is to be added
Add more useful io binding


The in-memory key is made of:
key - string
file_id - unsigned 8-bit integer
timestamp - time since epoch in sec as u32
vsz - length of the value as u32
voffset - value start location in the datafile u32

The datafile is built of:
timestamp - time since epoch in sec as u32
ksz - length of the key as u32
vsz - length of the value as u32
key - string
value - string

