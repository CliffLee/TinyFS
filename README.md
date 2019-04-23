CSCI485 - Project Part 3
========================

## Collaborators

* Clifford Lee
* Sneha Patkar
* Stephanie Hernandez

## Design

### Requirements

[x] Single master design capable of managing multiple chunkservers
[x] Master never communicates directly with chunkservers but instead infers state by interactions with clients
[ ] Master maintains a log of transactions for crash recovery; checkpointing
[ ] Many clients can interact with one master
[x] Resolution with master eventually leads to one client interacting with one chunkserver
[x] Record based file management on chunks
[ ] Master PoF design (shadow masters)
[ ] Atomic append (lock escalation strategies)

### Data Structure Decisions

1. ChunkServerMaster namespace is a `TreeMap<String, List<String>>`

Chose this data structure to leverage TreeMaps underlying tree data structure for prefix querying. Original design approaches used a regular hashmap but after some googling, we found one answer on [stackoverflow][1] that demonstrated how tree based maps actually turn our original O(n) prefix query into an O(log(n)) query.

Used a list of String types which represent our chunkhandles. This works locally in finding our chunkserver but we realize that should the chunks be running on different machines, we would need to encode more data in the handle (namely ip/mac address).

2. ChunkServer Chunk Structure

Chunks are composed of a header and tail sequence, essentially the exact same as GFS architecture's chunks. Header contains metadata about the chunk itself while the tail contains a slot map capable of mapping to individual records within the chunks.

### GFS Implementations 

* Implementation of Master to create a centralized system
* Fixed-sized Chunks are created from a File 
* Each ChunkHandle is unique
* Create, opem, read, write, and delete files
* Create, rename, list, and delete directory 
* Networking between Master, Client, and ChunkServer 
* Master handles all metadata: map of paths to potential Chunk Handle lists.  
* Append, Delete, Read First, Read Prev, Read Next, and Read Last of Records
* Data Replication: 3 chunk replicas assigned to different chunk servers 

## References

[1]: https://stackoverflow.com/questions/13530999/fastest-way-to-get-all-values-from-a-map-where-the-key-starts-with-a-certain-exp
