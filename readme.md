## What this is 

#### 🧑‍🎓 Note: This was done for personal learning purposes 

This is a (single threaded) node implementation of the paper "MapReduce: Simplified Data Processing on Large Clusters"

https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

This is sort of an "Emulation" of some of the key logic of the paper, as this isn't actually a process running on multiple machines. 

The paper mentions a ton of features and optimizations, this implementation focuses on just a few core things. Here is the what it does

- When the process spins up, it creates workers
- It also takes a path to document, and creates a map task to map the document
- The map task is assigned to an available worker
- The map does does the mapping from user supplied map CB, and writes the mapped file
- A reduce task is dispatched, which is later picked up by an idle worker
- The worker runs the mapped data through user suppolied reduce CB and writes to disc

There's a lot more one can do here, so todos.md has some good next steps

## AI notes

For learning purposes, I wrote everything by hand. Occasionally I would chat with LLMs to help walk through opaque parts of the paper. 

## To run

`tsx main.ts`