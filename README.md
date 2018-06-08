# file-indexer
This class allows you to get lines (a range) from very BIG FILES very fast

## The class contains an useful function
getLines(filename, fromLine, toLine)

## Usage
```javascript
const indexer = require('./indexer')
const i = new indexer()

// Get lines deep inside file, from line 5000000 to line 5000010
let lines = i.getLines('/home/mzalazar/big_file.txt', 5000000, 50000010) // this is ultra-fast (once indexed)
```

## Index file
* Files are indexed using 64bits numbers stored in a file called filename.ext.index (as newline offsets)
* Files are indexed automatically when getLines() are called, or by calling makeIndex(filename)
* Files/indexes are kept open and their handlers are saved (acting like a cache, for fast access)

*This class is experimental, do whatever you need to improve it or use it as it is!*
