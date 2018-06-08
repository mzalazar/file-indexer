# file-indexer
This class allows you to get lines (a range) from very BIG FILES very fast

## Usage

```javascript
const indexer = require('./indexer')
const i = new indexer()

// Get lines deep inside file :)
let lines = i.getLines('/home/mzalazar/big_file.txt', 5000000, 50000010)
```
