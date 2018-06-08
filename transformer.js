const fs = require('fs')
const stream = require('stream')

const Transform = stream.Transform

class transformer extends Transform {
    constructor() {
        super()
        this.oldChunkSize = 0
    }

    _transform(chunk, enc, done) {
        let arr = []
        let regex = new RegExp('\r|\n', 'g')
        let response;
        while (response = regex.exec(chunk)) {
            let offset = response.index + this.oldChunkSize
            arr.push(offset)
        }
        if (arr.length === 0) return done() // If NO new line detected, return
        this.oldChunkSize += chunk.length
        arr = arr.map((val) => val.toString(16))
        let data = ''

        for (var i = 0, l = arr.length; i < l; i++) {
            let hex = arr[i];
            hex = hex.padStart(16, '0')
            // PUT BYTES SEQUENTIALLY IN OUR data variable NO LOOPS HERE (better performance)
            data += hex.substr(0, 2)
            data += hex.substr(2, 2)
            data += hex.substr(4, 2)
            data += hex.substr(6, 2)
            data += hex.substr(8, 2)
            data += hex.substr(10, 2)
            data += hex.substr(12, 2)
            data += hex.substr(14, 2)
        }

        // Create Buffer to contain our addresses
        let buff = new Buffer.from(data, 'hex')
        this.push(buff)
        done()
    }
}

module.exports = transformer
