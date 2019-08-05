const fs = require('fs')
const path = require('path')
const fsext = require('fs-ext')
const Uint64BE = require('int64-buffer').Uint64BE
const filter = require('./transformer')

class Indexer {

  constructor() {
    console.log(`I'm indexer single-core!`)
    this.DEBUG = false
    this.handles = [] // Open files are cached here
    this.cpuCount = cpuCount
    this.remainingChunks
    this.tempDir = null
  }

  _createTempDir() {
    const dirname = `tempdir_${Date.now()}`
    fs.mkdirSync(dirname)
    return dirname
  }

  _fixCarriageReturns() {
    return new Promise((resolve, reject) => {
      const readableStream = fs.createReadStream(this.filename)
      const writableStream = fs.createWriteStream(this.tempIndex)
      const carriageFixer = new Transform({
        transform(chunk, encoding, callback) {
          chunk = chunk.replace(/\r\n|\r/g, '\n')
          this.push(chunk)
          callback()
        }
      })
      // Set Error Handlers
      readableStream.on('error', (err) => {
        console.error(err)
        return reject(err)
      })

      writableStream.on('error', (err) => {
        console.error(err)
        return reject(err)
      })

      // Normalize text (CRLF -> LF)
      readableStream.pipe(new carriageFixer()).pipe(writableStream)

      // Done fixing file
      writableStream.on('finish', () => {
        resolve()
      })
    })
  }

  _openFile(filename) {
    if (this.DEBUG) console.log('opening file')
    let fileStatus
    // CHECK IF FILE EXISTS (IF IT DOES, THEN GET INFO)
    try {
      fileStatus = fs.statSync(filename) // GET FILE INFO
    } catch (err) {
      this._sendError(err)
    }

    // OPEN MAIN FILE
    try {
      let fileHandle = fs.openSync(filename, 'r') // OPEN THE FILE
      this.handles[filename] = {
        fileHandle: fileHandle,
        size: fileStatus.size
      }
    } catch (err) {
      this._sendError(err)
    }

    let indexExists = false
    let indexHandle
    try {
      // OPEN INDEX FILE
      indexHandle = fs.openSync(`${filename}.index`, 'r') // OPEN INDEX FILE
      indexExists = true
    } catch (err) {
      this.handles[filename].indexed = false
      return
    }

    if (indexExists) {
      this.handles[filename].indexed = true
      this.handles[filename].indexHandle = indexHandle
    }
  }

  _sendError(err) {
    if (process.send) {
      process.send(err)
    } else {
      if (this.DEBUG) console.err('An error ocurred, unable to send message to master process.')
    }
  }

  _closeFile(filename) {
    try {
      fs.closeSync(this.handles[filename].fileHandle) // CLOSE FILE
      if (this.handles[filename].indexHandle) {
        fs.closeSync(this.handles[filename].indexHandle) // CLOSE INDEX
      }
      delete this.handles[filename] // DELETE HANDLES
    } catch (err) {
      throw err
    }
  }

  _getFileSize(filename) {
    return this.handles[filename].size
  }

  //╔════════════════════════╗
  //║ MAIN INDEXING FUNCTION ║
  //╚════════════════════════╝
  makeIndex(filename) {
    return new Promise((resolve, reject) => {

      // MAKE FILENAME ABSOLUTE
      filename = path.resolve(filename)
      this._openFile(filename)
      this.handles[filename].indexed = false
      this.handles[filename].indexing = true

      // Input Stream
      const readableStream = fs.createReadStream(filename, {
        encoding: 'ascii'
      })

      // Output Stream
      const writableStream = fs.createWriteStream(`${filename}.index_temp`, {
        encoding: 'ascii'
      })

      // USE STREAM
      readableStream.pipe(new filter()).pipe(writableStream)

      // SHOW SOME PROGRESS
      let mbLeft = this.handles[filename].size
      readableStream.on('data', chunk => {
        mbLeft -= chunk.length
        process.stdout.clearLine()
        process.stdout.write(`\x1Bc\rRemaining data: ${Number(mbLeft / 1024 / 2014).toFixed(1)} Mb`)
      });

      // ON END
      readableStream.on('end', () => {
        writableStream.end()
        // RENAME INDEX FILE
        try {
          fs.renameSync(`${filename}.index_temp`, `${filename}.index`)
        } catch (err) {
          if (this.DEBUG) console.log(`Cannot rename ${filename}.index_temp`, err)
          return reject(err)
        }
        process.stdout.write('\n') // WRITE A LAST NEWLINE (to clean)

        // OPEN OUR NEW SHINY INDEX
        try {
          let indexHandle = fs.openSync(`${filename}.index`, 'r')
          this.handles[filename].indexHandle = indexHandle
        } catch (err) {
          // FATAL ERROR
          return reject(err)
        }
        if (this.DEBUG) console.log(`File ${filename} INDEXED, now returning`)

        this.handles[filename].indexed = true
        this.handles[filename].indexing = false
        return resolve()
      })
      // ON ERROR
      readableStream.on('error', (err) => {
        if (this.DEBUG) console.log(err)
        return reject(err)
      });
    })
  }

  //╔════════════════════════════════════╗
  //║ GET LINES FROM FILE USING AN INDEX ║
  //╚════════════════════════════════════╝
  async getLines(filename, fromLine, toLine) {
    fromLine = parseInt(fromLine, 10)
    toLine = parseInt(toLine, 10)
    filename = path.resolve(filename) // Convert to absolute path
    this._openFile(filename)
    if (this.DEBUG) console.log('filename ' + filename + ' opened')
    if (this.handles[filename].indexed == false) {
      // LETS MAKE AN INDEX!
      if (this.DEBUG) console.log('MAKING AN INDEX!')
      await this.makeIndex(filename)
    }

    // GET HANDLE
    let fd = this.handles[filename].indexHandle
    let offsetStart = this._getStartLinePosition(fd, fromLine)
    let offsetEnd = this._getEndLinePosition(fd, toLine)
    let readSize = offsetEnd - offsetStart
    if (this.DEBUG) console.log('offsetStart: ' + offsetStart)
    if (this.DEBUG) console.log('offsetEnd: ' + offsetEnd)
    if (this.DEBUG) console.log('readSize: ' + readSize)

    // PREPARE BUFFER
    let buffer = new Buffer.alloc(readSize, 0) // FILL WITH 0x0.
    try {
      let fd = this.handles[filename].fileHandle
      // READ THE FILE
      if (this.DEBUG) console.log(`fseeking to position ${offsetStart}`)
      fsext.seekSync(fd, offsetStart, 0)
      if (this.DEBUG) console.log(`reading ${readSize} bytes`)
      fs.readSync(fd, buffer, 0, readSize, null)
      // RETURN LINES!
      return buffer.toString()
    } catch (err) {
      // FATAL ERROR
      throw err
    }
  }

  /**************/
  /* MARK START */
  /**************/
  _getStartLinePosition(fd, line) {
    let offset
    let buffer = new Buffer.alloc(8, 0) // FILL WITH 0x0.
    if (line === 1) {
      var num = new Uint64BE(0).toNumber()
      return num
    } else {
      // Set offset
      offset = (line * 8) - 16
      try {
        if (this.DEBUG) console.log(`fseeking to position ${offset}`)
        fsext.seekSync(fd, offset, 0)
        if (this.DEBUG) console.log(`reading 8 bytes`)
        fs.readSync(fd, buffer, 0, 8, null)
        if (this.DEBUG) console.log(buffer)
        var num = new Uint64BE(buffer).toNumber() + 1
        return num
      } catch (err) {
        throw err
      }
    }
  }

  /************/
  /* MARK END */
  /************/
  _getEndLinePosition(fd, line) {
    if (this.DEBUG) console.log('_getEndLinePosition()')
    let offset
    let buffer = new Buffer.alloc(8, 0) // FILL WITH 0x0.
    if (line === 1) {
      offset = 0
    } else {
      offset = (line * 8) - 8
    }
    try {
      if (this.DEBUG) console.log(`fseeking to position ${offset}`)
      fsext.seekSync(fd, offset, 0)
      if (this.DEBUG) console.log(`reading 8 bytes`)
      fs.readSync(fd, buffer, 0, 8, null)
      if (this.DEBUG) console.log(buffer)
      var num = new Uint64BE(buffer).toNumber() + 1
      return num
    } catch (err) {
      throw err
    }
  }
}
module.exports = Indexer
