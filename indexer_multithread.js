const fs = require('fs')
const path = require('path')
const fsext = require('fs-ext')
const colors = require('colors')
const Uint64BE = require('int64-buffer').Uint64BE
const DEBUG = true

// Message only if DEBUG is ENABLED
const LOG = (msg, ...args) => {
  if (DEBUG) {
    console.log(msg, args)
  }
}
const FATAL = (err, msg) => {
  if (msg) {
    console.error(msg)
  }
  console.log(err)
  process.exit(1)
}

const METER = function (fn, msg) {
  // MARK START
  let start = new Date()
  let hrstart = process.hrtime()

  // EXECUTE FUNCTION
  let val = fn.call(this, msg)
  // MARK END
  let end = new Date() - start
  let hrend = process.hrtime(hrstart)

  // REPORT
  LOG('Chunk indexed in: %dms'.bgCyan.black, end)
  return val
}

class IndexerMultithread {

  constructor(filename, tempdir) {
    console.log(`I'm indexer multi-core!`.bgGreen.black)
    this.filename = filename
    this.tempIndex = null
    this.tempDir = tempdir
    this.fileHandle = null
    this.chunkSize = 0

    process.on('message', msg => {
      LOG('MESSAGE RECEIVED IN WORKER: ', msg)
      switch (msg.type) {
        case 'EXIT':
          LOG('WORKER DONE.')
          break
        case 'CHUNK':
          console.log(msg)
          this.chunkSize = msg.data.end - msg.data.start
          console.log(`chunkSize: ${this.chunkSize}`.bgBlue.white)
          this.processChunk(msg)
          LOG(`FINISH WITH CHUNK ${msg.data.part}, NOW SENDING "READY" SIGNAL...`.bgYellow.black)
          process.send({ type: 'READY', part: msg.data.part })
          break
        default:
          LOG('default')
          LOG(msg)
      }
    })

    // Startup!
    try {
      this.start()
    } catch (err) {
      FATAL(err)
    }
  }

  start() {
    this._openMainFile()
    LOG('SENDING STARTING "READY" SIGNAL...'.bgYellow.black)
    process.send({ type: 'READY' })
  }

  processChunk(chunk) {
    let fromOffset = chunk.data.start
    let toOffset = chunk.data.end
    // READ OFFSETS FROM FILE
    let buffer = Buffer.alloc(this.chunkSize)
    fs.readSync(this.fileHandle, buffer, 0, this.chunkSize, fromOffset)
    // INDEX PARTS (and write to buffer)
    let indexedData = METER.call(this, this.indexDataChunk, buffer)
    // GENERATE INDEX FILE
    console.log(indexedData)
    let length = indexedData.length
    let toWrite = Buffer.alloc(length * 5)
    for (let i = 0, j = 0; i < length; i++ , j += 5) {
      toWrite.writeUIntBE(indexedData[i], j, 5); // Write 5 bytes on each loop
    }
    if (chunk.data.part == 1) {
      // Concatenate first FIX OFFSET (line 1: offset 0) for CODE optimization purposes
      toWrite = Buffer.concat([Buffer.alloc(5), toWrite])
    }
    this.writeIndexFile(toWrite, chunk.data.part)
  }

  indexDataChunk(data) {
    data = data.toString('ascii')
    let offsetLF = []
    const regex = /\n/g
    let match, currentBufferOffset = 0
    while ((match = regex.exec(data)) !== null) {
      let index = match.index + 1
      offsetLF.push(index)
    }
    return offsetLF
  }

  writeIndexFile(data, part) {
    // GENERATE INDEX PART AND WRITE DATA
    //    let chunkSize = this.chunkSize.toString(16)
    let newIndex = `${this.tempDir}/${path.basename(this.filename)}.index.${part}`
    LOG(`writting to: ${newIndex}`.bgGreen.black)
    fs.writeFileSync(newIndex, data)
    LOG(data)
  }

  _deleteTempDir() {
    fs.unlinkSync(this.tempDir)
  }

  /*  _isAlreadyIndexing(filename) {
      return fs.existsSync(`${this.tempDir}/${this.filename}.temp_index`)
    }*/

  _renameExtension() {
    let filename = `${this.tempDir}/${this.filename}`
    fs.renameSync()
  }

  _openMainFile() {
    this.fileHandle = fs.openSync(this.filename)
  }

  _closeFiles() {
    try {
      fs.closeSync(this.fileHandle)
    } catch (err) {
      console.error('Error closing filehandle')
    }

    try {
      fs.closeSync(this.indexHandle)
    } catch (err) {
      console.error('Error closing indexhandle')
    }
  }

  _openIndexFile() {

  }

  /*  _indexExists(filename) {
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
    }*/

  _getFileSize(filename) {
    fs.fstat
  }
  /*
        // SHOW SOME PROGRESS
        let mbLeft = this.handles[filename].size
        readableStream.on('data', chunk => {
          mbLeft -= chunk.length
          process.stdout.clearLine()
          process.stdout.write(`\x1Bc\rRemaining data: ${Number(mbLeft / 1024 / 2014).toFixed(1)} Mb`)
        })
  
          // RENAME INDEX FILE
          try {
            fs.renameSync(`${filename}.index_temp`, `${filename}.index`)
          } catch (err) {
            LOG(`Cannot rename ${filename}.index_temp`, err)
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
          LOG(`File ${filename} INDEXED, now returning`)
  
          this.handles[filename].indexed = true
          this.handles[filename].indexing = false
          return resolve()
        })*/

  //╔════════════════════════════════════╗
  //║ GET LINES FROM FILE USING AN INDEX ║
  //╚════════════════════════════════════╝
  getLines(filename, fromLine, toLine) {
    fromLine = parseInt(fromLine, 10)
    toLine = parseInt(toLine, 10)
    filename = path.resolve(filename) // Convert to absolute path
    this._openFile(filename)
    LOG('filename ' + filename + ' opened')
    if (this.handles[filename].indexed == false) {
      // LETS MAKE AN INDEX!
      LOG('Index file not found... making a new one.')
      this.makeIndex(filename)
    }

    // GET HANDLE
    let fd = this.handles[filename].indexHandle
    let offsetStart = this._getStartLinePosition(fd, fromLine)
    let offsetEnd = this._getEndLinePosition(fd, toLine)
    let readSize = offsetEnd - offsetStart
    LOG('offsetStart: ' + offsetStart)
    LOG('offsetEnd: ' + offsetEnd)
    LOG('readSize: ' + readSize)

    // PREPARE BUFFER
    let buffer = new Buffer.alloc(readSize, 0) // FILL WITH 0x0.
    try {
      let fd = this.handles[filename].fileHandle
      // READ THE FILE
      LOG(`fseeking to position ${offsetStart}`)
      fsext.seekSync(fd, offsetStart, 0)
      LOG(`reading ${readSize} bytes`)
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
        LOG(`fseeking to position ${offset}`)
        fsext.seekSync(fd, offset, 0)
        LOG(`reading 8 bytes`)
        fs.readSync(fd, buffer, 0, 8, null)
        LOG(buffer)
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
    LOG('_getEndLinePosition()')
    let offset
    let buffer = new Buffer.alloc(8, 0) // FILL WITH 0x0.
    if (line === 1) {
      offset = 0
    } else {
      offset = (line * 8) - 8
    }
    try {
      LOG(`fseeking to position ${offset}`)
      fsext.seekSync(fd, offset, 0)
      LOG(`reading 8 bytes`)
      fs.readSync(fd, buffer, 0, 8, null)
      LOG(buffer)
      var num = new Uint64BE(buffer).toNumber() + 1
      return num
    } catch (err) {
      throw err
    }
  }

  // GET FILE PART
  getFileChunk(fromOffset, toOffset) {
    const count = toOffset - fromOffset + 1
    const buffer = Buffer.alloc(count)
    try {
      fs.seekSync(this.fileHandle, fromOffset, 0)
      fs.readSync(this.fileHandle, buffer, 0, count, fromOffset)
    } catch (err) {
      throw new Error(err)
    }
    return buffer
  }

}

// Invoke indexer
let filename = process.argv[2]
let tempdir = process.argv[3]
LOG(`Indexing: ${filename}, ${tempdir}`)
const indexer = new IndexerMultithread(filename, tempdir)

module.exports = IndexerMultithread
