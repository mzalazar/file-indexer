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
  fn.call(this, msg)
  // MARK END
  let end = new Date() - start
  let hrend = process.hrtime(hrstart)

  // REPORT
  console.info('Chunk indexed in: %dms', end)
  console.log(`FINISH WITH CHUNK ${msg.data.part}, NOW SENDING "READY" SIGNAL...`.bgYellow.black)
}

class IndexerMultithread {

  constructor(filename, tempdir) {
    console.log(`I'm indexer multi-core!`.bgGreen.black)
    this.filename = filename
    this.tempIndex = null
    this.tempDir = tempdir
    this.buffer = null
    this.fileHandle = null

    process.on('message', msg => {
      LOG('MESSAGE RECEIVED IN WORKER: ', msg)
      switch (msg.type) {
        case 'EXIT':
          LOG('WORKER DONE.')
          break
        case 'CHUNK':
          METER.call(this, this.processChunk, msg)
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
    console.log('SENDING STARTING "READY" SIGNAL...'.bgYellow.black)
    process.send({ type: 'READY' })
  }

  processChunk(chunk) {
    console.log('processChunk()'.green)
    let fromOffset = chunk.data.start
    let toOffset = chunk.data.end
    let chunkSize = toOffset - fromOffset
    // READ OFFSETS FROM FILE
    let buffer = Buffer.alloc(chunkSize)
    fs.readSync(this.fileHandle, buffer, 0, chunkSize, fromOffset)
    // INDEX PARTS (and write to buffer)
    let indexedBuffer = this.indexDataChunk(buffer)
    console.log(`indexedBuffer.length = ${indexedBuffer.length}`)
    // GENERATE INDEX FILE
    this.writeIndexFile(Buffer.from(indexedBuffer), chunk.data.part)
  }

  /*  generateIndexFile(data) {
      this.tempIndex = `${this.tempDir}/${this.filename}.index.${data.part}`
      LOG(this.tempIndex)
    }*/

  indexDataChunk(data) {
    console.log('indexDataChunk()'.green)
    data = data.toString()
    let locations = []
    const regex = /\n/g
    let match, currentBufferOffset = 0
    while ((match = regex.exec(data)) !== null) {
      locations.push(match.index)
      //      buffer.writeInt32BE(match.index, currentBufferOffset) // 4 bytes each
      // currentBufferOffset += 4
    }
    return locations
  }

  writeIndexFile(data, part) {
    console.log('writeIndexFile()'.green)
    console.log(`this.filename = ${this.filename}`)
    // GENERATE INDEX PART AND WRITE DATA
    let newIndex = `${this.tempDir}/${path.basename(this.filename)}.index.${part}`
    console.log(`creating index file: ${newIndex}`.bgGreen.black)
    fs.writeFileSync(newIndex, data)
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

  _closeFile(filename) {
    fs.closeSync(this.handles[filename].fileHandle) // CLOSE FILE
    if (this.handles[filename].indexHandle) {
      fs.closeSync(this.handles[filename].indexHandle) // CLOSE INDEX
    }
    delete this.handles[filename] // DELETE HANDLES
  }

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

//LOG(`I'm alive... process id: ${process.pid}`)

// Invoke indexer
let filename = process.argv[2]
let tempdir = process.argv[3]
LOG(`Indexing: ${filename}, ${tempdir}`)
const indexer = new IndexerMultithread(filename, tempdir)

module.exports = IndexerMultithread
