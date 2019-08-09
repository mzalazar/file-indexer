const fs = require('fs')
const path = require('path')
const colors = require('colors')
const Uint64BE = require('int64-buffer').Uint64BE
const DEBUG = false

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
    LOG(`I'm indexer multi-core!`.bgGreen.black)
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
          LOG(msg)
          this.chunkSize = msg.data.end - msg.data.start
          LOG(`chunkSize: ${this.chunkSize}`.bgBlue.white)
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
    LOG(indexedData)
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

}

// Invoke indexer
let filename = process.argv[2]
let tempdir = process.argv[3]
LOG(`Indexing: ${filename}, ${tempdir}`)
const indexer = new IndexerMultithread(filename, tempdir)

module.exports = IndexerMultithread
