'use strict'
const { spawn, fork } = require('child_process')
require('better-log/install')
const EventEmitter = require('events')
const path = require('path')
const colors = require('colors')
const numCores = require('os').cpus().length
const fs = require('fs')
const glob = require('glob')
const sort = require('alphanum-sort')
const mb = 1024 * 1024
const SHOW_LAST_LF = false // SHOW LAST 'LF' WHEN SHOWING LINES
const DEBUG = false

// Message only if DEBUG is ENABLED
const LOG = (msg, ...args) => {
	if (DEBUG) {
		if (args.length === 0) {
			console.log(msg)
		} else {
			console.log(msg, args)
		}
	}
}

const FATAL = (err, msg) => {
	if (msg) {
		console.error(msg)
	}
	console.log(err)
	process.exit(1)
}

const replaceAt = (string, index, replace) => {
	return string.substring(0, index) + replace + string.substring(index + 1)
}

const process_options = {
	stdio: 'inherit'
}

class Indexer extends EventEmitter {

	constructor(filename) {
		super()
		this.minFileSize = 100 // In megabytes
		this.chunkSize = 10000000 // MUST BE AN EVEN NUMBER
		this.filesize = null
		this.filename = filename
		this.isMultiCpu = numCores > 1
		//		this.childs = [] // [{id:1, part:1, done:false, idx:`${this.tempDir}/${this.filename}.idx`}, ...]
		this.tempDir = null
		this.fixCRLF = true
		this.chunks = []
		this.chunksProcessed = 0
		this.threads = 0

		process.on('uncaughtException', FATAL)

		const indexExists = this.checkIndexExistence()
		if (indexExists) {
			LOG('1 Index already exists, no need to reindex.')
			// Need to do this in next "event loop"
			process.nextTick(() => {
				this.emit('indexed')
			})
		} else {
			this.startIndexing()
				.catch(FATAL)
		}
	}

	async startIndexing() {
		// VALIDATE PARAMETER
		if (typeof this.filename !== 'string' || this.filename.length === 0) {
			FATAL('Filename is required!')
		}

		// VALIDATE FILE EXISTENCE (getting filesize)
		this._getFileSize()

		// VALIDATE FILE SIZE
		/*		if (this.filesize < this.minFileSize * mb) { ** LO COMENTO POR AHORA
					throw new Error(`Cannot index a file less than ${this.minFileSize}Mb`)
				}*/

		// FIX CARRIAGE RETURN (if option is set)
		if (this.fixCRLF) {
			await this._fixCRLF()
		}

		this._divideCpuLoad()

		// CREATE TEMP DIRECTORY (FOR INDEXED PARTS)
		this._createTempDir()

		// LAUNCH WORKERS
		this.launchMultithread()
		/*		} else {
					// LAUNCH A SINGLE THREAD
					this.launchSinglethreaded()
				}*/
	}

	checkIndexExistence() {
		try {
			const size = fs.statSync(`${this.filename}.index`).size
			// We can do an exaustive index testing here.
			return size > 0
		} catch (err) {
			if (err.code && err.code === 'ENOENT') {
				return false
			} else {
				FATAL(err, 'Error checking index existence.')
			}
		}
	}

	launchMultithread() {
		LOG('JOBS:'.yellow)
		LOG(this.chunks)
		LOG('Master process is running with pid:', process.pid)
		LOG(`CORES: ${numCores}`)
		let completedBar = '░░░░░░░░░░░░░░░░░░░░'
		for (let i = 0; i < numCores; i++) {
			const thread = fork('./indexer_multithread.js', [this.filename, this.tempDir], process_options)
			this.threads++
			LOG(`Launched thread id: ${thread.pid}`)
			// ERROR HANDLER
			thread.on('error', FATAL)

			// WORKER FINISH INDEXING PART
			thread.on('message', (msg) => {
				switch (msg.type) {
					case 'READY':
						//						console.log(`READY part: ${msg.part}`)
						LOG(`Received "READY" from worker:`.bgYellow.black)
						LOG(msg)
						// If there are more CHUNKS, give one to worker
						if (this.chunks.length) {
							let chunk = this.chunks.shift()
							thread.send({ type: 'CHUNK', data: chunk })
							this.chunksProcessed++
							let currentPercentage = Math.floor((this.chunksProcessed * this.chunkSize) / this.filesize * 100)
							for (let i = 0; i < 20; i++) {
								if (i * 5 <= currentPercentage) {
									completedBar = replaceAt(completedBar, i, '▓')
								}
							}
							if (!DEBUG) {
								process.stdout.write(`\x1Bc\r${completedBar} ${currentPercentage}%`)
								//								console.log(completedBar)
								if (currentPercentage == 0) {
									console.log('\nDone indexing.')
									console.log('Starting index merge...')
								}
							}
						} else {
							LOG('No more chunks available, sending SIGTERM to thread.'.bgRed.black)
							thread.kill('SIGTERM')
						}
						break;
					default:
						LOG(msg)
				}
			})

			thread.on('exit', () => {
				LOG('Thread exit!')
				// If NOT flushing index right now... then do it!
				this.threads--
				if (this.threads === 0) {
					LOG('LAST THREAD HAS FINISHED!'.rainbow)
					this.mergeIndexes()
				}
			})
		}
	}

	mergeIndexes() {
		LOG('STARTING MERGE'.bgRed.black)
		let masterIndex = this.createMasterIndex()
		const self = this;
		// options is optional
		const files = sort(glob.sync(`${this.tempDir}/*`))
		let master_offset = 0
		files.forEach((e, i) => {
			//			let lastOne = (i === files.length - 1 ? true : false)
			LOG(`Processing ${e}`)
			let size = fs.statSync(e).size
			let indexPart = fs.openSync(e, 'r')
			let buffer = Buffer.alloc(size)
			let bytesReaded = fs.readSync(indexPart, buffer, 0, size, 0) // READ ENTIRE FILE
			LOG(`bytesReaded: ${bytesReaded}`)
			// Write each offset (readed from file) and add master_offset value to every offset found.
			let newData = Buffer.alloc(size)
			let newValue
			for (let offsetBuffer = 0; offsetBuffer <= (size - 5); offsetBuffer += 5) {
				let offsetLF = buffer.readUIntBE(offsetBuffer, 5)
				if (i > 0) {
					// ADD MASTER OFFSET
					newValue = offsetLF + master_offset
				} else {
					// THIS IS THE FIRST CHUNK, CONTINUE WITHOUT ADDING OFFSET
					newValue = offsetLF
				}
				newData.writeUIntBE(newValue, offsetBuffer, 5)
			}
			// Flush data to index file!
			let bytesWritten = fs.writeSync(masterIndex, newData, 0)
			LOG(`Written ${bytesWritten} bytes.`)
			// Here... we need to PARSE last "offset" detected (\n) and subtract that offset
			master_offset += this.chunkSize
			// Close file
			fs.closeSync(indexPart)
		})
		// Close master index
		fs.closeSync(masterIndex)

		// REMOVE DIRECTORY AND FILES
		self.deleteFolderRecursive(self.tempDir)
		this.emit('indexed')
	}

	createMasterIndex() {
		let filename = `${this.filename}.index`
		const handle = fs.openSync(filename, 'w')
		return handle
	}

	deleteFolderRecursive(path) {
		LOG(`deleting ${this.tempDir}`)
		if (fs.existsSync(path)) {
			fs.readdirSync(path).forEach(function (file, index) {
				var curPath = `${path}/${file}`
				LOG(`To be deleted: ${path}/${file}`)
				if (fs.lstatSync(curPath).isDirectory()) { // recurse
					deleteFolderRecursive(curPath)
				} else { // delete file
					fs.unlinkSync(curPath)
				}
			})
			fs.rmdirSync(path)
		}
	}

	launchSinglethreaded() {
		LOG('launchSinglethreaded.')
		const thread = fork('./indexer_singlethread.js', [this.filename])

		// CHECK IF INDEXER FINISH
		thread.on('exit', (code) => {
			FATAL(`child process exited with code ${code}`)
		})
	}


	_getFileSize() {
		let data
		LOG(`CHECKING: ${this.filename}`)
		data = fs.statSync(this.filename)
		this.filesize = data.size
		this.isBigFile = this.filesize >= (this.minFileSize * mb)
		return this.filesize
	}


	_divideCpuLoad() {
		let isOdd = this.filesize % 2
		let filesize = isOdd ? this.filesize - 1 : this.filesize
		let chunksCount = Math.ceil(filesize / this.chunkSize)
		let remainderBytes = this.filesize - (this.chunkSize * (chunksCount - 1))
		let currentOffset = 0
		LOG(`chunksCount: ${chunksCount}`)
		for (let i = 1; i <= chunksCount; i++) {
			this.chunks.push({ start: currentOffset, end: (i === chunksCount ? currentOffset + remainderBytes : currentOffset + this.chunkSize), part: i })
			currentOffset += this.chunkSize // Increment by chunkSize
		}
	}

	_createTempDir() {
		let folder = path.dirname(path.basename(this.filename))
		this.tempDir = `${folder}/tempdir_${Date.now()}`
		fs.mkdirSync(this.tempDir)
		LOG(`Create temp directory: ${this.tempDir}`)
	}

	_fixCRLF() {
		return new Promise((resolve, reject) => {
			LOG('Fixing CRLF...')
			setTimeout(resolve, 100)
			/*
						const readableStream = fs.createReadStream(this.filename)
						const writableStream = fs.createWriteStream(this.tempIndex)
						const carriageFixer = new Transform({
							transform(chunk, encoding, callback) {
								chunk = chunk.replace(/\r\n|\r/g, '\n') // TODO: Is this correct?
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
			*/
		})
	}
	async readLine(lineNum) {
		let indexFileName = `${this.filename}.index`
		let mainFileName = this.filename
		// CHECK INDEX EXISTENCE
		const indexExists = this.checkIndexExistence()
		if (indexExists) {
			LOG('Index already exists, no need to reindex.')
		} else {
			await startIndexing()
		}
		let indexHandle = fs.openSync(indexFileName)
		let fileHandle = fs.openSync(mainFileName)
		let maxLine = (fs.statSync(indexFileName).size / 5) - 1
		LOG(`lineNum: ${lineNum}`)
		LOG(`maxLine: ${maxLine}`)
		// compare if maxLine is reached.
		if (lineNum <= maxLine) {
			let bufferFromIndex = Buffer.alloc(10)
			fs.readSync(indexHandle, bufferFromIndex, 0, 10, (lineNum - 1) * 5)
			let fileOffsetFrom = bufferFromIndex.readUIntBE(0, 5)
			let fileOffsetTo = bufferFromIndex.readUIntBE(5, 5)
			if (!SHOW_LAST_LF) {
				fileOffsetTo-- // Fix (doesn't show last LF)
			}
			let length = fileOffsetTo - fileOffsetFrom
			let bufferFromFile = Buffer.alloc(length)
			fs.readSync(fileHandle, bufferFromFile, 0, length, fileOffsetFrom)
			//			LOG(`------>${bufferFromFile.toString()}<------`)
			return bufferFromFile.toString()
		} else {
			FATAL('Max line was reached, can\'t read after EOF!')
		}
	}

	async readLines(fromLine, toLine) {
		let indexFileName = `${this.filename}.index`
		let mainFileName = this.filename
		// CHECK INDEX EXISTENCE
		const indexExists = this.checkIndexExistence()
		if (indexExists) {
			LOG('Index already exists, no need to reindex.')
		} else {
			await this.startIndexing()
		}
		let indexHandle = fs.openSync(indexFileName)
		let fileHandle = fs.openSync(mainFileName)
		let maxLine = (fs.statSync(indexFileName).size / 5) - 1
		console.log(`maxLine=${maxLine}`)
		// Some validation
		if (fromLine >= toLine || toLine > maxLine) {
			FATAL('An error has occurred, i can\'t give you what you want... i think some numbers are misplaced.')
		}
		let bufferFromIndex = Buffer.alloc(5)
		let bufferToIndex = Buffer.alloc(5)
		fs.readSync(indexHandle, bufferFromIndex, 0, 5, (fromLine - 1) * 5)
		fs.readSync(indexHandle, bufferToIndex, 0, 5, (toLine) * 5)
		let fileOffsetFrom = bufferFromIndex.readUIntBE(0, 5)
		LOG(`fileOffsetFrom: ${fileOffsetFrom}`)
		let fileOffsetTo = bufferToIndex.readUIntBE(0, 5)
		if (!SHOW_LAST_LF) {
			fileOffsetTo-- // FIX (doesn't print last LF)
		}
		LOG(`fileOffsetTo: ${fileOffsetTo}`)
		let length = fileOffsetTo - fileOffsetFrom
		let bufferFromFile = Buffer.alloc(length)
		fs.readSync(fileHandle, bufferFromFile, 0, length, fileOffsetFrom)
		//		LOG(`------>${bufferFromFile.toString()}<------`)
		return bufferFromFile.toString()
	}

}

module.exports = Indexer

