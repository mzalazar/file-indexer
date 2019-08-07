'use strict'
const { spawn, fork } = require('child_process')
require('better-log/install')
const path = require('path')
const colors = require('colors')
const numCores = require('os').cpus().length
const fs = require('fs')
const glob = require('glob')
const sort = require('alphanum-sort')
const mb = 1024 * 1024
const SHOW_LAST_LF = false // SHOW LAST 'LF' WHEN SHOWING LINES
const DEBUG = true

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

const process_options = {
	stdio: 'inherit'
}

class Indexer {

	constructor(filename) {
		this.minFileSize = 100
		this.chunkSize = 10000000 // MUST BE AN EVEN NUMBER
		this.filesize = null
		this.filename = filename
		this.isMultiCpu = numCores > 1
		//		this.childs = [] // [{id:1, part:1, done:false, idx:`${this.tempDir}/${this.filename}.idx`}, ...]
		this.tempDir = null
		this.fixCRLF = true
		this.chunks = []
		this.threads = 0

		process.on('uncaughtException', FATAL)

		this.start()
			.catch(FATAL)
	}

	async start() {
		// VALIDATE PARAMETER
		if (typeof this.filename !== 'string' || this.filename.length === 0) {
			throw new Error('Filename is required!')
			process.exit(1)
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

	launchMultithread() {
		console.log('JOBS:'.yellow)
		console.log(this.chunks)
		console.log('Master process is running with pid:', process.pid)
		console.log(`CORES: ${numCores}`)
		for (let i = 0; i < numCores; i++) {
			const thread = fork('./indexer_multithread.js', [this.filename, this.tempDir], process_options)
			this.threads++
			console.log(`Launched thread id: ${thread.pid}`)
			// ERROR HANDLER
			thread.on('error', (err) => {
				console.log(`Thread ${thread.id} throws an error: ${err}`)
				process.exit(1)
			})

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
						} else {
							console.log('No more chunks available, sending SIGTERM to thread.'.bgRed.black)
							thread.kill('SIGTERM')
						}
						break;
					default:
						console.log('default')
						console.log(msg)
				}
			})

			thread.on('exit', () => {
				console.log('Thread exit!')
				// If NOT flushing index right now... then do it!
				this.threads--
				if (this.threads === 0) {
					console.log('LAST THREAD HAS FINISHED!'.rainbow)
					this.mergeIndexes()
				}
			})
		}
	}

	mergeIndexes() {
		console.log('STARTING MERGE'.bgRed.black)
		let masterIndex = this.createMasterIndex()
		const self = this;
		// options is optional
		const files = sort(glob.sync(`${this.tempDir}/*`))
		let master_offset = 0
		files.forEach((e, i) => {
			//			let lastOne = (i === files.length - 1 ? true : false)
			console.log(`Processing ${e}`)
			let size = fs.statSync(e).size
			let indexPart = fs.openSync(e, 'r')
			let buffer = Buffer.alloc(size)
			let bytesReaded = fs.readSync(indexPart, buffer, 0, size, 0) // READ ENTIRE FILE
			console.log(`bytesReaded: ${bytesReaded}`)
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
			console.log(`Written ${bytesWritten} bytes.`)
			// Here... we need to PARSE last "offset" detected (\n) and subtract that offset
			master_offset += this.chunkSize
			// Close file
			fs.closeSync(indexPart)
		})
		// Close master index
		fs.closeSync(masterIndex)

		// REMOVE DIRECTORY AND FILES
		setTimeout(() => {
			self.deleteFolderRecursive(self.tempDir)
		}, 1000)
	}

	createMasterIndex() {
		let filename = `${this.filename}.index`
		const handle = fs.openSync(filename, 'w')
		return handle
	}

	deleteFolderRecursive(path) {
		console.log(`deleting ${this.tempDir}`)
		if (fs.existsSync(path)) {
			fs.readdirSync(path).forEach(function (file, index) {
				var curPath = `${path}/${file}`
				console.log(`To be deleted: ${path}/${file}`)
				process.exit()
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
		console.log('launchSinglethreaded.')
		const thread = fork('./indexer_singlethread.js', [this.filename])

		// CHECK IF INDEXER FINISH
		thread.on('exit', (code) => {
			console.log(`child process exited with code ${code}`)
			process.exit() // EXIT MAIN PROCESS
		})
	}


	_getFileSize() {
		let data
		console.log(`CHECKING: ${this.filename}`)
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
		console.log(`chunksCount: ${chunksCount}`)
		for (let i = 1; i <= chunksCount; i++) {
			this.chunks.push({ start: currentOffset, end: (i === chunksCount ? currentOffset + remainderBytes : currentOffset + this.chunkSize), part: i })
			currentOffset += this.chunkSize // Increment by chunkSize
			//			remainderBytes -= this.chunkSize // Decrement by chunkSize
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
			console.log('Fixing CRLF...')
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
	readLine(lineNum) {
		// *** read from coordinates until NEXT CARRIAGE RETURN ***
		let indexFileName = '50m.txt.index'
		let mainFileName = '50m.txt'
		let indexHandle = fs.openSync(indexFileName)
		let fileHandle = fs.openSync(mainFileName)
		let maxLine = (fs.statSync(indexFileName).size / 5) - 1
		// compare if maxLine is reached.
		console.log(`lineNum: ${lineNum}`)
		console.log(`maxLine: ${maxLine}`)
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
			console.log(`------>${bufferFromFile.toString()}<------`)
		} else {
			console.log('Max line was reached, can\'t read after EOF!')
			process.exit(1)
		}
	}

	readLines(fromLine, toLine) {
		let indexFileName = '50m.txt.index'
		let mainFileName = '50m.txt'
		let indexHandle = fs.openSync(indexFileName)
		let fileHandle = fs.openSync(mainFileName)
		let maxLine = (fs.statSync(indexFileName).size / 5) - 1
		// Some validation
		if (fromLine >= toLine || toLine > maxLine) {
			console.log('An error has occurred, i can\'t give you what you want... i think some numbers are misplaced.')
			process.exit(1)
		}
		let bufferFromIndex = Buffer.alloc(5)
		let bufferToIndex = Buffer.alloc(5)
		fs.readSync(indexHandle, bufferFromIndex, 0, 5, (fromLine - 1) * 5)
		fs.readSync(indexHandle, bufferToIndex, 0, 5, (toLine) * 5)
		let fileOffsetFrom = bufferFromIndex.readUIntBE(0, 5)
		console.log(`fileOffsetFrom: ${fileOffsetFrom}`)
		let fileOffsetTo = bufferToIndex.readUIntBE(0, 5)
		if (!SHOW_LAST_LF) {
			fileOffsetTo-- // FIX (doesn't print last LF)
		}
		console.log(`fileOffsetTo: ${fileOffsetTo}`)
		let length = fileOffsetTo - fileOffsetFrom
		let bufferFromFile = Buffer.alloc(length)
		fs.readSync(fileHandle, bufferFromFile, 0, length, fileOffsetFrom)
		//    console.log(`------>${bufferFromFile.toString().replace(/\s/g, '_')}<------`)
		console.log(`------>${bufferFromFile.toString()}<------`)
	}

}

module.exports = Indexer

