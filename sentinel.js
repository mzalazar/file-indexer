'use strict'
//const indexer_multithread = require('./indexer_multithread')
const indexer_singlethread = require('./indexer_singlethread')
const { spawn, fork } = require('child_process')
require('better-log/install')
const path = require('path')
const colors = require('colors')
const numCores = require('os').cpus().length
const fs = require('fs')
const glob = require('glob')
const sort = require('alphanum-sort')
const mb = 1024 * 1024
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

const process_options = {
	stdio: 'inherit'
}

class Indexer {

	constructor(filename) {
		this.minFileSize = 100
		this.chunkSize = 100000 // MUST BE AN EVEN NUMBER
		this.filesize = null
		this.filename = filename
		this.isMultiCpu = numCores > 1
		//		this.childs = [] // [{id:1, part:1, done:false, idx:`${this.tempDir}/${this.filename}.idx`}, ...]
		this.tempDir = null
		this.fixCRLF = true
		this.chunks = []
		this.flushing = false

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
						LOG(`Received "READY" from worker!`.bgYellow.black)
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
				if (!this.flushing) {
					this.flushing = true
					this.mergeIndexes()
				}
			})
		}
	}

	mergeIndexes() {
		console.log('mergeIndexes()'.red)
		const fileHandle = this.createMasterIndex()
		// options is optional
		glob(`${this.tempDir}/*`, function (err, files) {
			let master_offset = 0
			sort(files).map(e => {
				console.log(`Processing ${e}`)
				let buffer = fs.readFileSync(e) // READ ENTIRE FILE
				console.log(buffer)
				let length = buffer.length
				// Write each offset (readed from file) and add master_offset value to every offset found.
				let newData = Buffer.alloc(length)
				console.log('length: ' + newData.length)
				for (let offsetBuffer = 0; offsetBuffer <= (length - 5); offsetBuffer += 5) {
					let extracted = buffer.readUIntBE(offsetBuffer, 5)
					newData.writeUIntBE(extracted + master_offset, offsetBuffer, 5)
				}
				// Flush data to index file!
				fs.writeSync(fileHandle, newData)
			})
			// Close master index
			fs.closeSync(fileHandle)
		})

		/*
			TODO: Crear algunos archivos índices (diminutos) para probar el ORDENAMIENTO
			// Foreach file in tempDir
			// Open it
			// SET master_offset = last_index_filesize_processed || 0 // At first file it will be ZERO
			// Write each offset (readed from file) and add master_offset value to every offset found.
			// Rinse and repeat until no more files remainds.
			// BE HAPPY!
			const tempDir = temp_pruebas
			files = new FileSet(`$tempDir/*`);
			const regex = /index.([0-9A-F]+).part.(\d+)/
			const orderByPart = (a, b) => {

			}
		*/
		// REMOVE DIRECTORY AND FILES
		setTimeout(() => {
			this.deleteFolderRecursive(this.tempDir)
		}, 1000)
	}

	createMasterIndex() {
		let filename = `${this.filename}.index`
		const handle = fs.openSync(filename, 'a')
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

	flushIndex() {
		//const sourceIdx = new 
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

	//  "splits"(only calculate offsets) data in "chunks" of 100.000 bytes
	/*	divideWorkload() {
			let size = this.filesize
			let chunk = {}
			for (let i = 0; i < this.filesize; i++) {
				chunk.start =
					chunks.push(chunk)
			}
		}*/


}

module.exports = Indexer

