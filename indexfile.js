const path = require('path')
const indexer = require('./sentinel')

const parameter = process.argv[2]
const filename = path.resolve(parameter)

const i = new indexer(filename)
i.on('indexed', process.exit)

