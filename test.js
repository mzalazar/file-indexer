const { spawn, fork } = require('child_process')
const indexer = require('./sentinel')
const fs = require('fs')
const path = require('path')
const process_options = {
  stdio: 'inherit'
}

const filename = './testfile.txt'
//let f = new i('./50m.txt');
//let f = new i('./testfile2.txt');

// Indexado se inicia al querer leer alguna/s linea/s
let i = new indexer(filename)
i.on('indexed', () => {
  i.readLines(900000, 900010)
    .then((lines) => {
      console.log('Lineas leídas:', lines)
    })
})
