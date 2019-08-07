const fs = require('fs')
const SHOW_LAST_LF = false

try {
  let lineNum = 150756 // line to read
  readLine(lineNum)
  //  readLines(150745, 150746)
  //  readLines(3, 4)
} catch (err) {
  console.log(err)
  process.exit(1)
}

function readLine(lineNum) {
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

function readLines(fromLine, toLine) {
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

