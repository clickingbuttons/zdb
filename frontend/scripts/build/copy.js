const { paths } = require('../helpers')
const path = require('path')
const fs = require('fs')

const fromPath = paths.staticDir
const outdir = paths.outdir

module.exports = {
  copy() {
    const start = process.hrtime()
    let fileCount = 0
    console.log('[copy] start')
    if (!fs.existsSync(outdir)) {
      fs.mkdirSync(outdir)
    }

    fs.readdirSync(fromPath).forEach(file => {
      fs.copyFileSync(path.join(fromPath, file), path.join(outdir, file))
      fileCount++
    })
    const elapsed = process.hrtime(start)[1] / 1000000
    console.log('[copy] copied', fileCount, 'files in', elapsed + 'ms')
  }
}

