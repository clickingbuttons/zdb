const { paths } = require('./helpers')
const fs = require('fs')

function clean() {
  console.log('[clean] start')
  const start = process.hrtime()
  fs.rmSync(paths.outdir, { recursive: true, force: true })
  const elapsed = process.hrtime(start)[1] / 1000000
  console.log('[clean] cleaned in', elapsed + 'ms')
}

if (require.main === module) clean()

module.exports = {
  clean
}
