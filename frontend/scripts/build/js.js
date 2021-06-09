const { esbuildConfig } = require('../helpers')
const esbuild = require('esbuild')

function js() {
  const start = process.hrtime()
  console.log('[js] start')
  const { metafile } = esbuild.buildSync(esbuildConfig)
  const elapsed = process.hrtime(start)[1] / 1000000
  console.log('[js] bundled', Object.keys(metafile.inputs).length, 'files in', elapsed + 'ms')
  return Object.keys(metafile.outputs).find(outFile => outFile.endsWith('.js'))
}

module.exports = {
  js
}

