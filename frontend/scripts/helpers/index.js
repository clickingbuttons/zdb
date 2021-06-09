const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

const paths = {
  outdir: 'dist',
  staticDir: 'static',
}

function getHash(string) {
	if (process.env.NODE_ENV === 'production') {
		const hash = crypto.createHash('md5').update(string).digest('hex')
		return hash.substr(0, 5)
	}

	return 'dev'
}

function _walk(dir, options, res) {
  if (typeof options.ext === 'string') {
    options.ext = new RegExp(options.ext)
  }
	fs.readdirSync(dir).forEach(file => {
    const filepath = path.join(dir, file);
    try {
      const stats = fs.statSync(filepath)
      if (stats.isDirectory()) {
        _walk(filepath, options, res)
      } else if (stats.isFile() && options.ext.test(filepath)) {
        res.push(filepath)
      }
    } catch { /* can't stat file */ }
  })
}

function walk(dir, options = { ext: /\..*$/ }) {
  const res = []
  _walk(dir, options, res)
  return res
}

const esbuildConfig = {
  entryPoints: [
    path.join(process.cwd(), 'src/entry.jsx')
  ],
  entryNames: '[name]',
  metafile: true,
  bundle: true,
  outdir: paths.outdir,
  jsxFactory: 'h',
  jsxFragment: 'Fragment',
  loader: {
    '.svg': 'dataurl',
    /* can't yet configure where these are copied to
    '.png': 'file',
    '.jpg': 'file',
    '.gif': 'file'
    */
  },
  define: {
    ZDB_URL: JSON.stringify('http://localhost:7878')
  },
}

module.exports = {
  paths,
  getHash,
  walk,
  esbuildConfig
}

