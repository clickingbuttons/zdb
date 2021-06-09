const { paths } = require('./helpers')
const http = require('http')
const fs = require('fs')
const path = require('path')

const port = 3000
const livereloadScript = '(() => new EventSource("/livereload").onmessage = () => location.reload())()'
const mimes = {
  '.ico': 'image/x-icon',
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.json': 'application/json',
  '.css': 'text/css',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.wav': 'audio/wav',
  '.mp3': 'audio/mpeg',
  '.svg': 'image/svg+xml',
  '.pdf': 'application/pdf',
  '.doc': 'application/msword'
}
const clients = []

function serve() {
  http.createServer((req, res) => {
    // console.log(req.method, req.url)

    if (req.url === '/livereload') {
      return clients.push(
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
        })
      )
    }
    let pathname = path.join(paths.outdir, req.url)
    const ogPathname = pathname
    const ext = path.extname(pathname) || '.html'

    let stat
    try {
      stat = fs.statSync(pathname)
    }
    catch {
      res.statusCode = 404
      res.end(`File ${pathname} not found!`)
      return
    }
    if (stat.isDirectory()) {
      if (!req.url.endsWith('/')) {
        // Redirect like nginx
        res.statusCode = 301
        const loc = `http://${req.headers.host}${req.url}/`
        console.log('redirect', loc)
        res.setHeader('location', loc)
        res.end(`Location: ${loc}`)
        return
      }
      pathname += '/index.html'
      if (!fs.existsSync(pathname)) {
        // Show an index on disk
        res.end(
          '<h1>Directory listing</h1><ul style="font-size: 24px">'
          + fs.readdirSync(ogPathname).map(file =>
            `<li><a href="${file}">${file}</a></li>`
          ).join('\n')
          + '</ul>'
        )
        return
      }
    }

    fs.readFile(pathname, (err, data) => {
      if (err && !stat.isDirectory()) {
        res.statusCode = 500
        res.end(err)
      } else {
        if (ext === '.html') {
          data = new String(data)
            .replace('</body>', '<script>' + livereloadScript + '</script></body>')
        }
        res.setHeader('Content-type', mimes[ext] || 'text/plain' )
        res.end(data)
      }
    })
  }).listen(port)
  console.log(`http://localhost:${port}`)
}

module.exports = {
  serve,
  clients
}

if (require.main === module) serve()

