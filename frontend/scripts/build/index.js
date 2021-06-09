const { copy } = require('./copy')
const { clean } = require('../clean')
const { js } = require('./js')

function build() {
  clean()
  copy()
  js()
}

if (require.main === module) build()

module.exports = {
  build
}

