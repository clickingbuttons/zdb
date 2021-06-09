const { build } = require('./build')
const { copy } = require('./build/copy');
const { serve, clients } = require('./serve')
const { esbuildConfig } = require('./helpers')
const esbuild = require('esbuild')
const { watch } = require('chokidar')

function register(path, listener) {
  watch(path, { ignoreInitial: true }).on('all', listener)
}

register('static', copy) // TODO: copy only what changes
build()
console.log('watching for changes')
esbuild.build({
  ...esbuildConfig,
  watch: {
    onRebuild(error) {
      if (error) {
        console.log(error)
        return
      }
      clients.forEach(res => res.write('data: update\n\n'))
      clients.length = 0
    },
  },
})
serve()

