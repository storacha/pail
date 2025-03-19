import { defineConfig } from 'vitest/config'

export default defineConfig({
  plugins: [],
  test: {
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html']
    },
    exclude: ['test/helpers.js'],
    include: ['test/*.js'],
    globals: true
  },
})
