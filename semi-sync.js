'use strict'

const fs = require('fs')
const EventEmitter = require('events')
const inherits = require('util').inherits

const BUF_SIZE = 64 * 1024

function openFile (file, sonic) {
  sonic.file = file
  sonic.fd = fs.openSync(file, 'a')

  if (sonic._reopening) {
    return
  }

  // start
  const pos = sonic._pos
  if (pos > 0 && !sonic.destroyed) {
    actualWrite(sonic)
  }

  process.nextTick(() => sonic.emit('ready'))
}

function SonicBoom (opts) {
  if (!(this instanceof SonicBoom)) {
    return new SonicBoom(opts)
  }

  let { fd, dest } = opts || {}

  fd = fd || dest

  this._buf = Buffer.allocUnsafeSlow(BUF_SIZE)
  this._pos = 0
  this.fd = -1
  this._drainScheduled = false
  this._ending = false
  this._reopening = false
  this.file = null
  this.destroyed = false

  if (typeof fd === 'number') {
    this.fd = fd
    process.nextTick(() => this.emit('ready'))
  } else if (typeof fd === 'string') {
    openFile(fd, this)
  } else {
    throw new Error('SonicBoom supports only file descriptors and files')
  }

  this.release = (err) => {
    if (err.code === 'EAGAIN') {
      // TODO handle properly
      throw new Error('Got EAGAIN')
    } else {
      this.emit('error', err)
    }
  }
}

inherits(SonicBoom, EventEmitter)

SonicBoom.prototype.write = function (data) {
  if (this.destroyed) {
    throw new Error('SonicBoom destroyed')
  }

  const pos = this._pos
  const written = this._buf.write(data, pos)
  // detect overflow
  if (pos + written === this._buf.length) {
    const dataLen = Buffer.byteLength(data)
    if (dataLen === written) {
      this._pos += written
      actualWrite(this)
      scheduleDrain(this)
      return true
    }

    // we need to write data into flushed buffer
    if (dataLen > this._buf.length) {
      // TODO handle properly
      throw new Error('Support large strings')
    } else {
      const written = this._buf.write(data, this._pos)
      this._pos += written
    }
  } else {
    this._pos += written
  }

  scheduleDrain(this)
  return true
}

function scheduleDrain (sonic) {
  if (!sonic._drainScheduled) {
    sonic._drainScheduled = true
    process.nextTick(() => {
      sonic._drainScheduled = false
      if (sonic._pos > 0 && !sonic.destroyed) {
        actualWrite(sonic)
      }
      sonic.emit('drain')
    })
  }
}

SonicBoom.prototype.flush = function () {
  if (this.destroyed) {
    throw new Error('SonicBoom destroyed')
  }

  actualWrite(this)
}

SonicBoom.prototype.reopen = function (file) {
  if (this.destroyed) {
    throw new Error('SonicBoom destroyed')
  }

  if (this._ending) {
    return
  }

  if (!this.file) {
    throw new Error('Unable to reopen a file descriptor, you must pass a file to SonicBoom')
  }

  this._reopening = true

  if (this._writing) {
    return
  }

  fs.close(this.fd, (err) => {
    if (err) {
      return this.emit('error', err)
    }
  })

  openFile(file || this.file, this)
}

SonicBoom.prototype.end = function () {
  if (this.destroyed) {
    throw new Error('SonicBoom destroyed')
  }

  if (this._ending) {
    return
  }

  this._ending = true

  if (this._pos > 0 && this.fd >= 0) {
    actualWrite(this)
    return
  }

  actualClose(this)
}

SonicBoom.prototype.flushSync = function () {
  if (this.destroyed) {
    throw new Error('SonicBoom destroyed')
  }

  if (this.fd < 0) {
    throw new Error('SonicBoom is not ready yet')
  }

  const pos = this._pos
  if (pos > 0) {
    actualWrite(this)
  }
}

SonicBoom.prototype.destroy = function () {
  if (this.destroyed) {
    return
  }
  actualClose(this)
}

function actualWrite (sonic) {
  const buf = sonic._buf
  const pos = sonic._pos
  const release = sonic.release
  try {
    sonic._pos = 0
    fs.writeSync(sonic.fd, buf.slice(0, pos))
  } catch (err) {
    release(err)
  }
}

function actualClose (sonic) {
  if (sonic.fd === -1) {
    sonic.once('ready', actualClose.bind(null, sonic))
    return
  }
  // TODO write a test to check if we are not leaking fds
  fs.close(sonic.fd, (err) => {
    if (err) {
      sonic.emit('error', err)
      return
    }

    if (sonic._ending) {
      sonic.emit('finish')
    }
    sonic.emit('close')
  })
  sonic.destroyed = true
  sonic._buf = Buffer.allocUnsafe(0)
}

module.exports = SonicBoom
