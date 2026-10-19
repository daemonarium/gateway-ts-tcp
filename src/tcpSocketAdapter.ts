import net from 'node:net'
import {
  LocalSocket,
  type SocketMessageChannelSocket,
  type SocketOptions
} from '@daemonarium/rpc'

const DEFAULTS: SocketOptions = {
  inputBufferSize: 65536,
  outputBufferSize: 65536,
  flushThreshold: 4096,
  dataEventThreshold: 0,
  drainEventThreshold: 4,
  flags: 0
}

/**
 * Pumps session bytes between a LocalSocket and a Node TCP socket.
 * Node only. Construct the session on `socket` before the peer sends.
 */
export class TcpSocketAdapter {
  readonly socket: LocalSocket
  private closed = false
  private paused = false
  private pending: Uint8Array[] = []
  private writable = true

  constructor(private readonly netSocket: net.Socket, options: Partial<SocketOptions> = {}) {
    this.socket = new LocalSocket(0, { ...DEFAULTS, ...options })
    const prev = this.socket.getOptions().flushCallback
    this.socket.setOptions({
      flushCallback: (id) => {
        if (prev) prev(id)
        this.pullToNetwork()
      }
    })
    this.netSocket.on('data', (chunk: Buffer) => {
      this.pushFromNetwork(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength))
    })
    this.netSocket.on('drain', () => {
      this.writable = true
      this.pullToNetwork()
    })
    this.netSocket.on('close', () => this.close())
    this.netSocket.on('error', () => this.close())
  }

  pushFromNetwork(bytes: Uint8Array): void {
    if (this.closed || bytes.length === 0) return
    this.pending.push(bytes.slice())
    this.flushPending()
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    this.pending = []
    try { this.netSocket.destroy() } catch { /* ignore */ }
  }

  private flushPending(): void {
    while (this.pending.length > 0 && !this.closed) {
      const space = this.socket.inputBuffer.writableBytes()
      if (space <= 0) {
        this.pause()
        return
      }
      const chunk = this.pending[0]
      const n = Math.min(space, chunk.length)
      const written = this.socket.inputBuffer.write(chunk.subarray(0, n), n)
      if (written <= 0) {
        this.pause()
        return
      }
      if (written >= chunk.length) this.pending.shift()
      else this.pending[0] = chunk.subarray(written)
    }
    // Loopback delivery is synchronous. Defer so rpc.call can register
    // its waiter before a response is parsed.
    queueMicrotask(() => {
      if (!this.closed) this.socket.handleData()
    })
    if (this.pending.length > 0) queueMicrotask(() => this.flushPending())
    else this.maybeResume()
  }

  private pullToNetwork(): void {
    if (this.closed || !this.writable) return
    const output = this.socket.outputBuffer
    while (output.readableBytes() > 0 && this.writable) {
      const n = output.readableBytes()
      const buf = Buffer.allocUnsafe(n)
      const read = output.read(buf, n)
      if (read <= 0) break
      const payload = read === n ? buf : buf.subarray(0, read)
      const ok = this.netSocket.write(payload)
      if (!ok) this.writable = false
    }
  }

  private pause(): void {
    if (this.paused) return
    this.paused = true
    this.netSocket.pause()
  }

  private maybeResume(): void {
    if (!this.paused) return
    this.paused = false
    this.netSocket.resume()
  }
}

export function wrapTcpSocket(
  socket: net.Socket,
  options?: Partial<SocketOptions>
): SocketMessageChannelSocket {
  return new TcpSocketAdapter(socket, options).socket
}

export function connectTcp(
  host: string,
  port: number,
  options?: Partial<SocketOptions>
): Promise<SocketMessageChannelSocket> {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host, port }, () => {
      resolve(new TcpSocketAdapter(socket, options).socket)
    })
    socket.once('error', reject)
  })
}

export function createTcpServer(
  handler: (socket: SocketMessageChannelSocket) => void,
  options?: Partial<SocketOptions>
): net.Server {
  return net.createServer((socket) => {
    handler(wrapTcpSocket(socket, options))
  })
}
