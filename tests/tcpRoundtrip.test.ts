import { expect } from 'chai'
import { type AddressInfo } from 'node:net'
import {
  CodecRegistry,
  LocalSocket,
  SocketRpcServer,
  TypeDescription,
  TypeKind,
  TypeRegistry,
  registerDynamicCodecs,
  registerMessageCodecs,
  serializeTypeRegistry
} from '@daemonarium/rpc'
import {
  CallbackEndpoint,
  PROTOCOL_VERSION_V1,
  SessionClient,
  SessionServer
} from '@daemonarium/gateway'
import { connectTcp, createTcpServer } from '../src/index.js'

const FLAT = { format: 'flat' as const }
const SOCKET_OPTS = {
  inputBufferSize: 65536,
  outputBufferSize: 65536,
  flushThreshold: 65536,
  dataEventThreshold: 0,
  drainEventThreshold: 65536,
  flags: 0
}

function drainCompleteFrames(buffer: {
  readableBytes: () => number
  peek: (offset: number, dest: Uint8Array, bytes: number) => number
  read: (dest: Uint8Array, bytes: number) => number
}): Uint8Array[] {
  const frames: Uint8Array[] = []
  const hdr = new Uint8Array(4)
  while (buffer.readableBytes() >= 4) {
    buffer.peek(0, hdr, 4)
    const len = new DataView(hdr.buffer, hdr.byteOffset, 4).getUint32(0, true)
    if (buffer.readableBytes() < 4 + len) break
    const frame = new Uint8Array(4 + len)
    buffer.read(frame, frame.length)
    frames.push(frame)
  }
  return frames
}

function echoRegistry(): TypeRegistry {
  const registry = new TypeRegistry()
  const uint64 = registry.getTypeByName('uint64')
  const stringType = registry.getTypeByName('string')
  if (!uint64 || !stringType) throw new Error('missing builtins')
  const runtimeError = TypeDescription.create({
    name: 'runtime_error',
    kind: TypeKind.Exception,
    members: [{ name: 'what', type: stringType, annotations: [] }]
  })
  const echoFn = TypeDescription.create({
    name: 'echo_api.echo',
    kind: TypeKind.Function,
    members: [{ name: 'x', type: uint64, annotations: [] }],
    item: uint64
  })
  echoFn.implementation = (x: bigint | number) => (typeof x === 'bigint' ? x : BigInt(x))
  const echoApi = TypeDescription.create({
    name: 'echo_api',
    kind: TypeKind.Object,
    members: [{ name: 'echo', type: echoFn, annotations: [] }]
  })
  registry.registerType(runtimeError)
  registry.registerType(echoFn)
  registry.registerType(echoApi)
  registry.assignMethodIds()
  return registry
}

function attachEcho(ep: CallbackEndpoint, registry: TypeRegistry): void {
  const innerSock = new LocalSocket(2, SOCKET_OPTS)
  const codecs = new CodecRegistry()
  registerDynamicCodecs(codecs, registry)
  registerMessageCodecs(codecs, registry)
  const innerServer = new SocketRpcServer(innerSock, registry, codecs, FLAT)
  innerSock.setOptions({
    flushCallback: () => {
      for (const frame of drainCompleteFrames(innerSock.outputBuffer)) {
        ep.emit(frame)
      }
    }
  })
  ep.setSink(async (payload) => {
    innerSock.inputBuffer.write(payload, payload.length)
    await innerServer.channel().handleData()
  })
}

describe('gateway tcp', () => {
  it('echo round-trip over TCP', async () => {
    const registry = echoRegistry()
    const blob = serializeTypeRegistry(registry)
    const ep = new CallbackEndpoint()
    attachEcho(ep, registry)
    let markReady: () => void = () => {}
    const ready = new Promise<void>((resolve) => { markReady = resolve })
    const server = createTcpServer((sock) => {
      new SessionServer(sock, [
        { api_id: 1, name: 'alpha', description_blob: blob, endpoint: ep }
      ])
      markReady()
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()))
    const port = (server.address() as AddressInfo).port
    try {
      const clientSock = await connectTcp('127.0.0.1', port)
      await ready
      const client = new SessionClient(clientSock)
      const hello = await client.hello() as { protocol_version: bigint | number }
      expect(Number(hello.protocol_version)).to.equal(Number(PROTOCOL_VERSION_V1))
      const infos = await client.listApis()
      expect(infos).to.have.length(1)
      expect(Number(infos[0].api_id)).to.equal(1)
      const conn = await client.openApi(1)
      expect(Number(await conn.rpc.call(0, 7n))).to.equal(7)
    } finally {
      server.close()
    }
  })
})
