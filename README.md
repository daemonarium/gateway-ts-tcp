# gateway-tcp

TCP transport for [@daemonarium/gateway](https://github.com/daemonarium/gateway-ts). It wraps a Node `net.Socket` as a `SocketMessageChannelSocket`. There is no kernel. This package is Node only.

The npm package is `@daemonarium/gateway-tcp`. This repository is `daemonarium/gateway-ts-tcp`.

## Install

```
npm i @daemonarium/gateway @daemonarium/gateway-tcp
```

`@daemonarium/gateway` is a peer dependency. `node:net` is built in.

## API

```ts
import { SessionClient, SessionServer } from '@daemonarium/gateway'
import { connectTcp, createTcpServer } from '@daemonarium/gateway-tcp'

const server = createTcpServer((sock) => {
  new SessionServer(sock, publications)
})
server.listen(9000)

const clientSock = await connectTcp('127.0.0.1', 9000)
const client = new SessionClient(clientSock)
await client.hello()
```

`wrapTcpSocket` does the same for a socket you already accepted.

## License

MIT. Conduct is in `CODE_OF_CONDUCT.md`.
