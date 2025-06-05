import { connect } from 'cloudflare:sockets';
import { EventEmitter } from 'events';

import type { SocketConnectOpts, SocketReadyState } from 'node:net';
import type { Duplex } from 'stream';

/**
 * This class is a polyfill for the `net.Socket` class in node.js but using the
 * `cloudflare:sockets`module instead.
 * Its main purpose is to enable use of npm modules that use `net.createConnection` to open up
 * a socket connection to a server. The most notable of which is the `mongodb` package.
 * It uses [`alias`](https://developers.cloudflare.com/workers/wrangler/configuration/#module-aliasing)
 * to replace the `net` module with this class.
 */
export class CloudflareSocket extends EventEmitter implements Pick<Duplex, 'pipe'> {
  writable = false;
  readonly autoSelectFamilyAttemptedAddresses: string[] = [];
  /**
   * This property shows the number of characters buffered for writing. The buffer
   * may contain strings whose length after encoding is not yet known. So this number
   * is only an approximation of the number of bytes in the buffer.
   *
   * `net.Socket` has the property that `socket.write()` always works. This is to
   * help users get up and running quickly. The computer cannot always keep up
   * with the amount of data that is written to a socket. The network connection
   * simply might be too slow. Node.js will internally queue up the data written to a
   * socket and send it out over the wire when it is possible.
   *
   * The consequence of this internal buffering is that memory may grow.
   * Users who experience large or growing `bufferSize` should attempt to
   * "throttle" the data flows in their program with `socket.pause()` and `socket.resume()`.
   * @since v0.3.8
   * @deprecated Since v14.6.0 - Use `writableLength` instead.
   */
  readonly bufferSize: number = 0;
  /**
   * The amount of received bytes.
   * @since v0.5.3
   */
  readonly bytesRead: number = 0;
  /**
   * The amount of bytes sent.
   * @since v0.5.3
   */
  readonly bytesWritten: number = 0;
  /**
   * If `true`, `socket.connect(options[, connectListener])` was
   * called and has not yet finished. It will stay `true` until the socket becomes
   * connected, then it is set to `false` and the `'connect'` event is emitted. Note
   * that the `socket.connect(options[, connectListener])` callback is a listener for the `'connect'` event.
   * @since v6.1.0
   */
  connecting: boolean = false;
  /**
   * This is `true` if the socket is not connected yet, either because `.connect()`has not yet been called or because it is still in the process of connecting
   * (see `socket.connecting`).
   * @since v11.2.0, v10.16.0
   */
  pending: boolean = true;
  /**
   * See `writable.destroyed` for further details.
   */
  destroyed: boolean = false;
  /**
   * The string representation of the local IP address the remote client is
   * connecting on. For example, in a server listening on `'0.0.0.0'`, if a client
   * connects on `'192.168.1.1'`, the value of `socket.localAddress` would be`'192.168.1.1'`.
   * @since v0.9.6
   */
  readonly localAddress?: string;
  /**
   * The numeric representation of the local port. For example, `80` or `21`.
   * @since v0.9.6
   */
  readonly localPort?: number;
  /**
   * The string representation of the local IP family. `'IPv4'` or `'IPv6'`.
   * @since v18.8.0, v16.18.0
   */
  readonly localFamily?: string;
  /**
   * This property represents the state of the connection as a string.
   *
   * * If the stream is connecting `socket.readyState` is `opening`.
   * * If the stream is readable and writable, it is `open`.
   * * If the stream is readable and not writable, it is `readOnly`.
   * * If the stream is not readable and writable, it is `writeOnly`.
   * @since v0.5.0
   */
  readonly readyState: SocketReadyState = 'open';
  /**
   * The string representation of the remote IP address. For example,`'74.125.127.100'` or `'2001:4860:a005::68'`. Value may be `undefined` if
   * the socket is destroyed (for example, if the client disconnected).
   * @since v0.5.10
   */
  readonly remoteAddress?: string | undefined;
  /**
   * The string representation of the remote IP family. `'IPv4'` or `'IPv6'`. Value may be `undefined` if
   * the socket is destroyed (for example, if the client disconnected).
   * @since v0.11.14
   */
  readonly remoteFamily?: string | undefined;
  /**
   * The numeric representation of the remote port. For example, `80` or `21`. Value may be `undefined` if
   * the socket is destroyed (for example, if the client disconnected).
   * @since v0.5.10
   */
  readonly remotePort?: number | undefined;
  /**
   * The socket timeout in milliseconds as set by `socket.setTimeout()`.
   * It is `undefined` if a timeout has not been set.
   * @since v10.7.0
   */
  readonly timeout?: number | undefined;

  private _upgrading = false;
  private _upgraded = false;
  private _cfSocket: Socket | null = null;
  private _cfWriter: WritableStreamDefaultWriter | null = null;
  private _cfReader: ReadableStreamDefaultReader | null = null;

  private _reading = false;
  private _paused = false;
  private _listening: boolean = false;

  constructor(readonly ssl: boolean, readonly quiet: boolean = true) {
    super();
  }

  private log(...args: unknown[]) {
    if (!this.quiet) log(...args);
  }

  sinks: Set<NodeJS.WritableStream> = new Set<NodeJS.WritableStream>();

  pipe<T extends NodeJS.WritableStream>(
    destination: T,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    options?: { end?: boolean | undefined } | undefined
  ): T {
    // calling this `thisSocket.pipe(<dest>)` will cause the destination to be added 
    // to the list of sinks. Data is written to this list of sinks when data comes into this socket.
    this.sinks.add(destination);
    return this as unknown as T;
  }

  setNoDelay() {
    // TODO: implement
    return this;
  }
  setKeepAlive() {
    // TODO: implement
    return this;
  }
  ref() {
    // TODO: implement
    return this;
  }
  unref() {
    // TODO: implement
    return this;
  }
  pause() {
    if (this._cfSocket && this._cfReader && !this.connecting && this._reading) {
      this._reading = false;
      this._paused = true;
      this.log("Socket paused");
    }
    return this;
  }
  resume() {
    if (this._cfSocket && this._cfReader && this._paused) {
      this._paused = false;
      this._reading = true; // Important to set this back to true
      if (!this._listening) { // Ensure _listen is only called once
        this._listen().catch((e) => this.emit('error', e));
      }
    }
    return this;
  }
  address() {
    // TODO: implement
    return {};
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  setTimeout(_timeout: number, _callback?: () => void) {
    // TODO: implement
    return this;
  }
  resetAndDestroy(): this {
    // TODO: implement
    return this;
  }

  destroySoon(): void {
    // TODO: implement
  }

  _connect(options: SocketConnectOpts, connectListener?: () => void): this {
    try {
      this.log('connecting');
      if (connectListener) this.once('connect', connectListener);

      const sOptions: Partial<SocketOptions> = this.ssl ? { secureTransport: 'on' } : {};

      const { host, port } = options as { host: string; port: number };

      this.pending = true;
      this.connecting = true;
      this._cfSocket = connect(`${host!}:${port!}`, {
        ...(sOptions as SocketOptions),
      });

      this._cfWriter = this._cfSocket.writable.getWriter();
      this._cfReader = this._cfSocket.readable.getReader();
      this._cfSocket.opened
        .then(() => {
          this.log('socket opened');
          this._addClosedHandler();

          this._cfWriter!.ready.then(() => {
            this.log(`${this._upgraded ? 'ssl' : ''} socket ready`);
            this._listen().catch((e) => this.emit('error', e));

            this.writable = true;
            this.pending = false;
            this.connecting = false;
            const eventName = this.ssl ? 'secureConnect' : 'connect';
            this.emit(eventName);
          });
        })
        .catch((err) => console.error('error connecting', err));

      return this;
    } catch (e) {
      this.emit('error', e);
    }
    return this;
  }

  connect(options: SocketConnectOpts, connectListener?: (() => void) | undefined): this;
  connect(port: number, host: string, connectListener?: (() => void) | undefined): this;
  connect(port: number, connectListener?: (() => void) | undefined): this;
  connect(path: string, connectListener?: (() => void) | undefined): this;
  connect(
    portOrOptions: number | string | SocketConnectOpts,
    hostOrConnectListener?: string | ((...args: unknown[]) => void),
    connectListener?: (...args: unknown[]) => void
  ): this {
    if (typeof portOrOptions === 'string') {
      throw new Error('connect options invalid');
    }

    const options: SocketConnectOpts =
      typeof portOrOptions === 'number'
        ? { port: portOrOptions, host: hostOrConnectListener as string }
        : portOrOptions;
    connectListener =
      typeof portOrOptions === 'number'
        ? connectListener
        : (hostOrConnectListener as (...args: unknown[]) => void);

    try {
      return this._connect(options, connectListener);
    } catch (e) {
      this.emit('error', e);
    }
    return this;
  }

  disableRenegotiation() {
    this.log('disableRenegotiation');
  }
    
  async _listen() {
    if (this._paused) return;
    if (this._listening) return;
    this._listening = true; 
    this._reading = true;
    let accumulatedData: Buffer | null = null;
    let documentSize: number | null = null;
    let isComplete: boolean = false;
    try {
      while (this._reading && !this._paused) {
        const readResult = await this._readChunk();
        if (readResult === null) break; 
        const { done, value } = readResult;
        if (value === undefined) {
            if (done) {
                break;
            }
            continue;
        } 
        const bufferedValue = Buffer.from(value!);
        if (accumulatedData === null) {
          accumulatedData = bufferedValue
          documentSize = accumulatedData.readInt32LE(0);
          isComplete = documentSize === accumulatedData.length;
        } else {
          accumulatedData = Buffer.concat([accumulatedData!, bufferedValue]);
          isComplete = documentSize === accumulatedData.length
        }

        if(   // sanity check 
            documentSize && (
            documentSize < 5 || 
            documentSize >  2000000000
          )
        ) { 
          this.destroy(new Error("Invalid document size"));
          break;
        }
        if(isComplete) {
          try {
            this.emit('data', accumulatedData);
            [...this.sinks.values()].forEach((sink) => {
                sink.write(accumulatedData!);
            });
          } catch (error) {
            this.emit('error', error);
          }
          documentSize = null;
          accumulatedData = null;
        }
        if(done) break;
      }
    } catch (e) {
      this.emit('error', e);
      this._reading = false;
    } finally {
      this._listening = false; 
    }
  }
  
  private async _readChunk(): Promise<{ done: boolean; value?: Uint8Array | null } | null> {
    console.log("Entering _readChunk()");
    try {
      const readResult = await this._cfReader!.read();
      return readResult;
    } catch (error) {
      console.log("Error in _readChunk():", error);
      return null;
    }
  }

  write(
    data: Uint8Array | string
    // encoding: BufferEncoding = 'utf8',
    // callback: (...args: unknown[]) => void = () => {}
  ) {
    this.log('write called', data);
    // if (data.length === 0) return callback();
    if (typeof data === 'string') data = Buffer.from(data);

    this.log('sending data direct:', data);

    this._cfWriter!.write(data).then(
      () => {
        this.log('data sent');
        this.emit('written');
      },
      (err) => {
        this.log('send error', err);
        this.emit('error');
      }
    );

    return true;
  }

  end(
    data?: Uint8Array | string,
    encoding?: BufferEncoding,
    callback?: (err?: Error) => void
  ): this {
    if (!data) {
      callback?.();
      return this;
    }
    this.log('ending CF socket');
    const errHandler = (err: Error) => {
      if (callback) {
        return callback(err);
      }
    };
    this.once('error', errHandler);
    this.once('written', () => {
      this._cfSocket!.close();
      this.removeListener('error', errHandler);
    });
    this.write(data);
    return this;
  }

  destroy(reason?: Error) {
    this.destroyed = true;
    return this.end();
  }

  _addClosedHandler() {
    this._cfSocket!.closed.then(() => {
      if (!this._upgrading) {
        this.log('CF socket closed');
        this._cfSocket = null;
        this.emit('close');
      } else {
        this.log('CF socket closed/upgraded');
        this._upgrading = false;
        this._upgraded = true;
      }
    }).catch((e) => this.emit('error', e));
  }
}


function dump(data: unknown) {
  if (data instanceof Uint8Array || data instanceof ArrayBuffer) {
    const hex = Buffer.from(data as unknown as Parameters<typeof Buffer.from>[0]).toString('hex');
    const str = new TextDecoder().decode(data);
    return `\n>>> STR: "${str.replace(/\n/g, '\\n')}"\n>>> HEX: ${hex}\n`;
  } else {
    return data;
  }
}

function log(...args: unknown[]) {
  // eslint-disable-next-line @typescript-eslint/no-unused-expressions
  console.log(...args.map(dump));
}