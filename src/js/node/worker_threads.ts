// import type { Readable, Writable } from "node:stream";
// import type { WorkerOptions } from "node:worker_threads";
declare const self: typeof globalThis;
type WebWorker = InstanceType<typeof globalThis.Worker>;

const EventEmitter = require("node:events");
const Readable = require("internal/streams/readable");
const Writable = require("internal/streams/writable");
const { throwNotImplemented, warnNotImplementedOnce } = require("internal/shared");
const {
  validateString,
  validateObject,
  validateInteger,
  validateNumber,
  validateBoolean,
} = require("internal/validators");

// Mirror node's lib/internal/worker.js name handling: default "WorkerThread",
// validate + trim when a name is provided.
// https://github.com/nodejs/node/blob/main/lib/internal/worker.js
function normalizeWorkerName(rawName) {
  // node gates on `!== undefined`, not truthiness: {name: 0|null|false} must
  // throw ERR_INVALID_ARG_TYPE (via validateString) and {name: ""} stays "".
  if (rawName !== undefined) {
    validateString(rawName, "options.name");
    return rawName.trim();
  }
  return "WorkerThread";
}

const { isAbsolute: pathIsAbsolute } = require("node:path");

// Mirror node's lib/internal/worker.js filename validation for non-eval
// Workers: accept absolute paths, "./"/"../"-relative paths, and file: URL
// objects; reject bare relative specifiers and string URLs.
// https://github.com/nodejs/node/blob/main/lib/internal/worker.js
function validateWorkerFilename(filename) {
  if (filename instanceof URL) {
    if (filename.protocol === "data:") return `${filename}`;
    // throws ERR_INVALID_URL_SCHEME (TypeError) for non-file: URLs
    return Bun.fileURLToPath(filename);
  }
  if (typeof filename !== "string") {
    // Not a string or URL: defer to the native Worker constructor, which
    // throws the canonical ERR_INVALID_ARG_TYPE with the exact node message.
    return filename;
  }
  const sep = String.fromCharCode(92); // backslash, avoids builtin-bundler escape handling
  if (
    pathIsAbsolute(filename) ||
    filename.startsWith("./") ||
    filename.startsWith("../") ||
    filename.startsWith("." + sep) ||
    filename.startsWith(".." + sep)
  ) {
    return filename;
  }
  let message =
    "The worker script or module filename must be an absolute path or a relative path starting with './' or '../'.";
  if (filename.startsWith("file://")) {
    message += " Wrap file:// URLs with `new URL`.";
  }
  if (filename.startsWith("data:text/javascript")) {
    message += " Wrap data: URLs with `new URL`.";
  }
  message += ` Received "${filename}"`;
  const err = new TypeError(message);
  err.code = "ERR_WORKER_PATH";
  throw err;
}

const {
  MessageChannel,
  BroadcastChannel,
  Worker: WebWorker,
} = globalThis as typeof globalThis & {
  // The Worker constructor secretly takes an extra parameter to provide the node:worker_threads
  // instance. This is so that it can emit the `worker` event on the process with the
  // node:worker_threads instance instead of the Web Worker instance.
  Worker: new (...args: [...ConstructorParameters<typeof globalThis.Worker>, nodeWorker: Worker]) => WebWorker;
};
const SHARE_ENV = Symbol.for("nodejs.worker_threads.SHARE_ENV");

const isMainThread = Bun.isMainThread;
const {
  0: _workerData,
  1: _threadId,
  2: _receiveMessageOnPort,
  3: environmentData,
  4: _threadName,
  5: _isMessagePortActive,
} = $cpp("Worker.cpp", "createNodeWorkerThreadsBinding") as [
  unknown,
  number,
  (port: unknown) => unknown,
  Map<unknown, unknown>,
  string,
  (port: unknown) => boolean,
];

type NodeWorkerOptions = import("node:worker_threads").WorkerOptions;

// Used to ensure that Blobs created to hold the source code for `eval: true` Workers get cleaned up
// after their Worker exits
let urlRevokeRegistry: FinalizationRegistry<string> | undefined = undefined;

function injectFakeEmitter(Class) {
  function messageEventHandler(event: MessageEvent) {
    return event.data;
  }

  function errorEventHandler(event: ErrorEvent) {
    return event.error;
  }

  function customEventHandler(event) {
    return event.detail;
  }

  const wrappedListener = Symbol("wrappedListener");

  function wrapped(run, listener) {
    const callback = function (event) {
      return listener(run(event));
    };
    listener[wrappedListener] = callback;
    return callback;
  }

  function functionForEventType(event, listener) {
    switch (event) {
      case "error":
      case "messageerror": {
        return wrapped(errorEventHandler, listener);
      }

      case "message": {
        return wrapped(messageEventHandler, listener);
      }

      default: {
        return wrapped(customEventHandler, listener);
      }
    }
  }

  function EventClass(eventName) {
    if (eventName === "error" || eventName === "messageerror") {
      return ErrorEvent;
    }

    return MessageEvent;
  }

  function on(event, listener) {
    this.addEventListener(event, functionForEventType(event, listener));
    return this;
  }

  function off(event, listener) {
    if (listener) {
      this.removeEventListener(event, listener[wrappedListener] || listener);
    } else {
      this.removeEventListener(event);
    }
    return this;
  }

  function once(event, listener) {
    this.addEventListener(event, functionForEventType(event, listener), { once: true });
    return this;
  }

  function emit(event, ...args) {
    switch (event) {
      case "error":
      case "messageerror":
      case "message":
        this.dispatchEvent(new (EventClass(event))(event, ...args));
        break;
      default:
        // node: a non-standard event emitted on a port surfaces to
        // addEventListener listeners as a CustomEvent (detail = first arg) and
        // to .on() listeners as the raw argument.
        this.dispatchEvent(new CustomEvent(event, { detail: args[0] }));
        break;
    }
    return this;
  }

  // node exposes these via EventEmitter.prototype (inherited), not as own
  // properties of MessagePort.prototype. Insert an intermediate prototype so
  // Object.getOwnPropertyNames(MessagePort.prototype) matches node.
  const proto = Class.prototype;
  const inherited = Object.create(Object.getPrototypeOf(proto));
  Object.defineProperty(inherited, "on", { value: on, writable: true, enumerable: false, configurable: true });
  Object.defineProperty(inherited, "off", { value: off, writable: true, enumerable: false, configurable: true });
  Object.defineProperty(inherited, "once", { value: once, writable: true, enumerable: false, configurable: true });
  Object.defineProperty(inherited, "emit", { value: emit, writable: true, enumerable: false, configurable: true });
  Object.defineProperty(inherited, "prependListener", {
    value: on,
    writable: true,
    enumerable: false,
    configurable: true,
  });
  Object.defineProperty(inherited, "prependOnceListener", {
    value: once,
    writable: true,
    enumerable: false,
    configurable: true,
  });
  Object.defineProperty(inherited, "addListener", {
    value: on,
    writable: true,
    enumerable: false,
    configurable: true,
  });
  Object.defineProperty(inherited, "removeListener", {
    value: off,
    writable: true,
    enumerable: false,
    configurable: true,
  });
  Object.setPrototypeOf(proto, inherited);
}

const _MessagePort = globalThis.MessagePort;
injectFakeEmitter(_MessagePort);

const MessagePort = _MessagePort;

// node: MessagePort.prototype.close(cb) registers cb as a one-time "close"
// listener, then performs the native close.
// https://github.com/nodejs/node/blob/main/lib/internal/worker/io.js
// Tracks ports closed via JS close() so moveMessagePortToContext can report
// ERR_CLOSED_MESSAGE_PORT for them, matching node.
const closedMessagePorts = new WeakSet();
const nativeMessagePortClose = MessagePort.prototype.close;
Object.defineProperty(MessagePort.prototype, "close", {
  value: function close(cb) {
    closedMessagePorts.add(this);
    // Native close() now dispatches the "close" event (after delivering any
    // queued messages). node invokes the optional callback asynchronously.
    const result = nativeMessagePortClose.$call(this);
    if (typeof cb === "function") {
      queueMicrotask(cb);
    }
    return result;
  },
  writable: true,
  enumerable: true,
  configurable: true,
});

// node-style util.inspect output for MessagePort (shows whether the channel is
// still active). Symbol-keyed so it does not appear in getOwnPropertyNames.
const kInspectCustom = Symbol.for("nodejs.util.inspect.custom");
Object.defineProperty(MessagePort.prototype, kInspectCustom, {
  value: function (_depth, _options) {
    return `MessagePort [EventTarget] { active: ${_isMessagePortActive(this)}, refed: ${this.hasRef()} }`;
  },
  writable: true,
  enumerable: false,
  configurable: true,
});

let resourceLimits = {};

const BUN_WORKER_STDIO_KEY = "@@bunWorkerThreadsStdio";
const BUN_WORKER_MESSAGING_KEY = "@@bunWorkerThreadsMessaging";

// Readable fed by a control MessagePort (worker.stdout/stderr on the parent,
// process.stdin in the worker). The peer posts Buffers; null signals EOF.
function makePortReadable(port) {
  let attached = false;
  let ended = false;
  function onMessage(chunk) {
    if (chunk === null) {
      if (ended === false) {
        ended = true;
        stream.push(null);
      }
      // Drop the listener so the control port stops holding the event loop
      // open once the stream has ended.
      port.off("message", onMessage);
    } else {
      stream.push(Buffer.from(chunk));
    }
  }
  // Attach the message listener lazily on first read(). A transferred port
  // refs the event loop while it has a message listener, so attaching eagerly
  // would keep a worker created with { stdin: true } alive even when it never
  // reads stdin. Buffered messages flush once the listener is added.
  const stream = new Readable({
    read() {
      if (attached === false) {
        attached = true;
        port.on("message", onMessage);
      }
    },
  });
  // Lets the parent end worker.stdout/stderr when the worker exits abruptly.
  stream.endFromOwner = function () {
    if (ended === false) {
      ended = true;
      stream.push(null);
      // Drop the listener so the port stops holding the event loop open once
      // the owner (worker exit) has ended the stream.
      port.off("message", onMessage);
    }
  };
  return stream;
}

// Writable that forwards chunks over a control MessagePort (worker.stdin on the
// parent, process.stdout/stderr in the worker). final() posts null as EOF.
function makePortWritable(port) {
  return new Writable({
    write(chunk, encoding, cb) {
      port.postMessage(typeof chunk === "string" ? Buffer.from(chunk, encoding) : chunk);
      cb();
    },
    final(cb) {
      port.postMessage(null);
      cb();
    },
  });
}

function setupWorkerStdio(stdio) {
  if (stdio.stdout) {
    Object.defineProperty(process, "stdout", {
      value: makePortWritable(stdio.stdout),
      writable: true,
      configurable: true,
      enumerable: true,
    });
  }
  if (stdio.stderr) {
    Object.defineProperty(process, "stderr", {
      value: makePortWritable(stdio.stderr),
      writable: true,
      configurable: true,
      enumerable: true,
    });
  }
  if (stdio.stdin) {
    Object.defineProperty(process, "stdin", {
      value: makePortReadable(stdio.stdin),
      writable: true,
      configurable: true,
      enumerable: true,
    });
  }
  // node routes console.log through process.stdout/stderr; Bun's global console
  // writes the fd directly, so rebind it to the captured streams when present.
  if (stdio.stdout || stdio.stderr) {
    const { Console } = require("node:console");
    globalThis.console = new Console(process.stdout, process.stderr);
  }
}

let workerData = _workerData;
let threadId = _threadId;
// node: main thread name is "", worker default is "WorkerThread" (trimmed).
const threadName = isMainThread ? "" : normalizeWorkerName(_threadName || undefined);
// postMessageToThread (Node 22+): the Worker ctor always smuggles a control
// MessagePort to the worker by wrapping workerData; unwrap it here.
const messaging = require("internal/worker/messaging");
messaging.initThreadInfo(threadId, isMainThread);
// Captured stdio + the messaging control port ride inside workerData (wrapped;
// ports transferred). Unwrap and bind the worker's stdio / messaging hub.
if (
  workerData &&
  typeof workerData === "object" &&
  (BUN_WORKER_STDIO_KEY in workerData || BUN_WORKER_MESSAGING_KEY in workerData)
) {
  const stdioPorts = workerData[BUN_WORKER_STDIO_KEY];
  const controlPort = workerData[BUN_WORKER_MESSAGING_KEY];
  workerData = workerData.data;
  if (stdioPorts) setupWorkerStdio(stdioPorts);
  if (controlPort) messaging.setupMainThreadPort(controlPort);
}
function receiveMessageOnPort(port: MessagePort) {
  let res = _receiveMessageOnPort(port);
  if (!res) return undefined;
  return {
    message: res,
  };
}

// TODO: parent port emulation is not complete
function fakeParentPort() {
  const fake = Object.create(MessagePort.prototype);
  Object.defineProperty(fake, "onmessage", {
    get() {
      return self.onmessage;
    },
    set(value) {
      self.onmessage = value;
    },
  });

  Object.defineProperty(fake, "onmessageerror", {
    get() {
      return self.onmessageerror;
    },
    set(value) {
      self.onmessageerror = value;
    },
  });

  const postMessage = $newCppFunction("ZigGlobalObject.cpp", "jsFunctionPostMessage", 1);
  Object.defineProperty(fake, "postMessage", {
    value(...args: [any, any]) {
      return postMessage.$apply(null, args);
    },
  });

  Object.defineProperty(fake, "close", {
    value() {},
  });

  Object.defineProperty(fake, "start", {
    value() {},
  });

  Object.defineProperty(fake, "unref", {
    value() {},
  });

  Object.defineProperty(fake, "ref", {
    value() {},
  });

  Object.defineProperty(fake, "hasRef", {
    value() {
      return false;
    },
  });

  Object.defineProperty(fake, "setEncoding", {
    value() {},
  });

  Object.defineProperty(fake, "addEventListener", {
    value: self.addEventListener.bind(self),
  });

  Object.defineProperty(fake, "removeEventListener", {
    value: self.removeEventListener.bind(self),
  });

  Object.defineProperty(fake, "removeListener", {
    value: self.removeEventListener.bind(self),
    enumerable: false,
  });

  Object.defineProperty(fake, "addListener", {
    value: self.addEventListener.bind(self),
    enumerable: false,
  });

  return fake;
}
let parentPort: MessagePort | null = isMainThread ? null : fakeParentPort();

function getEnvironmentData(key: unknown): unknown {
  return environmentData.get(key);
}

function setEnvironmentData(key: unknown, value: unknown): void {
  if (value === undefined) {
    environmentData.delete(key);
  } else {
    environmentData.set(key, value);
  }
}

const kUntransferable = Symbol.for("nodejs.worker_threads.untransferable");
const kUncloneable = Symbol.for("nodejs.worker_threads.uncloneable");

function markAsUntransferable(obj) {
  if ((typeof obj !== "object" && typeof obj !== "function") || obj === null) return;
  Object.defineProperty(obj, kUntransferable, { value: true, enumerable: false, configurable: true, writable: true });
}

function isMarkedAsUntransferable(obj) {
  if (obj == null) return false;
  return Object.hasOwn(obj, kUntransferable);
}

function markAsUncloneable(obj) {
  if ((typeof obj !== "object" && typeof obj !== "function") || obj === null) return;
  Object.defineProperty(obj, kUncloneable, { value: true, enumerable: false, configurable: true, writable: true });
}

function moveMessagePortToContext(port, _context) {
  if (port instanceof MessagePort) {
    if (closedMessagePorts.has(port)) {
      throw $ERR_CLOSED_MESSAGE_PORT("Cannot send data on closed MessagePort");
    }
  } else {
    throw $ERR_INVALID_ARG_TYPE("port", "MessagePort", port);
  }
  throwNotImplemented("worker_threads.moveMessagePortToContext");
}

class Worker extends EventEmitter {
  #worker: WebWorker;
  #performance;
  #name: string;
  #exited = false;
  #stdinPort;
  #stdoutPort;
  #stderrPort;
  #stdin;
  #stdout;
  #stderr;
  #stdoutAutoPipe = false;
  #stderrAutoPipe = false;

  // this is used by terminate();
  // either is the exit code if exited, a promise resolving to the exit code, or undefined if we haven't sent .terminate() yet
  #onExitPromise: Promise<number> | number | undefined = undefined;
  #urlToRevoke = "";
  // threadId captured for cleaning up the messaging control port on close.
  #messagingThreadId: number | undefined = undefined;

  constructor(filename: string, options: NodeWorkerOptions = {}) {
    super();

    // The `= {}` default only covers undefined; normalize null too so the
    // option accesses below don't throw on `new Worker(file, null)`.
    options ??= {};

    this.#name = normalizeWorkerName(options.name);

    const builtinsGeneratorHatesEval = "ev" + "a" + "l"[0];
    if (options[builtinsGeneratorHatesEval]) {
      // TODO: consider doing this step in native code and letting the Blob be cleaned up by the
      // C++ Worker object's destructor
      const blob = new Blob([filename], { type: "" });
      this.#urlToRevoke = filename = URL.createObjectURL(blob);
    } else {
      // node validates the worker path when not running eval'd code (eval:false
      // is equivalent to omitting eval).
      filename = validateWorkerFilename(filename);
    }

    // node-style captured stdio: a control MessageChannel per requested stream.
    // Keep the parent end here; hand the worker end to the worker via workerData
    // (transferred). The worker unwraps it and rebinds its process stdio.
    const stdioForWorker: any = {};
    const stdioTransfer: any[] = [];
    if (options.stdin) {
      const channel = new MessageChannel();
      this.#stdinPort = channel.port1;
      stdioForWorker.stdin = channel.port2;
      stdioTransfer.push(channel.port2);
    }
    // node always makes worker.stdout/stderr Readables fed by the worker's
    // process.stdout/stderr. When the user did not request capture, the parent
    // auto-pipes them to its own stdout/stderr so output still surfaces.
    {
      const channel = new MessageChannel();
      this.#stdoutPort = channel.port1;
      stdioForWorker.stdout = channel.port2;
      stdioTransfer.push(channel.port2);
      if (!options.stdout) this.#stdoutAutoPipe = true;
    }
    {
      const channel = new MessageChannel();
      this.#stderrPort = channel.port1;
      stdioForWorker.stderr = channel.port2;
      stdioTransfer.push(channel.port2);
      if (!options.stderr) this.#stderrAutoPipe = true;
    }
    // Always create a control channel so postMessageToThread can reach this worker.
    // Wrap the user's workerData so the control port (and any stdio ports) ride
    // along transferred; the worker unwraps it on load.
    const { portToMain, portToWorker } = messaging.createMessagingChannel();
    const workerDataWrapper: any = { [BUN_WORKER_MESSAGING_KEY]: portToWorker, data: options.workerData };
    if (stdioTransfer.length > 0) {
      workerDataWrapper[BUN_WORKER_STDIO_KEY] = stdioForWorker;
    }
    options = {
      ...options,
      workerData: workerDataWrapper,
      transferList: options.transferList
        ? [...options.transferList, portToWorker, ...stdioTransfer]
        : [portToWorker, ...stdioTransfer],
    };

    // `env: SHARE_ENV` requests that the worker share a live environment with
    // the parent. Convert it to a native-visible boolean flag so it doesn't hit
    // the object-validation throw in the native Worker constructor, and so the
    // native side skips the env snapshot and wires up the shared store.
    if (options && (options as any).env === SHARE_ENV) {
      options = { ...options, env: undefined, shareEnv: true } as NodeWorkerOptions;
    } else if (options && (options as any).shareEnv !== undefined) {
      // shareEnv is internal — only `env: SHARE_ENV` may enable it. Strip a
      // user-supplied value so it can't trigger env sharing on its own.
      options = { ...options, shareEnv: undefined } as NodeWorkerOptions;
    }
    try {
      this.#worker = new WebWorker(filename, options as Bun.WorkerOptions, this);
      // With uncaptured stdio, forward the worker's output to the parent's
      // stdout/stderr. end:false so the worker exiting (which ends worker.stdout)
      // does not close the parent's stream.
      // Auto-piped (uncaptured) stdio must not independently keep the parent
      // alive: the worker's own ref does that, and worker.unref() must let the
      // parent exit. Forward, but unref these ports so they don't pin the loop.
      if (this.#stdoutAutoPipe) {
        this.stdout.pipe(process.stdout, { end: false });
        this.#stdoutPort.unref();
      }
      if (this.#stderrAutoPipe) {
        this.stderr.pipe(process.stderr, { end: false });
        this.#stderrPort.unref();
      }
    } catch (e) {
      if (this.#urlToRevoke) {
        URL.revokeObjectURL(this.#urlToRevoke);
      }
      throw e;
    }
    // threadId is only assigned once the WebWorker exists; register the hub-side
    // control port with the messaging hub now.
    this.#messagingThreadId = this.#worker.threadId;
    messaging.registerMainThreadPort(this.#messagingThreadId, portToMain);
    this.#worker.addEventListener("close", this.#onClose.bind(this), { once: true });
    this.#worker.addEventListener("error", this.#onError.bind(this));
    this.#worker.addEventListener("message", this.#onMessage.bind(this));
    this.#worker.addEventListener("messageerror", this.#onMessageError.bind(this));
    this.#worker.addEventListener("open", this.#onOpen.bind(this), { once: true });

    if (this.#urlToRevoke) {
      if (!urlRevokeRegistry) {
        urlRevokeRegistry = new FinalizationRegistry<string>(url => {
          URL.revokeObjectURL(url);
        });
      }
      urlRevokeRegistry.register(this.#worker, this.#urlToRevoke);
    }
  }

  get threadId() {
    return this.#worker.threadId;
  }

  get threadName() {
    return this.#exited ? null : this.#name;
  }

  ref() {
    this.#worker.ref();
  }

  unref() {
    this.#worker.unref();
  }

  get stdin() {
    if (this.#stdinPort === undefined) return null;
    return (this.#stdin ??= makePortWritable(this.#stdinPort));
  }

  get stdout() {
    if (this.#stdoutPort === undefined) return null;
    return (this.#stdout ??= makePortReadable(this.#stdoutPort));
  }

  get stderr() {
    if (this.#stderrPort === undefined) return null;
    return (this.#stderr ??= makePortReadable(this.#stderrPort));
  }

  get performance() {
    return (this.#performance ??= {
      eventLoopUtilization() {
        warnNotImplementedOnce("worker_threads.Worker.performance");
        return {
          idle: 0,
          active: 0,
          utilization: 0,
        };
      },
    });
  }

  terminate(callback: unknown) {
    if (typeof callback === "function") {
      process.emitWarning(
        "Passing a callback to worker.terminate() is deprecated. It returns a Promise instead.",
        "DeprecationWarning",
        "DEP0132",
      );
      this.#worker.addEventListener("close", event => callback(null, event.code), { once: true });
    }

    const onExitPromise = this.#onExitPromise;
    // Use `!== undefined`, not a truthy test: once the worker has exited
    // #onExitPromise holds its exit code, which is 0 (falsy) for a clean exit.
    // A truthy check would fall through and attach a 'close' listener to an
    // already-closed worker, so the returned promise would never settle.
    if (onExitPromise !== undefined) {
      // node's terminate() resolves with undefined regardless of exit code.
      return $isPromise(onExitPromise) ? onExitPromise : Promise.$resolve(undefined);
    }

    const { resolve, promise } = Promise.withResolvers();
    this.#worker.addEventListener(
      "close",
      () => {
        resolve(undefined);
      },
      { once: true },
    );
    // Keep the event loop alive until termination completes so the returned
    // promise still resolves even if the worker was unref()'ed.
    this.#worker.ref();
    this.#worker.terminate();

    return (this.#onExitPromise = promise);
  }

  postMessage(...args: [any, any]) {
    return this.#worker.postMessage.$apply(this.#worker, args);
  }

  getHeapSnapshot(options: unknown) {
    const stringPromise = this.#worker.getHeapSnapshot(options);
    return stringPromise.then(s => new HeapSnapshotStream(s));
  }

  getHeapStatistics() {
    return this.#worker.getHeapStatistics();
  }

  startCpuProfile(options?: { sampleInterval?: number; maxBufferSize?: number }) {
    // node validates synchronously before starting; the underlying JSC sampler
    // ignores these knobs but the range checks must still match.
    if (options !== undefined && options !== null) {
      validateObject(options, "options");
      if (options.maxBufferSize !== undefined) validateInteger(options.maxBufferSize, "options.maxBufferSize", 1);
      if (options.sampleInterval !== undefined) validateNumber(options.sampleInterval, "options.sampleInterval");
    }
    return this.#worker.startCpuProfileInternal().then(() => ({
      stop: () => this.#worker.stopCpuProfileInternal(),
    }));
  }

  cpuUsage(prevValue?: { user: number; system: number }) {
    if (prevValue) {
      validateObject(prevValue, "prevValue");
      validateNumber(prevValue.user, "prevValue.user");
      if (prevValue.user < 0 || !Number.isFinite(prevValue.user))
        throw $ERR_OUT_OF_RANGE("prevValue.user", ">= 0 and a finite number", prevValue.user);
      validateNumber(prevValue.system, "prevValue.system");
      if (prevValue.system < 0 || !Number.isFinite(prevValue.system))
        throw $ERR_OUT_OF_RANGE("prevValue.system", ">= 0 and a finite number", prevValue.system);
    }
    return this.#worker
      .cpuUsageInternal()
      .then((abs: { user: number; system: number }) =>
        prevValue ? { user: abs.user - prevValue.user, system: abs.system - prevValue.system } : abs,
      );
  }

  startHeapProfile(options?: object) {
    if (options !== undefined && options !== null) {
      validateObject(options, "options");
      const o = options as any;
      if (o.sampleInterval !== undefined) validateInteger(o.sampleInterval, "options.sampleInterval", 1);
      if (o.stackDepth !== undefined) validateInteger(o.stackDepth, "options.stackDepth", 0);
      if (o.forceGC !== undefined) validateBoolean(o.forceGC, "options.forceGC");
      if (o.includeObjectsCollectedByMajorGC !== undefined)
        validateBoolean(o.includeObjectsCollectedByMajorGC, "options.includeObjectsCollectedByMajorGC");
      if (o.includeObjectsCollectedByMinorGC !== undefined)
        validateBoolean(o.includeObjectsCollectedByMinorGC, "options.includeObjectsCollectedByMinorGC");
    }
    if (this.#exited) {
      return Promise.$reject($ERR_WORKER_NOT_RUNNING("Worker instance not running"));
    }
    // Bun has no allocation-sampling heap profiler; yield a valid but empty
    // v8 sampling-heap-profile so the handle/stop() shape matches node.
    const empty =
      '{"head":{"callFrame":{"functionName":"(root)","scriptId":"0","url":"","lineNumber":-1,"columnNumber":-1},"selfSize":0,"id":1,"children":[]},"samples":[]}';
    return Promise.$resolve({ stop: () => Promise.$resolve(empty) });
  }

  #onClose(e) {
    this.#exited = true;
    if (this.#messagingThreadId !== undefined) {
      messaging.destroyMainThreadPort(this.#messagingThreadId);
      this.#messagingThreadId = undefined;
    }
    // End captured stdio readables when the worker exits, even if it was
    // terminated before its own streams finished.
    if (this.#stdout) {
      this.#stdout.endFromOwner();
    }
    if (this.#stderr) {
      this.#stderr.endFromOwner();
    }
    this.#onExitPromise = e.code;
    this.emit("exit", e.code);
  }

  #onError(event: ErrorEvent) {
    let error = event?.error;
    // if the thrown value serialized successfully, the message will be empty
    // if not the message is the actual error
    if (event.message !== "") {
      error = new Error(event.message, { cause: event });
      const stack = event?.stack;
      if (stack) {
        error.stack = stack;
      }
    }
    this.emit("error", error);
  }

  #onMessage(event: MessageEvent) {
    // TODO: is this right?
    this.emit("message", event.data);
  }

  #onMessageError(event: MessageEvent) {
    // TODO: is this right?
    this.emit("messageerror", (event as any).error ?? event.data ?? event);
  }

  #onOpen() {
    this.emit("online");
  }

  async [Symbol.asyncDispose]() {
    await this.terminate();
  }
}

class HeapSnapshotStream extends Readable {
  #json: string | undefined;

  constructor(json: string) {
    super();
    this.#json = json;
  }

  _read() {
    if (this.#json !== undefined) {
      this.push(this.#json);
      this.push(null);
      this.#json = undefined;
    }
  }
}

export default {
  Worker,
  workerData,
  parentPort,
  resourceLimits,
  isMainThread,
  MessageChannel,
  BroadcastChannel,
  MessagePort,
  getEnvironmentData,
  setEnvironmentData,
  getHeapSnapshot() {
    return {};
  },
  markAsUntransferable,
  markAsUncloneable,
  isMarkedAsUntransferable,
  moveMessagePortToContext,
  postMessageToThread: messaging.postMessageToThread,
  receiveMessageOnPort,
  SHARE_ENV,
  threadId,
  threadName,
  // node:inspector's minimal NodeWorker domain reports this worker's title
  // ("[worker N] <name>") via NodeWorker.attachedToWorker. Exposed through a
  // well-known symbol so inspector.ts can read it without a public export.
  [Symbol.for("nodejs.worker_threads.inspectorTitle")]: isMainThread ? undefined : `[worker ${threadId}] ${threadName}`,
};
