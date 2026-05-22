// import type { Readable, Writable } from "node:stream";
// import type { WorkerOptions } from "node:worker_threads";
declare const self: typeof globalThis;
type WebWorker = InstanceType<typeof globalThis.Worker>;

const EventEmitter = require("node:events");
const Readable = require("internal/streams/readable");
const { throwNotImplemented, warnNotImplementedOnce } = require("internal/shared");
const { validateString } = require("internal/validators");

// Mirror node's lib/internal/worker.js name handling: default "WorkerThread",
// validate + trim when a name is provided.
// https://github.com/nodejs/node/blob/main/lib/internal/worker.js
function normalizeWorkerName(rawName) {
  if (rawName) {
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
const SHARE_ENV = Symbol("nodejs.worker_threads.SHARE_ENV");

const isMainThread = Bun.isMainThread;
const {
  0: _workerData,
  1: _threadId,
  2: _receiveMessageOnPort,
  3: environmentData,
  4: _threadName,
} = $cpp("Worker.cpp", "createNodeWorkerThreadsBinding") as [
  unknown,
  number,
  (port: unknown) => unknown,
  Map<unknown, unknown>,
  string,
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

      default: {
        return wrapped(messageEventHandler, listener);
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
    this.dispatchEvent(new (EventClass(event))(event, ...args));
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
  Object.defineProperty(inherited, "prependListener", { value: on, writable: true, enumerable: false, configurable: true });
  Object.defineProperty(inherited, "prependOnceListener", { value: once, writable: true, enumerable: false, configurable: true });
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
    if (typeof cb === "function") {
      this.once("close", cb);
    }
    closedMessagePorts.add(this);
    // Bun's native close() does not emit a "close" event. Dispatch it here,
    // synchronously while the port is still attached, so registered listeners
    // and the optional callback run — matching node's MessagePort close.
    this.dispatchEvent(new Event("close"));
    return nativeMessagePortClose.$call(this);
  },
  writable: true,
  enumerable: true,
  configurable: true,
});

let resourceLimits = {};

let workerData = _workerData;
let threadId = _threadId;
// node: main thread name is "", worker default is "WorkerThread" (trimmed).
const threadName = isMainThread ? "" : normalizeWorkerName(_threadName);
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

function markAsUntransferable() {
  throwNotImplemented("worker_threads.markAsUntransferable");
}

function moveMessagePortToContext(port, context) {
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

  // this is used by terminate();
  // either is the exit code if exited, a promise resolving to the exit code, or undefined if we haven't sent .terminate() yet
  #onExitPromise: Promise<number> | number | undefined = undefined;
  #urlToRevoke = "";

  constructor(filename: string, options: NodeWorkerOptions = {}) {
    super();

    this.#name = normalizeWorkerName(options?.name);

    const builtinsGeneratorHatesEval = "ev" + "a" + "l"[0];
    if (options && builtinsGeneratorHatesEval in options) {
      if (options[builtinsGeneratorHatesEval]) {
        // TODO: consider doing this step in native code and letting the Blob be cleaned up by the
        // C++ Worker object's destructor
        const blob = new Blob([filename], { type: "" });
        this.#urlToRevoke = filename = URL.createObjectURL(blob);
      } else {
        // if options.eval = false, allow the constructor below to fail, if
        // we convert the code to a blob, it will succeed.
        this.#urlToRevoke = filename;
      }
    } else {
      // node validates the worker path when not running eval'd code.
      filename = validateWorkerFilename(filename);
    }
    try {
      this.#worker = new WebWorker(filename, options as Bun.WorkerOptions, this);
    } catch (e) {
      if (this.#urlToRevoke) {
        URL.revokeObjectURL(this.#urlToRevoke);
      }
      throw e;
    }
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
    // TODO:
    return null;
  }

  get stdout() {
    // TODO:
    return null;
  }

  get stderr() {
    // TODO:
    return null;
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
    if (onExitPromise) {
      return $isPromise(onExitPromise) ? onExitPromise : Promise.$resolve(onExitPromise);
    }

    const { resolve, promise } = Promise.withResolvers();
    this.#worker.addEventListener(
      "close",
      event => {
        resolve(event.code);
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

  #onClose(e) {
    this.#exited = true;
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
  moveMessagePortToContext,
  receiveMessageOnPort,
  SHARE_ENV,
  threadId,
  threadName,
};
