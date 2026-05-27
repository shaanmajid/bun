// Implements worker_threads.postMessageToThread (Node 22+).
//
// Ported from Node.js lib/internal/worker/messaging.js. The main thread acts as a
// hub: every other thread keeps a control MessagePort to the main thread, and the
// main thread keeps a map of threadId -> port for all threads. A message destined
// for thread N is routed through the main thread to N's control port. The result of
// the delivery (delivered / no-listeners / listener-threw / timed-out) is reported
// back to the caller through a SharedArrayBuffer + Atomics, so the async caller can
// resolve/reject without an extra round-trip message.
//
// Differences from Node:
//   - Thread info comes from `initThreadInfo` (called from worker_threads.ts) instead
//     of an internalBinding, because Bun assigns the threadId differently.
//   - Node's `createMainThreadPort` is split into `createMessagingChannel` (called
//     before `new Worker`) and `registerMainThreadPort` (called after, once Bun has
//     assigned the child's threadId).
//   - Bun's `process.emit` returns true even with no listeners and routes a throwing
//     listener to uncaughtException instead of propagating it, so the `workerMessage`
//     listeners are invoked directly (see receiveMessageFromWorker) rather than via emit.

const { validateNumber } = require("internal/validators");

const messageTypes = {
  REGISTER_MAIN_THREAD_PORT: "registerMainThreadPort",
  UNREGISTER_MAIN_THREAD_PORT: "unregisterMainThreadPort",
  SEND_MESSAGE_TO_WORKER: "sendMessageToWorker",
  RECEIVE_MESSAGE_FROM_WORKER: "receiveMessageFromWorker",
};

// Set once via initThreadInfo() when worker_threads.ts loads.
let currentThreadId = 0;
let isMainThread = true;

// Only populated on the main thread (the hub); always empty elsewhere.
const threadsPorts = new Map<number, any>();

// Only populated on child threads; always undefined on the main thread.
let mainThreadPort: any;

// SharedArrayBuffer must always be Int32, so it's * 4.
// One slot for the operation status (performing / performed) and one for the result.
const WORKER_MESSAGING_SHARED_DATA = 2 * 4;
const WORKER_MESSAGING_STATUS_INDEX = 0;
const WORKER_MESSAGING_RESULT_INDEX = 1;

// Response codes
const WORKER_MESSAGING_RESULT_DELIVERED = 0;
const WORKER_MESSAGING_RESULT_NO_LISTENERS = 1;
const WORKER_MESSAGING_RESULT_LISTENER_ERROR = 2;

function initThreadInfo(threadId: number, mainThread: boolean) {
  currentThreadId = threadId;
  isMainThread = mainThread;
}

// This event handler is always executed on the main thread only.
function handleMessageFromThread(message) {
  switch (message.type) {
    case messageTypes.REGISTER_MAIN_THREAD_PORT: {
      const { threadId, port } = message;

      // Register the port.
      threadsPorts.set(threadId, port);

      // Handle messages on this port. When another thread wants to register a
      // child, this takes care of relaying it, so any thread links to the main one.
      port.on("message", handleMessageFromThread);

      // Never block the thread on this port.
      port.unref();
      break;
    }
    case messageTypes.UNREGISTER_MAIN_THREAD_PORT: {
      const port = threadsPorts.get(message.threadId);
      if (port) {
        port.close();
        threadsPorts.delete(message.threadId);
      }
      break;
    }
    case messageTypes.SEND_MESSAGE_TO_WORKER: {
      const { source, destination, value, transferList, memory } = message;
      sendMessageToWorker(source, destination, value, transferList, memory);
      break;
    }
  }
}

function handleMessageFromMainThread(message) {
  switch (message.type) {
    case messageTypes.RECEIVE_MESSAGE_FROM_WORKER:
      receiveMessageFromWorker(message.source, message.value, message.memory);
      break;
  }
}

function sendMessageToWorker(source, destination, value, transferList, memory) {
  // We are on the main thread, we can directly process the message.
  if (destination === 0) {
    receiveMessageFromWorker(source, value, memory);
    return;
  }

  // Find the port to the target thread.
  const port = threadsPorts.get(destination);

  if (!port) {
    const status = new Int32Array(memory);
    Atomics.store(status, WORKER_MESSAGING_RESULT_INDEX, WORKER_MESSAGING_RESULT_NO_LISTENERS);
    Atomics.store(status, WORKER_MESSAGING_STATUS_INDEX, 1);
    Atomics.notify(status, WORKER_MESSAGING_STATUS_INDEX, 1);
    return;
  }

  port.postMessage(
    {
      type: messageTypes.RECEIVE_MESSAGE_FROM_WORKER,
      source,
      // destination is intentionally omitted: the receiver routes by port, not
      // by re-reading it, so it would be clone-serialized on every message for
      // nothing.
      value,
      memory,
    },
    transferList,
  );
}

function receiveMessageFromWorker(source, value, memory) {
  let response = WORKER_MESSAGING_RESULT_NO_LISTENERS;

  // We can't use process.emit("workerMessage", ...) here for two reasons specific to Bun:
  //   1. process.emit returns true even when there are no listeners, so its return value
  //      can't be used to detect the NO_LISTENERS case.
  //   2. When a listener throws, Bun's process.emit routes the error to the worker's
  //      uncaughtException handler instead of propagating it synchronously, so a try/catch
  //      around process.emit would never see it.
  // Invoking the listeners directly avoids both: NO_LISTENERS is the empty-array case, and a
  // throwing listener propagates synchronously so we can map it to LISTENER_ERROR.
  const listeners = process.listeners("workerMessage");
  if (listeners.length > 0) {
    try {
      for (let i = 0; i < listeners.length; i++) {
        listeners[i].$call(process, value, source);
      }
      response = WORKER_MESSAGING_RESULT_DELIVERED;
    } catch {
      response = WORKER_MESSAGING_RESULT_LISTENER_ERROR;
    }
  }

  // Populate the result.
  const status = new Int32Array(memory);
  Atomics.store(status, WORKER_MESSAGING_RESULT_INDEX, response);
  Atomics.store(status, WORKER_MESSAGING_STATUS_INDEX, 1);
  Atomics.notify(status, WORKER_MESSAGING_STATUS_INDEX, 1);
}

// Bun half of Node's createMainThreadPort: create the channel linking a (future)
// thread to the main thread. Called before `new Worker`.
function createMessagingChannel() {
  const { port1, port2 } = new globalThis.MessageChannel();
  // port1 (portToMain) stays with the hub; port2 (portToWorker) is transferred to
  // the new thread where it becomes that thread's mainThreadPort.
  return { portToMain: port1, portToWorker: port2 };
}

// Bun half of Node's createMainThreadPort: register the hub-side port now that the
// child's threadId is known. Called after `new Worker`.
function registerMainThreadPort(threadId: number, portToMain: any) {
  const registrationMessage = {
    type: messageTypes.REGISTER_MAIN_THREAD_PORT,
    threadId,
    port: portToMain,
  };

  if (isMainThread) {
    handleMessageFromThread(registrationMessage);
  } else if (mainThreadPort) {
    mainThreadPort.postMessage(registrationMessage, [portToMain]);
  }
  // Otherwise this thread is not connected to the main-thread hub (e.g. it was created
  // via the raw Web Worker API rather than worker_threads.Worker). The child is still
  // created fine; it just won't be reachable through postMessageToThread.
}

function destroyMainThreadPort(threadId: number) {
  const unregistrationMessage = {
    type: messageTypes.UNREGISTER_MAIN_THREAD_PORT,
    threadId,
  };

  if (isMainThread) {
    handleMessageFromThread(unregistrationMessage);
  } else if (mainThreadPort) {
    mainThreadPort.postMessage(unregistrationMessage);
  }
}

function setupMainThreadPort(port: any) {
  mainThreadPort = port;
  mainThreadPort.on("message", handleMessageFromMainThread);

  // Never block the process on this port.
  mainThreadPort.unref();
}

async function postMessageToThread(threadId, value, transferList, timeout) {
  if (typeof transferList === "number" && typeof timeout === "undefined") {
    timeout = transferList;
    transferList = [];
  }

  if (typeof transferList === "undefined") {
    transferList = [];
  }

  if (typeof timeout !== "undefined") {
    validateNumber(timeout, "timeout", 0);
  }

  if (threadId === currentThreadId) {
    throw $ERR_WORKER_MESSAGING_SAME_THREAD("Cannot send a message to the same thread.");
  }

  const memory = new SharedArrayBuffer(WORKER_MESSAGING_SHARED_DATA);
  const status = new Int32Array(memory);
  const promise = Atomics.waitAsync(status, WORKER_MESSAGING_STATUS_INDEX, 0, timeout).value;

  const message = {
    type: messageTypes.SEND_MESSAGE_TO_WORKER,
    source: currentThreadId,
    destination: threadId,
    value,
    memory,
    transferList,
  };

  if (isMainThread) {
    handleMessageFromThread(message);
  } else if (mainThreadPort) {
    mainThreadPort.postMessage(message, transferList);
  } else {
    // This thread is not connected to the main-thread hub (e.g. created via the raw Web
    // Worker API), so there is no route to the destination.
    Atomics.store(status, WORKER_MESSAGING_RESULT_INDEX, WORKER_MESSAGING_RESULT_NO_LISTENERS);
    Atomics.store(status, WORKER_MESSAGING_STATUS_INDEX, 1);
    Atomics.notify(status, WORKER_MESSAGING_STATUS_INDEX, 1);
  }

  // Wait for the response.
  const response = await promise;

  if (response === "timed-out") {
    throw $ERR_WORKER_MESSAGING_TIMEOUT("The operation timed out.");
  } else if (status[WORKER_MESSAGING_RESULT_INDEX] === WORKER_MESSAGING_RESULT_NO_LISTENERS) {
    throw $ERR_WORKER_MESSAGING_FAILED(
      "The destination thread no longer exists or is not listening for `workerMessage` events.",
    );
  } else if (status[WORKER_MESSAGING_RESULT_INDEX] === WORKER_MESSAGING_RESULT_LISTENER_ERROR) {
    throw $ERR_WORKER_MESSAGING_ERRORED("The destination thread threw an error while processing the message.");
  }
}

export default {
  initThreadInfo,
  createMessagingChannel,
  registerMainThreadPort,
  destroyMainThreadPort,
  setupMainThreadPort,
  postMessageToThread,
};
