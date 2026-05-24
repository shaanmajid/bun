// https://github.com/oven-sh/bun/issues/31318
// node:http IncomingMessage.rawHeaders must preserve the wire case of header
// names (Node docs: "Header names are not lowercased, and duplicates are not
// merged"), and IncomingMessage.headersDistinct must be a per-name array
// dictionary parallel to `headers`.
import { expect, test } from "bun:test";
import * as http from "node:http";
import * as net from "node:net";

test("IncomingMessage.rawHeaders preserves original case and headersDistinct is populated", async () => {
  const { promise, resolve, reject } = Promise.withResolvers<{
    rawHeaders: string[];
    headers: Record<string, string | string[] | undefined>;
    headersDistinct: Record<string, string[] | undefined>;
  }>();

  const server = http.createServer((req, res) => {
    try {
      resolve({
        rawHeaders: req.rawHeaders,
        headers: req.headers,
        headersDistinct: (req as any).headersDistinct,
      });
    } catch (e) {
      reject(e);
    }
    res.end();
  });

  await using _cleanup = {
    [Symbol.asyncDispose]: () => new Promise<void>(r => server.close(() => r())),
  };

  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as net.AddressInfo).port;

  await new Promise<void>((r, rej) => {
    const sock = net.connect(port, "127.0.0.1", () => {
      sock.write(
        "GET / HTTP/1.1\r\n" +
          "Host: x\r\n" +
          "X-Mixed-CASE: alpha\r\n" +
          "x-LOWER-mixed: beta\r\n" +
          "Connection: close\r\n\r\n",
      );
    });
    sock.on("data", () => {});
    sock.on("end", () => r());
    sock.on("error", rej);
  });

  const got = await promise;

  expect(got.rawHeaders).toEqual([
    "Host",
    "x",
    "X-Mixed-CASE",
    "alpha",
    "x-LOWER-mixed",
    "beta",
    "Connection",
    "close",
  ]);

  expect(got.headers).toEqual({
    host: "x",
    "x-mixed-case": "alpha",
    "x-lower-mixed": "beta",
    connection: "close",
  });

  expect(got.headersDistinct).toEqual({
    host: ["x"],
    "x-mixed-case": ["alpha"],
    "x-lower-mixed": ["beta"],
    connection: ["close"],
  });
});

test("rawHeaders / headersDistinct preserve duplicates and multi-value headers", async () => {
  const { promise, resolve, reject } = Promise.withResolvers<{
    rawHeaders: string[];
    headersDistinct: Record<string, string[] | undefined>;
    setCookie: string[] | string | undefined;
  }>();

  const server = http.createServer((req, res) => {
    try {
      resolve({
        rawHeaders: req.rawHeaders,
        headersDistinct: (req as any).headersDistinct,
        setCookie: req.headers["set-cookie"],
      });
    } catch (e) {
      reject(e);
    }
    res.end();
  });

  await using _cleanup = {
    [Symbol.asyncDispose]: () => new Promise<void>(r => server.close(() => r())),
  };

  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as net.AddressInfo).port;

  await new Promise<void>((r, rej) => {
    const sock = net.connect(port, "127.0.0.1", () => {
      sock.write(
        "GET / HTTP/1.1\r\n" +
          "Host: x\r\n" +
          "Set-Cookie: a=1\r\n" +
          "set-COOKIE: b=2\r\n" +
          "X-Dup: one\r\n" +
          "X-Dup: two\r\n" +
          "Connection: close\r\n\r\n",
      );
    });
    sock.on("data", () => {});
    sock.on("end", () => r());
    sock.on("error", rej);
  });

  const got = await promise;

  // Every occurrence keeps its original case, in arrival order.
  expect(got.rawHeaders).toEqual([
    "Host",
    "x",
    "Set-Cookie",
    "a=1",
    "set-COOKIE",
    "b=2",
    "X-Dup",
    "one",
    "X-Dup",
    "two",
    "Connection",
    "close",
  ]);

  // headersDistinct groups by lowercase name, keeps every value.
  expect(got.headersDistinct).toEqual({
    host: ["x"],
    "set-cookie": ["a=1", "b=2"],
    "x-dup": ["one", "two"],
    connection: ["close"],
  });

  // set-cookie is an array in `req.headers` regardless of incoming case.
  expect(got.setCookie).toEqual(["a=1", "b=2"]);
});
