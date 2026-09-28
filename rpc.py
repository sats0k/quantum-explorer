"""Minimal JSON-RPC client for the Phoenixcoin Quantum daemon."""

import base64
import json
import urllib.request
from decimal import Decimal


class RPCError(Exception):
    pass


class CallError(Exception):
    """A failed call inside a non-strict batch.

    The daemon's answer to an errored call is a JSON-RPC error object *inside*
    a batch reply that itself succeeded, so there is nothing to raise -- but
    the caller still needs to know the failure happened and why. Collapsing it
    to None made "the daemon says it cannot resolve this tx" and "the request
    hiccuped" indistinguishable, so the first permanent case and the second
    transient one were treated alike. Carrying the code and message back lets
    the caller tell them apart.
    """

    def __init__(self, method, index, code=None, message=""):
        super().__init__("batch %s[%d] failed: %s" % (method, index, message))
        self.method = method
        self.index = index
        self.code = code
        self.message = message


class RPC:
    def __init__(self, host="127.0.0.1", port=9554, user="user", password="pass",
                 timeout=120):
        self.url = "http://%s:%d/" % (host, port)
        self.auth = base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
        self.timeout = timeout

    def call(self, method, *params):
        body = json.dumps({"method": method, "params": list(params), "id": 1}).encode()
        return self._post(body)

    def batch(self, calls, strict=True):
        """Send a JSON-RPC batch: list of (method, params) tuples.
        Returns a list of results in the same order as `calls`. Responses are
        matched to requests by their JSON-RPC `id` (the batch spec does not
        guarantee response ordering).

        Per-call errors are raised as RPCError in strict mode. The daemon
        answers a failed call with `"result": null` PLUS an `error` object, so
        `error` has to be inspected first -- a plain `"result" in reply` test
        turns every error into a silent None.

        With strict=False a failed call yields a CallError in its slot instead of
        a result, so one bad call doesn't discard the whole window (a batch reply
        comes back HTTP 200; only a single-call failure is HTTP 500). The error's
        code and message ride along for the caller to classify.
        """
        body = json.dumps([
            {"method": m, "params": list(p), "id": i}
            for i, (m, p) in enumerate(calls)
        ]).encode()
        if not calls:
            return []
        replies = self._post(body, is_batch=True)
        by_id = {}
        if isinstance(replies, list):
            for r in replies:
                if isinstance(r, dict) and "id" in r:
                    by_id[r["id"]] = r
        elif isinstance(replies, dict) and "id" in replies:
            by_id[replies["id"]] = replies
        out = []
        for i, c in enumerate(calls):
            r = by_id.get(i) or {}
            err = r.get("error")
            if err:
                msg = err.get("message") if isinstance(err, dict) else str(err)
                if strict:
                    raise RPCError("batch %s[%d] failed: %s" % (c[0], i, msg))
                out.append(CallError(
                    c[0], i, code=err.get("code"), message=msg))
            elif "result" not in r:
                if strict:
                    raise RPCError("batch %s[%d] got no result" % (c[0], i))
                out.append(CallError(c[0], i, message="no result"))
            else:
                out.append(r["result"])
        return out

    def _post(self, body, is_batch=False):
        req = urllib.request.Request(
            self.url, data=body,
            headers={"Authorization": "Basic " + self.auth, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                # parse_float=Decimal: amounts must not pass through a binary
                # float. json.load's default float parser is lossy past 2**53,
                # and the value is already wrong by the time it reaches us --
                # {"value": 90071992.54740993} arrives as ...94. Decimal keeps
                # the decimal literal the daemon actually sent, so the integer
                # conversion downstream is exact.
                obj = json.load(resp, parse_float=Decimal)
        except urllib.error.HTTPError as e:
            raise RPCError("HTTP %d: %s" % (e.code, e.read().decode(errors="replace")))
        except json.JSONDecodeError as e:
            raise RPCError("invalid JSON-RPC response: %s" % e)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            raise RPCError("connection failed: %s" % e)
        if is_batch:
            return obj
        if not isinstance(obj, dict):
            raise RPCError("invalid JSON-RPC response type: %s" % type(obj).__name__)
        if obj.get("error"):
            raise RPCError(str(obj["error"]))
        if "result" not in obj:
            raise RPCError("JSON-RPC response missing 'result'")
        return obj["result"]

    def getblockcount(self):
        return int(self.call("getblockcount"))

    def getblockhash(self, height):
        return self.call("getblockhash", int(height))

    def getblock(self, blockhash):
        return self.call("getblock", blockhash)

    def getrawtransaction(self, txid, verbose=True):
        return self.call("getrawtransaction", txid, 1 if verbose else 0)

    def getrawmempool(self):
        return self.call("getrawmempool")

    def validateaddress(self, address):
        return self.call("validateaddress", address)