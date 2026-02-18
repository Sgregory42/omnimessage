import { Ok, Error } from './gleam.mjs';
import {
  InvalidUrl,
  UnsupportedEnvironment
} from './omnimessage/lustre/internal/transports/websocket.mjs';

export const get_page_url = () => document.URL;

const DO_NOT_RECONNECT_CODES = new Set([
  1000, // Normal closure
  1001, // Going away (page navigation)
  1002, // Protocol error
  1003, // Unexpected type of data
  1007, // Incomprehensible frame
  1008, // Policy violated
  1009, // Message too big
]);

class ReconnectableWebSocket {
  constructor(url, config) {
    this.url = url;
    this.maxAttempts = config.maxAttempts;
    this.initialDelayMs = config.initialDelayMs;
    this.maxDelayMs = config.maxDelayMs;
    this.backoffMultiplier = config.backoffMultiplier;
    this.ws = null;
    this.attempt = 0;
    this.timer = null;
    this.intentionallyClosed = false;
    this.callbacks = null;
  }

  connect() {
    try {
      this.ws = new WebSocket(this.url);
    } catch (_) {
      this.scheduleReconnect();
      return;
    }

    this.ws.addEventListener("open", (_) => {
      this.attempt = 0;
      if (this.callbacks) {
        this.callbacks.onOpen(this);
      }
    });

    this.ws.addEventListener("message", (event) => {
      if (typeof event.data === "string" && this.callbacks) {
        this.callbacks.onText(event.data);
      }
    });

    this.ws.addEventListener("close", (event) => {
      if (this.intentionallyClosed) {
        if (this.callbacks) {
          this.callbacks.onClose(event.code, event.reason ?? "");
        }
        return;
      }

      if (DO_NOT_RECONNECT_CODES.has(event.code)) {
        if (this.callbacks) {
          this.callbacks.onClose(event.code, event.reason ?? "");
        }
        return;
      }

      // Notify app of disconnect before attempting reconnection
      if (this.callbacks) {
        this.callbacks.onClose(event.code, event.reason ?? "");
      }
      this.scheduleReconnect();
    });

    this.ws.addEventListener("error", (_) => {
      // The close event fires after error, so reconnection is handled there.
    });
  }

  scheduleReconnect() {
    if (this.maxAttempts !== null && this.attempt >= this.maxAttempts) {
      return;
    }

    const delay = Math.min(
      this.initialDelayMs * Math.pow(this.backoffMultiplier, this.attempt),
      this.maxDelayMs,
    );

    if (this.callbacks) {
      this.callbacks.onReconnecting(this.attempt, Math.round(delay));
    }

    this.timer = setTimeout(() => {
      this.attempt += 1;
      this.connect();
    }, delay);
  }

  send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(msg);
    }
    // Otherwise silently drop — no queuing
  }

  close() {
    this.intentionallyClosed = true;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.ws) {
      this.ws.close();
    }
  }
}

function optionToNullable(opt) {
  // Gleam's Some(value) stores value at [0], None has no such property
  return 0 in opt ? opt[0] : null;
}

export const ws_init_reconnectable = (url, max_attempts, initial_delay_ms, max_delay_ms, backoff_multiplier) => {
  if (typeof WebSocket === "function") {
    try {
      const rws = new ReconnectableWebSocket(url, {
        maxAttempts: optionToNullable(max_attempts),
        initialDelayMs: initial_delay_ms,
        maxDelayMs: max_delay_ms,
        backoffMultiplier: backoff_multiplier,
      });
      return new Ok(rws);
    } catch (error) {
      return new Error(new InvalidUrl(error.message));
    }
  } else {
    return new Error(new UnsupportedEnvironment("WebSocket global unavailable"));
  }
}

export const ws_listen_reconnectable = (rws, on_open, on_text, on_close, on_reconnecting) => {
  rws.callbacks = {
    onOpen: on_open,
    onText: on_text,
    onClose: on_close,
    onReconnecting: on_reconnecting,
  };
  rws.connect();
}

export const ws_send_reconnectable = (rws, msg) => {
  rws.send(msg);
}

export const ws_close_reconnectable = (rws) => {
  rws.close();
}
