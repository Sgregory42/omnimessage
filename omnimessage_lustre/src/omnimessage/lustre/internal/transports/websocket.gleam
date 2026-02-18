import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/uri.{type Uri, Uri}

pub type WebSocketError {
  InvalidUrl(message: String)
  UnsupportedEnvironment(message: String)
}

fn get_websocket_path(path) -> Result(String, Nil) {
  page_uri()
  |> result.try(do_get_websocket_path(path, _))
}

fn do_get_websocket_path(path: String, page_uri: Uri) -> Result(String, Nil) {
  let path_uri =
    uri.parse(path)
    |> result.unwrap(Uri(
      scheme: None,
      userinfo: None,
      host: None,
      port: None,
      path: path,
      query: None,
      fragment: None,
    ))
  use merged <- result.try(uri.merge(page_uri, path_uri))
  use merged_scheme <- result.try(option.to_result(merged.scheme, Nil))
  use ws_scheme <- result.try(convert_scheme(merged_scheme))
  Uri(..merged, scheme: Some(ws_scheme))
  |> uri.to_string
  |> Ok
}

fn convert_scheme(scheme: String) -> Result(String, Nil) {
  case scheme {
    "https" -> Ok("wss")
    "http" -> Ok("ws")
    "ws" | "wss" -> Ok(scheme)
    _ -> Error(Nil)
  }
}

fn page_uri() -> Result(Uri, Nil) {
  do_get_page_url()
  |> uri.parse
}

@external(javascript, "../../../../websocket.ffi.mjs", "get_page_url")
fn do_get_page_url() -> String

// --- Reconnectable WebSocket ---

/// Opaque handle to a reconnectable websocket manager.
pub type ReconnectableWebSocket

pub fn init_reconnectable(
  path: String,
  max_attempts: Option(Int),
  initial_delay_ms: Int,
  max_delay_ms: Int,
  backoff_multiplier: Float,
) -> Result(ReconnectableWebSocket, WebSocketError) {
  case get_websocket_path(path) {
    Ok(url) ->
      do_init_reconnectable(
        url,
        max_attempts,
        initial_delay_ms,
        max_delay_ms,
        backoff_multiplier,
      )
    _ -> Error(InvalidUrl("Invalid Url"))
  }
}

pub fn listen_reconnectable(
  rws: ReconnectableWebSocket,
  on_open on_open: fn(ReconnectableWebSocket) -> Nil,
  on_text_message on_text_message: fn(String) -> Nil,
  on_close on_close: fn(Int, String) -> Nil,
  on_reconnecting on_reconnecting: fn(Int, Int) -> Nil,
) {
  do_listen_reconnectable(rws, on_open, on_text_message, on_close, on_reconnecting)
}

@external(javascript, "../../../../websocket.ffi.mjs", "ws_init_reconnectable")
fn do_init_reconnectable(
  url: String,
  max_attempts: Option(Int),
  initial_delay_ms: Int,
  max_delay_ms: Int,
  backoff_multiplier: Float,
) -> Result(ReconnectableWebSocket, WebSocketError)

@external(javascript, "../../../../websocket.ffi.mjs", "ws_listen_reconnectable")
fn do_listen_reconnectable(
  rws: ReconnectableWebSocket,
  on_open: fn(ReconnectableWebSocket) -> Nil,
  on_text_message: fn(String) -> Nil,
  on_close: fn(Int, String) -> Nil,
  on_reconnecting: fn(Int, Int) -> Nil,
) -> Nil

@external(javascript, "../../../../websocket.ffi.mjs", "ws_send_reconnectable")
pub fn send_reconnectable(rws: ReconnectableWebSocket, msg: String) -> Nil

@external(javascript, "../../../../websocket.ffi.mjs", "ws_close_reconnectable")
pub fn close_reconnectable(rws: ReconnectableWebSocket) -> Nil
