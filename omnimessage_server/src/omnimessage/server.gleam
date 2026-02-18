/// omnimessage/server is the collection of tools allowing you to handle
/// connections from omnimessage/lustre applications.
///
/// The rule of thumb is if it initiates the connection, it's a client. If it
/// responds to a connection request, it's a server.
///
/// While you could do this manually fairly simples, theese tools can help you
/// get started quicker and provide a nicer quality-of-life.
///
/// Do read the source of the functions to understand how to make your own
/// customized solution.
///
/// Currently only the erlang target is supported, but you could easily adapt
/// the principles to the Node, Deno, or Bun targets. Or even to a runtime
/// outside Gleam's ecosystem -- as long as you can send and receive encoded
/// messages, you can communicate with omnimessage/lustre.
///
import gleam/erlang/process.{type Subject}
import gleam/function
import gleam/http
import gleam/http/request
import gleam/option.{type Option, None, Some}
import gleam/result
import lustre/component
import lustre/server_component

import dream/servers/mist/websocket as dream_websocket
import lustre
import lustre/effect.{type Effect}
import lustre/element
import mist
import wisp

/// Holds decode and encode functions for omnimessage messages. Decode errors
/// will be called back for you to handle, while Encode errors are interpreted
/// as "skip this message" -- no error will be raised for them and they won't
/// be sent over.
///
/// Since an `EncoderDecoder` is expected to receive the whole message type of
/// an application, but usually will ignore messages that aren't shared, it's
/// best to define it as a thin wrapper around shared encoders/decoders:
///
/// ```gleam
/// // Holds shared message types, encoders and decoders
/// import shared
///
/// let encoder_decoder =
///   EncoderDecoder(
///     fn(msg) {
///       case msg {
///         // Messages must be encodable
///         ClientMessage(message) -> Ok(shared.encode_client_message(message))
///         // Return Error(Nil) for messages you don't want to send out
///         _ -> Error(Nil)
///       }
///     },
///     fn(encoded_msg) {
///       // Unsupported messages will cause TransportError(DecodeError(error))
///       shared.decode_server_message(encoded_msg)
///       |> result.map(ServerMessage)
///     },
///   )
/// ```
///
pub type EncoderDecoder(msg, encoding, decode_error) {
  EncoderDecoder(
    encode: fn(msg) -> Result(encoding, Nil),
    decode: fn(encoding) -> Result(msg, decode_error),
  )
}

/// A utility function for easily handling messages:
///
/// ```gleam
/// let out_msg = pipe(in_msg, encoder_decoder, handler)
/// ```
///
pub fn pipe(
  msg: encoding,
  encoder_decoder: EncoderDecoder(msg, encoding, decode_error),
  handler: fn(msg) -> msg,
) -> Result(Option(encoding), decode_error) {
  msg
  |> encoder_decoder.decode
  |> result.map(handler)
  |> result.map(encoder_decoder.encode)
  // Encoding error means "skip this message"
  |> result.map(option.from_result)
}

///
pub opaque type App(start_args, model, msg, encoding, decode_error) {
  App(
    init: fn(start_args) -> #(model, Effect(msg)),
    update: fn(model, msg) -> #(model, Effect(msg)),
    options: Option(List(component.Option(msg))),
    encoder_decoder: EncoderDecoder(msg, encoding, decode_error),
  )
}

/// This creates a version of a Lustre application that can be used in
/// `omnimessage/server.start_actor` (see below). A view is not necessary, as
/// this application will never render anything.
///
pub fn application(
  init init: fn(start_args) -> #(model, Effect(msg)),
  update update: fn(model, msg) -> #(model, Effect(msg)),
  encoder_decoder encoder_decoder: EncoderDecoder(msg, encoding, decode_error),
) -> App(start_args, model, msg, encoding, decode_error) {
  App(init: init, update: update, options: None, encoder_decoder:)
}

pub fn component(
  init init: fn(start_args) -> #(model, Effect(msg)),
  update update: fn(model, msg) -> #(model, Effect(msg)),
  options options: List(component.Option(msg)),
  encoder_decoder encoder_decoder: EncoderDecoder(msg, encoding, decode_error),
) -> App(start_args, model, msg, encoding, decode_error) {
  App(init: init, update: update, options: Some(options), encoder_decoder:)
}

/// This is a beefed up version of `lustre.start_actor` that allows subscribing
/// to messages dispatched inside the runtime.
///
/// This is what enables using a Lustre server component for communication,
/// powering `mist_websocket_application()` below.
///
pub fn start_server_component(
  app: App(start_args, model, msg, encoding, decode_error),
  with_args start_args: start_args,
  with_listener listener: fn(msg) -> Nil,
) -> Result(lustre.Runtime(msg), lustre.Error) {
  let wrapped_update = fn(model, msg) {
    listener(msg)
    app.update(model, msg)
  }

  let view = fn(_model) { element.none() }
  let lustre_app = case app.options {
    None -> lustre.application(app.init, wrapped_update, view)
    Some(options) -> lustre.component(app.init, wrapped_update, view, options)
  }

  lustre.start_server_component(lustre_app, with: start_args)
}

/// A wisp middleware to automatically handle HTTP POST omnimessage messages.
///
///   - `req`              The wisp request
///   - `path`             The path to which messages are POSTed
///   - `encoder_decoder`  For encoding and decoding messages
///   - `handler`          For handling the incoming messages
///
/// See a full example using this in the Readme or in the examples folder.
///
pub fn wisp_http_middleware(
  req: wisp.Request,
  path: String,
  encoder_decoder,
  handler,
  fun: fn() -> wisp.Response,
) -> wisp.Response {
  case req.path == path, req.method {
    True, http.Post -> {
      use req_body <- wisp.require_string_body(req)

      case
        req_body
        |> pipe(encoder_decoder, handler)
      {
        Ok(Some(res_body)) -> wisp.response(200) |> wisp.string_body(res_body)
        Ok(None) -> wisp.response(200)
        Error(_) -> wisp.unprocessable_entity()
      }
    }
    _, _ -> fun()
  }
}

/// A mist websocket handler to automatically respond to omnimessage messages.
///
/// Return this as a response to the websocket init request.
///
///   - `req`              The mist request
///   - `encoder_decoder`  For encoding and decoding messages
///   - `handler`          For handling the incoming messages
///   - `on_error`         For handling decode errors
///
/// See a full example using this in the Readme or in the examples folder.
///
pub fn mist_websocket_pipe(
  req: request.Request(mist.Connection),
  encoder_decoder: EncoderDecoder(msg, String, decode_error),
  handler: fn(msg) -> msg,
  on_error: fn(decode_error) -> Nil,
) {
  mist.websocket(
    request: req,
    on_init: fn(_conn) { #(None, None) },
    handler: fn(runtime, msg, conn) {
      case msg {
        mist.Text(text_msg) -> {
          let _ = case pipe(text_msg, encoder_decoder, handler) {
            Ok(Some(encoded_msg)) -> mist.send_text_frame(conn, encoded_msg)
            Ok(None) -> Ok(Nil)
            Error(decode_error) -> Ok(on_error(decode_error))
          }
          mist.continue(runtime)
        }

        mist.Binary(_) -> mist.continue(runtime)

        mist.Custom(_) -> mist.continue(runtime)

        mist.Closed | mist.Shutdown -> mist.stop()
      }
    },
    on_close: fn(_) { Nil },
  )
}

type WebsocketState(msg) {
  WebsocketState(
    runtime: lustre.Runtime(msg),
    omni_self: Subject(msg),
    lustre_self: Subject(server_component.ClientMessage(msg)),
  )
}

/// A mist websocket handler to automatically respond to omnimessage messages
/// via a Lustre server component. The server component can then be used
/// similarly to one created by an `omnimessage/lustre` and handle the messages
/// via update, dispatch, and effects.
///
/// Return this as a response to the websocket init request.
///
///   - `req`       The mist request
///   - `app`       An application created with `omnimessage/server.application`
///   - `flags`     Flags to hand to the application's `init`
///   - `on_error`  For handling decode errors
///
/// See a full example using this in the Readme or in the examples folder.
///
pub fn mist_websocket_application(
  req: request.Request(mist.Connection),
  app: App(flags, model, msg, String, decode_error),
  flags: flags,
  on_error: fn(decode_error) -> Nil,
) {
  mist.websocket(
    request: req,
    on_init: fn(_conn) {
      let omni_self = process.new_subject()
      let lustre_self = process.new_subject()
      let assert Ok(runtime) =
        start_server_component(app, flags, process.send(omni_self, _))

      let state = WebsocketState(runtime:, omni_self:, lustre_self:)

      #(
        state,
        option.Some(
          process.new_selector()
          |> process.select_map(for: omni_self, mapping: function.identity),
        ),
      )
    },
    handler: fn(state: WebsocketState(msg), websocket_msg, conn) {
      case websocket_msg {
        mist.Text(text_msg) -> {
          case app.encoder_decoder.decode(text_msg) {
            Ok(decoded_msg) ->
              lustre.send(state.runtime, lustre.dispatch(decoded_msg))
            Error(decode_error) -> on_error(decode_error)
          }
          mist.continue(state)
        }
        mist.Binary(_) -> mist.continue(state)
        mist.Custom(custom_msg) -> {
          // TODO: do we really want to crash this?
          let assert Ok(_) = case app.encoder_decoder.encode(custom_msg) {
            Ok(encoded_msg) -> mist.send_text_frame(conn, encoded_msg)
            // Encode error is interpreted as "skip this message"
            Error(_) -> Ok(Nil)
          }

          mist.continue(state)
        }
        mist.Closed | mist.Shutdown -> {
          server_component.deregister_subject(state.lustre_self)
          |> lustre.send(to: state.runtime)

          mist.stop()
        }
      }
    },
    on_close: fn(state) {
      server_component.deregister_subject(state.lustre_self)
      |> lustre.send(to: state.runtime)
    },
  )
}

/// State for Dream WebSocket pipe handlers.
pub type DreamPipeState {
  DreamPipeState
}

/// Handlers for Dream WebSocket integration with omnimessage.
/// Use with `websocket.upgrade_websocket()`.
pub type DreamWebsocketHandlers {
  DreamWebsocketHandlers(
    on_init: fn(dream_websocket.Connection, Nil) ->
      #(DreamPipeState, Option(process.Selector(Nil))),
    on_message: fn(
      DreamPipeState,
      dream_websocket.Message(Nil),
      dream_websocket.Connection,
      Nil,
    ) ->
      dream_websocket.Action(DreamPipeState, Nil),
    on_close: fn(DreamPipeState, Nil) -> Nil,
  )
}

/// Dream WebSocket handlers for omnimessage pipe pattern.
///
/// Returns handlers compatible with Dream's `websocket.upgrade_websocket()`.
/// Use with Dream like this:
///
/// ```gleam
/// fn handle_ws(request, _context, _services) {
///   let handlers =
///     omniserver.dream_websocket_pipe_handlers(
///       encoder_decoder(),
///       handle_message,
///       fn(err) { logging.log(logging.Error, string.inspect(err)) },
///     )
///   websocket.upgrade_websocket(
///     request,
///     dependencies: Nil,
///     on_init: handlers.on_init,
///     on_message: handlers.on_message,
///     on_close: handlers.on_close,
///   )
/// }
/// ```
pub fn dream_websocket_pipe_handlers(
  encoder_decoder: EncoderDecoder(msg, String, decode_error),
  handler: fn(msg) -> msg,
  on_error: fn(decode_error) -> Nil,
) -> DreamWebsocketHandlers {
  let on_init = fn(_conn: dream_websocket.Connection, _deps: Nil) {
    #(DreamPipeState, None)
  }

  let on_message = fn(
    state: DreamPipeState,
    message: dream_websocket.Message(Nil),
    connection: dream_websocket.Connection,
    _deps: Nil,
  ) -> dream_websocket.Action(DreamPipeState, Nil) {
    case message {
      dream_websocket.TextMessage(text) -> {
        case pipe(text, encoder_decoder, handler) {
          Ok(Some(response)) -> {
            let _ = dream_websocket.send_text(connection, response)
            dream_websocket.continue_connection(state)
          }
          Ok(None) -> dream_websocket.continue_connection(state)
          Error(decode_error) -> {
            on_error(decode_error)
            dream_websocket.continue_connection(state)
          }
        }
      }
      dream_websocket.ConnectionClosed -> dream_websocket.stop_connection()
      dream_websocket.BinaryMessage(_) ->
        dream_websocket.continue_connection(state)
      dream_websocket.CustomMessage(_) ->
        dream_websocket.continue_connection(state)
    }
  }

  let on_close = fn(_state: DreamPipeState, _deps: Nil) { Nil }

  DreamWebsocketHandlers(on_init:, on_message:, on_close:)
}

/// Internal message type for selector in stateful handlers.
/// Allows the selector to handle both push messages and bridge messages.
pub type InternalMsg(msg) {
  PushMsg(msg)
  BridgeMsg(String)
}

/// State for Dream WebSocket stateful handlers.
/// Wraps the connection, a subject for server-initiated messages, and user state.
pub type StatefulState(state, msg) {
  StatefulState(
    connection: dream_websocket.Connection,
    push_subject: Subject(msg),
    bridge_subject: Subject(String),
    user_state: state,
  )
}

/// Handlers for Dream WebSocket integration with stateful support.
/// Use with `websocket.upgrade_websocket()`.
pub type StatefulHandlers(state, msg, deps, decode_error) {
  StatefulHandlers(
    on_init: fn(dream_websocket.Connection, deps) ->
      #(StatefulState(state, msg), Option(process.Selector(InternalMsg(msg)))),
    on_message: fn(
      StatefulState(state, msg),
      dream_websocket.Message(InternalMsg(msg)),
      dream_websocket.Connection,
      deps,
    ) ->
      dream_websocket.Action(StatefulState(state, msg), InternalMsg(msg)),
    on_close: fn(StatefulState(state, msg), deps) -> Nil,
  )
}

/// Dream WebSocket handlers with server-initiated push support.
///
/// Creates handlers that support both request-response and server-initiated messages.
/// Returns handlers compatible with Dream's `websocket.upgrade_websocket()`.
///
/// Unlike `dream_websocket_pipe_handlers`, this version:
/// - Creates a `Subject` for each connection to receive push messages
/// - Creates a `Subject` for each connection to receive bridge messages (raw JSON strings)
/// - Returns a `Selector` from `on_init` to listen for both push and bridge messages
/// - Handles `CustomMessage` to send server-initiated messages to clients
/// - Bridge messages are converted to msg type using `bridge_converter` before sending
///
/// Use with Dream like this:
///
/// ```gleam
/// fn handle_ws(request, _context, _services) {
///   let handlers =
///     omniserver.dream_websocket_stateful_handlers(
///       encoder_decoder(),
///       fn(push_subject, bridge_subject) { MyState(push: push_subject, bridge: bridge_subject) },
///       handle_message,
///       fn(json_str) { json.decode(json_str, using: my_decoder) |> result.nil_error },
///       fn(err) { logging.log(logging.Error, string.inspect(err)) },
///     )
///   websocket.upgrade_websocket(
///     request,
///     dependencies: Nil,
///     on_init: handlers.on_init,
///     on_message: handlers.on_message,
///     on_close: handlers.on_close,
///   )
/// }
/// ```
pub fn dream_websocket_stateful_handlers(
  encoder_decoder: EncoderDecoder(msg, String, decode_error),
  init_state: fn(Subject(msg), Subject(String)) -> state,
  handler: fn(msg, state) -> #(Option(msg), state),
  bridge_converter: fn(String) -> Result(msg, Nil),
  on_error: fn(decode_error) -> Nil,
) -> StatefulHandlers(state, msg, deps, decode_error) {
  let on_init = fn(connection: dream_websocket.Connection, _deps: deps) {
    // Create subjects for server-initiated push messages and bridge messages
    let push_subject = process.new_subject()
    let bridge_subject = process.new_subject()

    // Initialize user state with both subjects
    let user_state = init_state(push_subject, bridge_subject)

    // Create state wrapper with connection
    let state = StatefulState(connection:, push_subject:, bridge_subject:, user_state:)

    // Create selector to receive both push and bridge messages
    let selector =
      process.new_selector()
      |> process.select_map(for: push_subject, mapping: fn(msg) { PushMsg(msg) })
      |> process.select_map(for: bridge_subject, mapping: fn(str) { BridgeMsg(str) })
      |> Some

    #(state, selector)
  }

  let on_message = fn(
    state: StatefulState(state, msg),
    message: dream_websocket.Message(InternalMsg(msg)),
    connection: dream_websocket.Connection,
    _deps: deps,
  ) -> dream_websocket.Action(StatefulState(state, msg), InternalMsg(msg)) {
    case message {
      dream_websocket.TextMessage(text) -> {
        // Decode incoming message
        case encoder_decoder.decode(text) {
          Ok(decoded_msg) -> {
            // Handle the message and get optional response + new state
            let #(maybe_response, new_user_state) = handler(decoded_msg, state.user_state)

            // Send response if handler returned one
            case maybe_response {
              Some(response_msg) -> {
                case encoder_decoder.encode(response_msg) {
                  Ok(encoded) -> {
                    let _ = dream_websocket.send_text(connection, encoded)
                    Nil
                  }
                  Error(_) -> Nil  // Encoding error means "skip this message"
                }
              }
              None -> Nil
            }

            // Update state and continue
            let new_state = StatefulState(..state, user_state: new_user_state)
            dream_websocket.continue_connection(new_state)
          }
          Error(decode_error) -> {
            on_error(decode_error)
            dream_websocket.continue_connection(state)
          }
        }
      }

      dream_websocket.CustomMessage(internal_msg) -> {
        // Server-initiated messages: either push or bridge
        case internal_msg {
          PushMsg(push_msg) -> {
            // Existing push logic: encode and send
            case encoder_decoder.encode(push_msg) {
              Ok(encoded) -> {
                let _ = dream_websocket.send_text(state.connection, encoded)
                Nil
              }
              Error(_) -> Nil  // Encoding error means "skip this message"
            }
          }
          BridgeMsg(json_str) -> {
            // Convert bridge message using converter, then send
            case bridge_converter(json_str) {
              Ok(msg) -> {
                case encoder_decoder.encode(msg) {
                  Ok(encoded) -> {
                    let _ = dream_websocket.send_text(state.connection, encoded)
                    Nil
                  }
                  Error(_) -> Nil  // Encoding error means "skip this message"
                }
              }
              Error(_) -> Nil  // Silently ignore failed conversions
            }
          }
        }
        dream_websocket.continue_connection(state)
      }

      dream_websocket.BinaryMessage(_) -> {
        dream_websocket.continue_connection(state)
      }

      dream_websocket.ConnectionClosed -> {
        dream_websocket.stop_connection()
      }
    }
  }

  // TODO: handle on_close
  let on_close = fn(_state: StatefulState(state, msg), _deps: deps) { Nil }

  StatefulHandlers(on_init:, on_message:, on_close:)
}
