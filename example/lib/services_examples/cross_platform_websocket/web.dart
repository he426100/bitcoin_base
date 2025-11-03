import 'dart:async';
import 'dart:js_interop';
import 'dart:typed_data';

import 'core.dart';
import 'package:web/web.dart';

Future<WebSocketCore> connectSoc(String url, {List<String>? protocols}) async =>
    await WebsocketWeb.connect(url);

class WebsocketWeb implements WebSocketCore {
  final WebSocket _socket;
  final StreamController<dynamic> _streamController =
      StreamController<dynamic>();
  final Completer<void> _connectedCompleter = Completer<void>();

  WebsocketWeb._(this._socket) {
    _socket.onOpen.listen((Event event) {
      _connectedCompleter.complete();
    });

    _socket.onMessage.listen((MessageEvent event) {
      _streamController.add(event.data);
    });

    _socket.onClose.listen((CloseEvent event) {
      _streamController.close();
    });
  }

  @override
  void close({int? code}) {
    if (code != null) {
      _socket.close(code, '');
    } else {
      _socket.close();
    }
  }

  @override
  bool get isConnected => _socket.readyState == WebSocket.OPEN;
  @override
  Stream<dynamic> get stream => _streamController.stream;

  static Future<WebsocketWeb> connect(String url,
      {List<String>? protocols}) async {
    final completer = Completer<WebsocketWeb>();
    final socket = protocols != null 
        ? WebSocket(url, protocols.map((e) => e.toJS).toList().toJS)
        : WebSocket(url);
    WebsocketWeb._(socket)._connectedCompleter.future.then((_) {
      completer.complete(WebsocketWeb._(socket));
    });
    return completer.future;
  }

  @override
  void sink(List<int> message) {
    _socket.send(Uint8List.fromList(message).toJS);
  }
}
