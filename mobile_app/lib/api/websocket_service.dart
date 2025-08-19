import 'dart:convert';
import 'dart:async';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/status.dart' as status;

class WebSocketMessage {
  final String type;
  final Map<String, dynamic> data;

  WebSocketMessage({required this.type, required this.data});

  factory WebSocketMessage.fromJson(Map<String, dynamic> json) {
    return WebSocketMessage(
      type: json['type'] ?? '',
      data: json['data'] ?? {},
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'type': type,
      'data': data,
    };
  }
}

class WebSocketService {
  WebSocketChannel? _channel;
  StreamController<WebSocketMessage>? _messageController;
  bool _isConnected = false;
  String? _currentUsername;

  bool get isConnected => _isConnected;
  Stream<WebSocketMessage>? get messageStream => _messageController?.stream;

  Future<void> connect() async {
    try {
      final uri = Uri.parse('ws://localhost:3000/ws');
      _channel = WebSocketChannel.connect(uri);
      _messageController = StreamController<WebSocketMessage>.broadcast();
      _isConnected = true;

      print('🔌 WebSocket connected');

      // Listen for messages
      _channel!.stream.listen(
        (message) {
          try {
            final data = jsonDecode(message);
            final wsMessage = WebSocketMessage.fromJson(data);
            _messageController?.add(wsMessage);
            print('📨 WebSocket message received: ${wsMessage.type}');
          } catch (e) {
            print('❌ Failed to parse WebSocket message: $e');
          }
        },
        onError: (error) {
          print('❌ WebSocket error: $error');
          _isConnected = false;
        },
        onDone: () {
          print('🔌 WebSocket connection closed');
          _isConnected = false;
        },
      );
    } catch (e) {
      print('❌ Failed to connect to WebSocket: $e');
      _isConnected = false;
    }
  }

  Future<void> subscribe(String username) async {
    if (!_isConnected) {
      print('⚠️ WebSocket not connected, attempting to connect...');
      await connect();
    }

    if (_isConnected && _channel != null) {
      final message = {
        'type': 'Subscribe',
        'data': {'username': username},
      };

      _channel!.sink.add(jsonEncode(message));
      _currentUsername = username;
      print('📡 WebSocket subscription sent for user: $username');
    }
  }

  Future<void> unsubscribe(String username) async {
    if (_isConnected && _channel != null) {
      final message = {
        'type': 'Unsubscribe',
        'data': {'username': username},
      };

      _channel!.sink.add(jsonEncode(message));
      _currentUsername = null;
      print('📡 WebSocket unsubscription sent for user: $username');
    }
  }

  Future<void> disconnect() async {
    if (_channel != null) {
      if (_currentUsername != null) {
        await unsubscribe(_currentUsername!);
      }
      
      await _channel!.sink.close(status.goingAway);
      _messageController?.close();
      _isConnected = false;
      _currentUsername = null;
      print('🔌 WebSocket disconnected');
    }
  }

  void dispose() {
    disconnect();
  }
} 