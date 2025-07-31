import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../api/auth_service.dart';
import '../models/login_request.dart';

class AuthProvider with ChangeNotifier {
  final AuthService _authService = AuthService();
  String? _token;
  bool _isAuthenticated = false;

  String? get token => _token;
  bool get isAuthenticated => _isAuthenticated;

  Future<void> login(String username, String password) async {
    try {
      final loginRequest = LoginRequest(username: username, password: password);
      final loginResponse = await _authService.login(loginRequest);
      _token = loginResponse.token;
      _isAuthenticated = true;
      await _saveToken(_token!);
      notifyListeners();
    } catch (e) {
      // Handle login failure
      print(e);
    }
  }

  Future<void> logout() async {
    _token = null;
    _isAuthenticated = false;
    await _clearToken();
    notifyListeners();
  }

  Future<void> _saveToken(String token) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString('token', token);
  }

  Future<void> _clearToken() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove('token');
  }

  Future<void> tryAutoLogin() async {
    final prefs = await SharedPreferences.getInstance();
    if (!prefs.containsKey('token')) {
      return;
    }
    _token = prefs.getString('token');
    _isAuthenticated = true;
    notifyListeners();
  }
}
