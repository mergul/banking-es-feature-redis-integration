import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../api/auth_service.dart';
import '../models/login_request.dart';
import '../models/login_response.dart';

class AuthProvider with ChangeNotifier {
  final AuthService _authService = AuthService();
  String? _token;
  String? _username;
  LoginResponse? _loginResponse;
  bool _isAuthenticated = false;

  String? get token => _token;
  String? get username => _username;
  LoginResponse? get loginResponse => _loginResponse;
  bool get isAuthenticated => _isAuthenticated;

  Future<void> login(String username, String password) async {
    try {
      final loginRequest = LoginRequest(username: username, password: password);
      final loginResponse = await _authService.login(loginRequest);
      
      _token = loginResponse.token['access_token']?.toString();
      _username = loginResponse.username;
      _loginResponse = loginResponse;
      _isAuthenticated = true;
      
      print('🔐 Login successful for user: $_username');
      print('🔑 Token: ${_token?.substring(0, 20)}...');
      
      await _saveAuthData();
      notifyListeners();
    } catch (e) {
      print('❌ Login failed: $e');
      rethrow;
    }
  }

  Future<void> logout() async {
    _token = null;
    _username = null;
    _loginResponse = null;
    _isAuthenticated = false;
    await _clearAuthData();
    notifyListeners();
  }

  Future<void> _saveAuthData() async {
    final prefs = await SharedPreferences.getInstance();
    if (_token != null) {
      await prefs.setString('token', _token!);
      print('💾 Saved token: ${_token!.substring(0, 20)}...');
    }
    if (_username != null) {
      await prefs.setString('username', _username!);
      print('💾 Saved username: $_username');
    } else {
      print('⚠️ Username is null, not saving');
    }
  }

  Future<void> _clearAuthData() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove('token');
    await prefs.remove('username');
  }

  Future<void> tryAutoLogin() async {
    final prefs = await SharedPreferences.getInstance();
    if (!prefs.containsKey('token')) {
      return;
    }
    _token = prefs.getString('token');
    _username = prefs.getString('username');
    _isAuthenticated = true;
    
    print('🔄 Auto login - Token: ${_token?.substring(0, 20)}...');
    print('🔄 Auto login - Username: $_username');
    
    notifyListeners();
  }
}
