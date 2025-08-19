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
  bool _isLoading = false;

  String? get token => _token;
  String? get username => _username;
  LoginResponse? get loginResponse => _loginResponse;
  bool get isAuthenticated => _isAuthenticated;
  bool get isLoading => _isLoading;

  Future<void> login(String username, String password) async {
    _isLoading = true;
    notifyListeners();
    
    try {
      final loginRequest = LoginRequest(username: username, password: password);
      final loginResponse = await _authService.login(loginRequest);
      
      // Extract access_token safely
      final accessToken = loginResponse.token['access_token'];
      _token = accessToken?.toString() ?? '';
      _username = loginResponse.username;
      _loginResponse = loginResponse;
      _isAuthenticated = true;
      
      print('🔐 Login successful for user: $_username');
      print('🔑 Token: ${_token?.substring(0, 20)}...');
      print('🔑 Full Token: $_token');
      print('🔑 Token type: ${_token.runtimeType}');
      print('🔑 Token length: ${_token?.length}');
      print('🔑 Raw access_token: $accessToken');
      print('🔑 Raw access_token type: ${accessToken.runtimeType}');
      print('📊 Accounts count: ${loginResponse.accounts.length}');
      
      // Debug: Check if accessToken is a Map
      if (accessToken is Map) {
        print('🔍 accessToken is a Map, extracting access_token value');
        final actualToken = accessToken['access_token']?.toString() ?? '';
        print('🔍 Actual token: ${actualToken.substring(0, 20)}...');
        _token = actualToken;
      }
      
      await _saveAuthData();
      _isLoading = false;
      notifyListeners();
    } catch (e) {
      print('❌ Login failed: $e');
      _isLoading = false;
      notifyListeners();
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

  Future<void> refreshUserAccounts() async {
    if (_username == null || _username!.isEmpty) {
      print('⚠️ Cannot refresh accounts: username is null or empty');
      return;
    }

    try {
      print('🔄 Refreshing user accounts for: $_username');
      final response = await _authService.getUserAccounts(_username!);
      _loginResponse = response;
      print('✅ Refreshed accounts count: ${response.accounts.length}');
      notifyListeners();
    } catch (e) {
      print('❌ Failed to refresh user accounts: $e');
      rethrow;
    }
  }

  Future<void> _saveAuthData() async {
    final prefs = await SharedPreferences.getInstance();
    if (_token != null && _token!.isNotEmpty) {
      // Ensure token is a clean string
      final cleanToken = _token!.trim();
      await prefs.setString('token', cleanToken);
      print('💾 Saved token: ${cleanToken.substring(0, 20)}...');
      print('💾 Token type saved: ${cleanToken.runtimeType}');
      print('💾 Token length saved: ${cleanToken.length}');
    } else {
      print('⚠️ Token is null or empty, not saving');
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
      print('🔄 No saved token found for auto login');
      return;
    }
    
    _token = prefs.getString('token');
    _username = prefs.getString('username');
    _isAuthenticated = true;
    
    print('🔄 Auto login - Token: ${_token?.substring(0, 20)}...');
    print('🔄 Auto login - Username: $_username');
    print('🔄 Auto login - Token type: ${_token.runtimeType}');
    print('🔄 Auto login - Token length: ${_token?.length}');
    
    // Try to load user accounts for auto login
    if (_username != null && _username!.isNotEmpty) {
      try {
        await refreshUserAccounts();
      } catch (e) {
        print('⚠️ Failed to load accounts during auto login: $e');
        // Don't fail auto login if accounts can't be loaded
      }
    }
    
    notifyListeners();
  }
}
