import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../api/auth_service.dart';
import '../api/websocket_service.dart';
import '../models/login_request.dart';
import '../models/login_response.dart';
import '../models/register_request.dart';

class AuthProvider with ChangeNotifier {
  final AuthService _authService = AuthService();
  final WebSocketService _websocketService = WebSocketService();
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
  WebSocketService get websocketService => _websocketService;

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
      
      // Subscribe to WebSocket updates for this user
      await _websocketService.subscribe(username);
      
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
    // Unsubscribe from WebSocket updates
    if (_username != null) {
      await _websocketService.unsubscribe(_username!);
    }
    
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

  Future<void> registerAndLogin(String username, String email, String password) async {
    _isLoading = true;
    notifyListeners();
    
    try {
      // Step 1: Register user
      final registerRequest = RegisterRequest(username: username, email: email, password: password);
      final registerResponse = await _authService.register(registerRequest);
      
      print('✅ Registration successful for user: $username');
      print('📊 Account created: ${registerResponse.accounts.first.id}');
      
      // Step 2: Wait for projection to be updated with polling
      print('⏳ Waiting for projection to be updated...');
      await _waitForProjectionUpdate(username);
      print('✅ Projection update wait completed');
      
      // Step 3: Auto login after registration
      final loginRequest = LoginRequest(username: username, password: password);
      final loginResponse = await _authService.login(loginRequest);
      
      // If login response has no accounts but register response does, use register response accounts
      if (loginResponse.accounts.isEmpty && registerResponse.accounts.isNotEmpty) {
        print('⚠️ Login response has no accounts, using register response accounts');
        _loginResponse = LoginResponse(
          message: loginResponse.message,
          token: loginResponse.token,
          username: loginResponse.username,
          accounts: registerResponse.accounts,
          primaryAccountId: registerResponse.primaryAccountId,
        );
      } else {
        _loginResponse = loginResponse;
      }
      
      // Extract access_token safely
      final accessToken = _loginResponse!.token['access_token'];
      _token = accessToken?.toString() ?? '';
      _username = _loginResponse!.username;
      _isAuthenticated = true;
      
      print('🔐 Auto login successful for user: $_username');
      print('📊 Accounts count: ${_loginResponse!.accounts.length}');
      
      await _saveAuthData();
      
      // Subscribe to WebSocket updates for this user
      await _websocketService.subscribe(username);
      
      _isLoading = false;
      notifyListeners();
    } catch (e) {
      print('❌ Register and login failed: $e');
      _isLoading = false;
      notifyListeners();
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

  Future<void> _waitForProjectionUpdate(String username) async {
    const maxAttempts = 30; // 30 attempts * 200ms = 6 seconds max
    const delayMs = 200;
    
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        print('🔄 Polling attempt $attempt/$maxAttempts for user: $username');
        
        final response = await _authService.getUserAccounts(username);
        
        if (response.accounts.isNotEmpty) {
          print('✅ Projection updated! Found ${response.accounts.length} accounts');
          return;
        }
        
        print('⏳ No accounts found yet, waiting ${delayMs}ms...');
        await Future.delayed(Duration(milliseconds: delayMs));
        
      } catch (e) {
        print('⚠️ Polling error on attempt $attempt: $e');
        await Future.delayed(Duration(milliseconds: delayMs));
      }
    }
    
    print('⚠️ Projection update timeout after $maxAttempts attempts');
  }
}
