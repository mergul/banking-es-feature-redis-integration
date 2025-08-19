import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/login_request.dart';
import '../models/login_response.dart';
import '../models/register_request.dart';

class AuthService {
  final String baseUrl = 'http://localhost:3000/api/auth';

  Future<LoginResponse> login(LoginRequest loginRequest) async {
    final response = await http.post(
      Uri.parse('$baseUrl/login'),
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode(loginRequest.toJson()),
    );

    if (response.statusCode == 200) {
      return LoginResponse.fromJson(jsonDecode(response.body));
    } else {
      throw Exception('Failed to login');
    }
  }

  Future<LoginResponse> register(RegisterRequest registerRequest) async {
    final response = await http.post(
      Uri.parse('$baseUrl/register'),
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode(registerRequest.toJson()),
    );

    if (response.statusCode == 200) {
      // Parse the response to get account information
      final responseData = jsonDecode(response.body);
      
      // Create a LoginResponse with the created account
      return LoginResponse(
        message: responseData['message'] ?? 'Registration successful',
        token: {'access_token': 'temp_token'}, // Will be replaced after login
        username: responseData['username'] ?? registerRequest.username,
        accounts: [
          AccountInfo(
            id: responseData['account_id']?.toString() ?? '',
            balance: responseData['initial_balance']?.toString() ?? '1000.00',
            isActive: true,
            isPrimary: true,
            createdAt: responseData['account_created_at']?.toString() ?? DateTime.now().toIso8601String(),
          ),
        ],
        primaryAccountId: responseData['account_id']?.toString(),
      );
    } else {
      throw Exception('Failed to register');
    }
  }

  Future<LoginResponse> getUserAccounts(String username) async {
    final response = await http.get(
      Uri.parse('$baseUrl/accounts/$username'),
      headers: {'Content-Type': 'application/json'},
    );

    if (response.statusCode == 200) {
      return LoginResponse.fromJson(jsonDecode(response.body));
    } else {
      throw Exception('Failed to get user accounts');
    }
  }

  Future<void> logout(String token) async {
    final response = await http.post(
      Uri.parse('$baseUrl/logout'),
      headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer $token'},
    );

    if (response.statusCode != 200) {
      throw Exception('Failed to logout');
    }
  }
}
