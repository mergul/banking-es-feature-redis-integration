import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/login_request.dart';
import '../models/login_response.dart';

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
}
