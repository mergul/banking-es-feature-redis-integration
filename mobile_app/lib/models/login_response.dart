class LoginResponse {
  final String message;
  final String token;
  final String username;

  LoginResponse({
    required this.message,
    required this.token,
    required this.username,
  });

  factory LoginResponse.fromJson(Map<String, dynamic> json) {
    return LoginResponse(
      message: json['message']?.toString() ?? '',
      token: json['token']?.toString() ?? '',
      username: json['username']?.toString() ?? '',
    );
  }
}
