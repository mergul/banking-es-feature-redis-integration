class LoginResponse {
  final String message;
  final Map<String, dynamic> token;
  final String username;
  final List<AccountInfo> accounts;
  final String? primaryAccountId;

  LoginResponse({
    required this.message,
    required this.token,
    required this.username,
    required this.accounts,
    this.primaryAccountId,
  });

  factory LoginResponse.fromJson(Map<String, dynamic> json) {
    // Parse token more carefully
    Map<String, dynamic> tokenMap = {};
    if (json['token'] is Map) {
      tokenMap = Map<String, dynamic>.from(json['token']);
    }

    return LoginResponse(
      message: json['message']?.toString() ?? '',
      token: tokenMap,
      username: json['username']?.toString() ?? '',
      accounts: (json['accounts'] as List<dynamic>?)
          ?.map((account) => AccountInfo.fromJson(account))
          .toList() ?? [],
      primaryAccountId: json['primary_account_id']?.toString(),
    );
  }
}

class AccountInfo {
  final String id;
  final String balance;
  final bool isActive;
  final bool isPrimary;
  final String createdAt;

  AccountInfo({
    required this.id,
    required this.balance,
    required this.isActive,
    required this.isPrimary,
    required this.createdAt,
  });

  factory AccountInfo.fromJson(Map<String, dynamic> json) {
    return AccountInfo(
      id: json['id']?.toString() ?? '',
      balance: json['balance']?.toString() ?? '0.00',
      isActive: json['is_active'] ?? false,
      isPrimary: json['is_primary'] ?? false,
      createdAt: json['created_at']?.toString() ?? '',
    );
  }
}
