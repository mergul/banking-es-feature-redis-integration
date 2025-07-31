import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/account.dart';
import '../models/transaction.dart';
import '../models/deposit_request.dart';
import '../models/withdraw_request.dart';

class AccountService {
  final String baseUrl = 'http://localhost:3000/api/cqrs/accounts';

  Future<List<Account>> getAllAccounts(String token) async {
    final response = await http.get(
      Uri.parse(baseUrl),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
    );

    if (response.statusCode == 200) {
      final List<dynamic> accountsJson = jsonDecode(response.body);
      return accountsJson.map((json) => Account.fromJson(json)).toList();
    } else {
      throw Exception('Failed to get accounts');
    }
  }

  Future<Account> getAccount(String token, String accountId) async {
    final response = await http.get(
      Uri.parse('$baseUrl/$accountId'),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
    );

    if (response.statusCode == 200) {
      return Account.fromJson(jsonDecode(response.body));
    } else {
      throw Exception('Failed to get account');
    }
  }

  Future<double> getAccountBalance(String token, String accountId) async {
    final response = await http.get(
      Uri.parse('$baseUrl/$accountId/balance'),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
    );

    if (response.statusCode == 200) {
      final balanceJson = jsonDecode(response.body);
      return balanceJson['balance'];
    } else {
      throw Exception('Failed to get account balance');
    }
  }

  Future<List<Transaction>> getAccountTransactions(String token, String accountId) async {
    final response = await http.get(
      Uri.parse('$baseUrl/$accountId/transactions'),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
    );

    if (response.statusCode == 200) {
      final List<dynamic> transactionsJson = jsonDecode(response.body);
      return transactionsJson.map((json) => Transaction.fromJson(json)).toList();
    } else {
      throw Exception('Failed to get account transactions');
    }
  }

  Future<void> deposit(String token, String accountId, DepositRequest request) async {
    final response = await http.post(
      Uri.parse('$baseUrl/$accountId/deposit'),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
      body: jsonEncode(request.toJson()),
    );

    if (response.statusCode != 200) {
      throw Exception('Failed to deposit');
    }
  }

  Future<void> withdraw(String token, String accountId, WithdrawRequest request) async {
    final response = await http.post(
      Uri.parse('$baseUrl/$accountId/withdraw'),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
      body: jsonEncode(request.toJson()),
    );

    if (response.statusCode != 200) {
      throw Exception('Failed to withdraw');
    }
  }
}
