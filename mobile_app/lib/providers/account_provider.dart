import 'package:flutter/material.dart';
import '../api/account_service.dart';
import '../models/account.dart';
import '../models/transaction.dart';
import '../models/deposit_request.dart';
import '../models/withdraw_request.dart';
import '../models/login_response.dart';

class AccountProvider with ChangeNotifier {
  final AccountService _accountService = AccountService();
  Account? _account;
  List<Transaction> _transactions = [];
  bool _isLoading = false;
  AccountInfo? _selectedAccount;

  Account? get account => _account;
  List<Transaction> get transactions => _transactions;
  bool get isLoading => _isLoading;
  AccountInfo? get selectedAccount => _selectedAccount;

  void setSelectedAccount(AccountInfo account) {
    _selectedAccount = account;
    notifyListeners();
  }

  void setAccountsFromLoginResponse(LoginResponse loginResponse) {
    // Bu metod AuthProvider'dan gelen veriyi AccountProvider'a senkronize eder
    if (loginResponse.accounts.isNotEmpty) {
      _selectedAccount = loginResponse.accounts.first;
      notifyListeners();
    }
  }

  Future<void> fetchAccountData(String token, String accountId) async {
    _isLoading = true;
    notifyListeners();

    try {
      _account = await _accountService.getAccount(token, accountId);
      _transactions = await _accountService.getAccountTransactions(token, accountId);
    } catch (e) {
      // Handle error
      print('❌ Error fetching account data: $e');
    }

    _isLoading = false;
    notifyListeners();
  }

  Future<void> transfer(String token, String fromAccountId, String toAccountId, double amount) async {
    _isLoading = true;
    notifyListeners();

    try {
      final withdrawRequest = WithdrawRequest(amount: amount);
      await _accountService.withdraw(token, fromAccountId, withdrawRequest);

      final depositRequest = DepositRequest(amount: amount);
      await _accountService.deposit(token, toAccountId, depositRequest);

      // Refresh account data after transfer
      await fetchAccountData(token, fromAccountId);
    } catch (e) {
      // Handle error
      print('❌ Error during transfer: $e');
      rethrow;
    }

    _isLoading = false;
    notifyListeners();
  }

  Future<void> refreshTransactions(String token, String accountId) async {
    try {
      _transactions = await _accountService.getAccountTransactions(token, accountId);
      notifyListeners();
    } catch (e) {
      print('❌ Error refreshing transactions: $e');
    }
  }

  void clearData() {
    _account = null;
    _transactions = [];
    _selectedAccount = null;
    notifyListeners();
  }
}
