import 'package:flutter/material.dart';
import '../api/account_service.dart';
import '../models/account.dart';
import '../models/transaction.dart';
import '../models/deposit_request.dart';
import '../models/withdraw_request.dart';

class AccountProvider with ChangeNotifier {
  final AccountService _accountService = AccountService();
  Account? _account;
  List<Transaction> _transactions = [];
  bool _isLoading = false;

  Account? get account => _account;
  List<Transaction> get transactions => _transactions;
  bool get isLoading => _isLoading;

  Future<void> fetchAccountData(String token, String accountId) async {
    _isLoading = true;
    notifyListeners();

    try {
      _account = await _accountService.getAccount(token, accountId);
      _transactions = await _accountService.getAccountTransactions(token, accountId);
    } catch (e) {
      // Handle error
      print(e);
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
      print(e);
    }

    _isLoading = false;
    notifyListeners();
  }
}
