import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:mobile_app/api/account_service.dart';
import 'package:mobile_app/models/account.dart';
import 'package:mobile_app/models/transaction.dart';
import 'package:mobile_app/providers/account_provider.dart';

class MockAccountService extends Mock implements AccountService {}

void main() {
  late AccountProvider accountProvider;
  late MockAccountService mockAccountService;

  setUp(() {
    mockAccountService = MockAccountService();
    accountProvider = AccountProvider();
  });

  group('AccountProvider', () {
    test('fetchAccountData success', () async {
      final account = Account(id: '1', ownerName: 'test', balance: 100.0);
      final transactions = [
        Transaction(id: '1', amount: 50.0, transactionType: 'deposit', timestamp: DateTime.now()),
      ];

      when(() => mockAccountService.getAccount(any(), any())).thenAnswer((_) async => account);
      when(() => mockAccountService.getAccountTransactions(any(), any())).thenAnswer((_) async => transactions);

      await accountProvider.fetchAccountData('token', '1');

      expect(accountProvider.account, account);
      expect(accountProvider.transactions, transactions);
    });
  });
}
