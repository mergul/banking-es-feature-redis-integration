import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../providers/account_provider.dart';

class TransactionHistoryScreen extends StatelessWidget {
  const TransactionHistoryScreen({super.key});

  @override
  Widget build(BuildContext context) {
    final accountProvider = Provider.of<AccountProvider>(context);

    return Scaffold(
      appBar: AppBar(
        title: const Text('Transaction History'),
      ),
      body: ListView.builder(
        itemCount: accountProvider.transactions.length,
        itemBuilder: (ctx, i) => ListTile(
          leading: Icon(
            accountProvider.transactions[i].transactionType == 'deposit'
                ? Icons.arrow_downward
                : Icons.arrow_upward,
            color: accountProvider.transactions[i].transactionType == 'deposit'
                ? Colors.green
                : Colors.red,
          ),
          title: Text(
            '${accountProvider.transactions[i].transactionType} - \$${accountProvider.transactions[i].amount.toStringAsFixed(2)}',
          ),
          subtitle: Text(
            accountProvider.transactions[i].timestamp.toIso8601String(),
          ),
        ),
      ),
    );
  }
}
