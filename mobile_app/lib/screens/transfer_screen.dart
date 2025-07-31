import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../providers/account_provider.dart';
import '../providers/auth_provider.dart';

class TransferScreen extends StatefulWidget {
  const TransferScreen({super.key});

  @override
  State<TransferScreen> createState() => _TransferScreenState();
}

class _TransferScreenState extends State<TransferScreen> {
  final _recipientIdController = TextEditingController();
  final _amountController = TextEditingController();

  Future<void> _transfer() async {
    final authProvider = Provider.of<AuthProvider>(context, listen: false);
    final accountProvider = Provider.of<AccountProvider>(context, listen: false);
    try {
      final fromAccountId = accountProvider.account!.id;
      final toAccountId = _recipientIdController.text;
      final amount = double.parse(_amountController.text);

      await accountProvider.transfer(
        authProvider.token!,
        fromAccountId,
        toAccountId,
        amount,
      );

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Transfer successful!')),
        );
        Navigator.pop(context);
      }
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Transfer failed: $e')),
        );
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Transfer'),
      ),
      body: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          children: [
            TextField(
              controller: _recipientIdController,
              decoration: const InputDecoration(
                labelText: 'Recipient Account ID',
              ),
            ),
            const SizedBox(height: 16.0),
            TextField(
              controller: _amountController,
              keyboardType: TextInputType.number,
              decoration: const InputDecoration(
                labelText: 'Amount',
              ),
            ),
            const SizedBox(height: 32.0),
            ElevatedButton(
              onPressed: _transfer,
              child: const Text('Transfer'),
            ),
          ],
        ),
      ),
    );
  }
}
