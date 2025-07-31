import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../providers/account_provider.dart';
import '../providers/auth_provider.dart';
import 'transfer_screen.dart';
import 'transaction_history_screen.dart';

class DashboardScreen extends StatefulWidget {
  const DashboardScreen({super.key});

  @override
  State<DashboardScreen> createState() => _DashboardScreenState();
}

class _DashboardScreenState extends State<DashboardScreen> {
  @override
  void initState() {
    super.initState();
    // TODO: Replace with actual account ID from user data
    final accountId = 'some-account-id';
    final authProvider = Provider.of<AuthProvider>(context, listen: false);
    Provider.of<AccountProvider>(context, listen: false)
        .fetchAccountData(authProvider.token!, accountId);
  }

  @override
  Widget build(BuildContext context) {
    final accountProvider = Provider.of<AccountProvider>(context);
    final authProvider = Provider.of<AuthProvider>(context);

    return Scaffold(
      appBar: AppBar(
        title: const Text('Dashboard'),
        actions: [
          IconButton(
            icon: const Icon(Icons.logout),
            onPressed: () {
              authProvider.logout();
            },
          ),
        ],
      ),
      body: accountProvider.isLoading
          ? const Center(child: CircularProgressIndicator())
          : Padding(
              padding: const EdgeInsets.all(16.0),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Welcome, ${accountProvider.account?.ownerName ?? ''}',
                    style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                  ),
                  const SizedBox(height: 16.0),
                  Text(
                    'Balance: \$${accountProvider.account?.balance.toStringAsFixed(2) ?? '0.00'}',
                    style: const TextStyle(fontSize: 20),
                  ),
                  const SizedBox(height: 32.0),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                    children: [
                      ElevatedButton(
                        onPressed: () {
                          Navigator.of(context).push(MaterialPageRoute(
                            builder: (context) => const TransferScreen(),
                          ));
                        },
                        child: const Text('Transfer'),
                      ),
                      ElevatedButton(
                        onPressed: () {
                          Navigator.of(context).push(MaterialPageRoute(
                            builder: (context) => const TransactionHistoryScreen(),
                          ));
                        },
                        child: const Text('History'),
                      ),
                    ],
                  ),
                  const SizedBox(height: 32.0),
                  const Text(
                    'Recent Transactions',
                    style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                  ),
                  Expanded(
                    child: ListView.builder(
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
                  ),
                ],
              ),
            ),
    );
  }
}
