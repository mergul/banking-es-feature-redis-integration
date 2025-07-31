import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../providers/account_provider.dart';
import '../providers/auth_provider.dart';
import '../models/login_response.dart';
import '../api/auth_service.dart';
import '../api/account_service.dart';
import 'transfer_screen.dart';
import 'transaction_history_screen.dart';

class DashboardScreen extends StatefulWidget {
  const DashboardScreen({super.key});

  @override
  State<DashboardScreen> createState() => _DashboardScreenState();
}

class _DashboardScreenState extends State<DashboardScreen> {
  LoginResponse? loginResponse;
  AccountInfo? selectedAccount;
  bool isLoading = true;
  final AuthService _authService = AuthService();
  final AccountService _accountService = AccountService();

  @override
  void initState() {
    super.initState();
    _loadUserAccounts();
  }

  Future<void> _loadUserAccounts() async {
    try {
      final authProvider = Provider.of<AuthProvider>(context, listen: false);
      final username = authProvider.username ?? '';

      if (username.isNotEmpty) {
        final response = await _authService.getUserAccounts(username);

        setState(() {
          loginResponse = response;
          selectedAccount = response.accounts.isNotEmpty ? response.accounts.firstWhere((acc) => acc.isPrimary, orElse: () => response.accounts.first) : null;
          isLoading = false;
        });

        if (selectedAccount != null) {
          Provider.of<AccountProvider>(context, listen: false).fetchAccountData(authProvider.token!, selectedAccount!.id);
        }

      } else {
        setState(() {
          isLoading = false;
        });
      }
    } catch (e) {
      print('❌ Error loading accounts: $e');
      setState(() {
        isLoading = false;
      });
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to load accounts: $e')),
      );
    }
  }

  void _selectAccount(AccountInfo account) {
    setState(() {
      selectedAccount = account;
    });
    final authProvider = Provider.of<AuthProvider>(context, listen: false);
    Provider.of<AccountProvider>(context, listen: false).fetchAccountData(authProvider.token!, selectedAccount!.id);
  }

  @override
  Widget build(BuildContext context) {
    final authProvider = Provider.of<AuthProvider>(context);
    final accountProvider = Provider.of<AccountProvider>(context);

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
      body: isLoading
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
            onRefresh: _loadUserAccounts,
            child: SingleChildScrollView(
                child: Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        'Welcome, ${authProvider.username ?? ''}',
                        style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 24.0),

                      // Account Selection
                      const Text(
                        'Your Accounts',
                        style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 8.0),
                      if (loginResponse?.accounts.isNotEmpty == true)
                        SizedBox(
                          height: 130,
                          child: ListView.builder(
                            scrollDirection: Axis.horizontal,
                            itemCount: loginResponse!.accounts.length,
                            itemBuilder: (context, index) {
                              final account = loginResponse!.accounts[index];
                              final isSelected = selectedAccount?.id == account.id;

                              return GestureDetector(
                                onTap: () => _selectAccount(account),
                                child: Card(
                                  elevation: isSelected ? 8 : 2,
                                  shape: RoundedRectangleBorder(
                                    borderRadius: BorderRadius.circular(10),
                                    side: isSelected ? const BorderSide(color: Colors.blue, width: 2) : BorderSide.none,
                                  ),
                                  child: Container(
                                    width: 220,
                                    padding: const EdgeInsets.all(16.0),
                                    child: Column(
                                      crossAxisAlignment: CrossAxisAlignment.start,
                                      children: [
                                        Row(
                                          mainAxisAlignment: MainAxisAlignment.spaceBetween,
                                          children: [
                                            Text(
                                              'Account ${index + 1}',
                                              style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 16),
                                            ),
                                            if (account.isPrimary)
                                              const Icon(Icons.star, color: Colors.amber, size: 16),
                                          ],
                                        ),
                                        const SizedBox(height: 8),
                                        Text(
                                          '\$${account.balance}',
                                          style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
                                        ),
                                        const SizedBox(height: 4),
                                        Text(
                                          'Status: ${account.isActive ? "Active" : "Inactive"}',
                                          style: TextStyle(
                                            color: account.isActive ? Colors.green : Colors.red,
                                            fontSize: 12,
                                          ),
                                        ),
                                      ],
                                    ),
                                  ),
                                ),
                              );
                            },
                          ),
                        )
                      else
                        const Text("You don't have any accounts yet."),

                      const SizedBox(height: 24.0),

                      // Action Buttons
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceAround,
                        children: [
                          _buildActionButton(Icons.send, 'Transfer', () {
                             if(selectedAccount?.isActive == true) {
                                Navigator.of(context).push(MaterialPageRoute(
                                  builder: (context) => const TransferScreen(),
                                ));
                             }
                          }),
                          _buildActionButton(Icons.history, 'History', () {
                             if(selectedAccount != null) {
                               Navigator.of(context).push(MaterialPageRoute(
                                  builder: (context) => const TransactionHistoryScreen(),
                                ));
                             }
                          }),
                           _buildActionButton(Icons.add, 'Add Account', () async {
                              try {
                                final username = authProvider.username ?? '';
                                final token = authProvider.token ?? '';
                                await _accountService.createAdditionalAccount(token, username, 500.0);
                                await _loadUserAccounts();
                                ScaffoldMessenger.of(context).showSnackBar(
                                  const SnackBar(content: Text('Additional account created successfully!')),
                                );
                              } catch (e) {
                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(content: Text('Failed to create account: $e')),
                                );
                              }
                           }),
                        ],
                      ),

                      const SizedBox(height: 24.0),

                      // Recent Transactions
                      const Text(
                        'Recent Transactions',
                        style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 8.0),
                      accountProvider.isLoading
                        ? const Center(child: CircularProgressIndicator())
                        : accountProvider.transactions.isEmpty
                            ? const Text("No recent transactions.")
                            : ListView.builder(
                                shrinkWrap: true,
                                physics: const NeverScrollableScrollPhysics(),
                                itemCount: accountProvider.transactions.length > 5 ? 5 : accountProvider.transactions.length,
                                itemBuilder: (ctx, i) {
                                  final transaction = accountProvider.transactions[i];
                                  return Card(
                                    margin: const EdgeInsets.symmetric(vertical: 4),
                                    child: ListTile(
                                      leading: Icon(
                                        transaction.transactionType == 'deposit'
                                            ? Icons.arrow_downward
                                            : Icons.arrow_upward,
                                        color: transaction.transactionType == 'deposit'
                                            ? Colors.green
                                            : Colors.red,
                                      ),
                                      title: Text(
                                        '${transaction.transactionType} - \$${transaction.amount.toStringAsFixed(2)}',
                                      ),
                                      subtitle: Text(
                                        transaction.timestamp.toIso8601String(),
                                      ),
                                    ),
                                  );
                                },
                              ),
                    ],
                  ),
                ),
              ),
          ),
    );
  }

  Widget _buildActionButton(IconData icon, String label, VoidCallback? onPressed) {
    return Column(
      children: [
        IconButton.filled(
          icon: Icon(icon),
          onPressed: onPressed,
          iconSize: 32,
          style: IconButton.styleFrom(
            backgroundColor: Colors.blue.shade100,
            foregroundColor: Colors.blue.shade800,
            padding: const EdgeInsets.all(16)
          ),
        ),
        const SizedBox(height: 4),
        Text(label, style: const TextStyle(fontSize: 12)),
      ],
    );
  }
}
