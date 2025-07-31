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
      
      print('🔍 Loading accounts for username: "$username"');
      print('🔐 AuthProvider username: "${authProvider.username}"');
      print('🔑 AuthProvider token: "${authProvider.token?.substring(0, 20)}..."');
      print('✅ AuthProvider isAuthenticated: ${authProvider.isAuthenticated}');
      
      if (username.isNotEmpty) {
        print('📡 Making API call to get user accounts...');
        final response = await _authService.getUserAccounts(username);
        print('✅ API response received: ${response.accounts.length} accounts');
        
        setState(() {
          loginResponse = response;
          selectedAccount = response.accounts.isNotEmpty ? response.accounts.first : null;
          isLoading = false;
        });
        
        print('📊 Selected account: ${selectedAccount?.balance}');
      } else {
        print('⚠️ Username is empty');
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
  }

  @override
  Widget build(BuildContext context) {
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
      body: isLoading
          ? const Center(child: CircularProgressIndicator())
          : Padding(
              padding: const EdgeInsets.all(16.0),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Welcome, ${authProvider.username ?? ''}',
                    style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                  ),
                  const SizedBox(height: 16.0),
                  
                  // Account Selection
                  if (loginResponse?.accounts.isNotEmpty == true) ...[
                    const Text(
                      'Select Account:',
                      style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
                    ),
                    const SizedBox(height: 8.0),
                    Container(
                      height: 120,
                      child: ListView.builder(
                        scrollDirection: Axis.horizontal,
                        itemCount: loginResponse!.accounts.length,
                        itemBuilder: (context, index) {
                          final account = loginResponse!.accounts[index];
                          final isSelected = selectedAccount?.id == account.id;
                          
                          return GestureDetector(
                            onTap: () => _selectAccount(account),
                            child: Container(
                              width: 200,
                              margin: const EdgeInsets.only(right: 12.0),
                              padding: const EdgeInsets.all(12.0),
                              decoration: BoxDecoration(
                                color: isSelected ? Colors.blue.shade100 : Colors.grey.shade100,
                                borderRadius: BorderRadius.circular(8.0),
                                border: isSelected 
                                    ? Border.all(color: Colors.blue, width: 2)
                                    : null,
                              ),
                              child: Column(
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  Row(
                                    children: [
                                      Text(
                                        'Account ${index + 1}',
                                        style: const TextStyle(fontWeight: FontWeight.bold),
                                      ),
                                      if (account.isPrimary) ...[
                                        const SizedBox(width: 8.0),
                                        Container(
                                          padding: const EdgeInsets.symmetric(horizontal: 6.0, vertical: 2.0),
                                          decoration: BoxDecoration(
                                            color: Colors.green,
                                            borderRadius: BorderRadius.circular(4.0),
                                          ),
                                          child: const Text(
                                            'Primary',
                                            style: TextStyle(color: Colors.white, fontSize: 10),
                                          ),
                                        ),
                                      ],
                                    ],
                                  ),
                                  const SizedBox(height: 4.0),
                                  Text(
                                    'Balance: \$${account.balance}',
                                    style: const TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
                                  ),
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
                          );
                        },
                      ),
                    ),
                    const SizedBox(height: 16.0),
                  ],
                  
                  // Selected Account Info
                  if (selectedAccount != null) ...[
                    Container(
                      padding: const EdgeInsets.all(16.0),
                      decoration: BoxDecoration(
                        color: Colors.blue.shade50,
                        borderRadius: BorderRadius.circular(8.0),
                      ),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            'Selected Account',
                            style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                          ),
                          const SizedBox(height: 8.0),
                          Text(
                            'Balance: \$${selectedAccount!.balance}',
                            style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
                          ),
                          Text(
                            'Status: ${selectedAccount!.isActive ? "Active" : "Inactive"}',
                            style: TextStyle(
                              color: selectedAccount!.isActive ? Colors.green : Colors.red,
                            ),
                          ),
                        ],
                      ),
                    ),
                    const SizedBox(height: 32.0),
                  ],
                  
                  // Action Buttons
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                    children: [
                      ElevatedButton(
                        onPressed: selectedAccount?.isActive == true ? () {
                          Navigator.of(context).push(MaterialPageRoute(
                            builder: (context) => const TransferScreen(),
                          ));
                        } : null,
                        child: const Text('Transfer'),
                      ),
                      ElevatedButton(
                        onPressed: selectedAccount != null ? () {
                          Navigator.of(context).push(MaterialPageRoute(
                            builder: (context) => const TransactionHistoryScreen(),
                          ));
                        } : null,
                        child: const Text('History'),
                      ),
                    ],
                  ),
                  
                  const SizedBox(height: 16.0),
                  
                  // Add Account Button
                  ElevatedButton.icon(
                    onPressed: () async {
                      try {
                        final username = authProvider.username ?? '';
                        await _accountService.createAdditionalAccount(token, username, 500.0);
                        await _loadUserAccounts(); // Reload accounts
                        ScaffoldMessenger.of(context).showSnackBar(
                          const SnackBar(content: Text('Additional account created successfully!')),
                        );
                      } catch (e) {
                        ScaffoldMessenger.of(context).showSnackBar(
                          SnackBar(content: Text('Failed to create account: $e')),
                        );
                      }
                    },
                    icon: const Icon(Icons.add),
                    label: const Text('Add Account'),
                  ),
                ],
              ),
            ),
    );
  }
}
