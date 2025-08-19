import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../providers/account_provider.dart';
import '../providers/auth_provider.dart';
import '../models/login_response.dart';
import '../api/auth_service.dart';
import '../api/account_service.dart';
import '../api/websocket_service.dart';
import 'transfer_screen.dart';
import 'transaction_history_screen.dart';

class DashboardScreen extends StatefulWidget {
  const DashboardScreen({super.key});

  @override
  State<DashboardScreen> createState() => _DashboardScreenState();
}

class _DashboardScreenState extends State<DashboardScreen> 
    with TickerProviderStateMixin {
  LoginResponse? loginResponse;
  AccountInfo? selectedAccount;
  bool isLoading = true;
  bool isRefreshing = false;
  final AuthService _authService = AuthService();
  final AccountService _accountService = AccountService();
  
  late AnimationController _fadeController;
  late AnimationController _slideController;
  late Animation<double> _fadeAnimation;
  late Animation<Offset> _slideAnimation;

  @override
  void initState() {
    super.initState();
    _initializeAnimations();
    _loadUserAccounts();
    _setupWebSocketListener();
  }

  void _initializeAnimations() {
    _fadeController = AnimationController(
      duration: const Duration(milliseconds: 800),
      vsync: this,
    );
    _slideController = AnimationController(
      duration: const Duration(milliseconds: 600),
      vsync: this,
    );
    
    _fadeAnimation = Tween<double>(
      begin: 0.0,
      end: 1.0,
    ).animate(CurvedAnimation(
      parent: _fadeController,
      curve: Curves.easeInOut,
    ));
    
    _slideAnimation = Tween<Offset>(
      begin: const Offset(0, 0.3),
      end: Offset.zero,
    ).animate(CurvedAnimation(
      parent: _slideController,
      curve: Curves.easeOutCubic,
    ));
  }

  @override
  void dispose() {
    _fadeController.dispose();
    _slideController.dispose();
    super.dispose();
  }

  // Mock data oluştur
  LoginResponse _createMockData(String username) {
    return LoginResponse(
      message: 'Login successful',
      token: {'access_token': 'mock_token'},
      username: username,
      accounts: [
        AccountInfo(
          id: '1',
          balance: '2500.00',
          isActive: true,
          isPrimary: true,
          createdAt: '2024-01-15',
        ),
        AccountInfo(
          id: '2',
          balance: '1500.75',
          isActive: true,
          isPrimary: false,
          createdAt: '2024-01-20',
        ),
        AccountInfo(
          id: '3',
          balance: '500.25',
          isActive: false,
          isPrimary: false,
          createdAt: '2024-01-25',
        ),
      ],
      primaryAccountId: '1',
    );
  }

  Future<void> _loadUserAccounts() async {
    if (isRefreshing) return;
    
    setState(() {
      if (!isLoading) isRefreshing = true;
    });

    try {
      final authProvider = Provider.of<AuthProvider>(context, listen: false);
      
      // Önce AuthProvider'dan mevcut veriyi kontrol et
      if (authProvider.loginResponse != null && authProvider.loginResponse!.accounts.isNotEmpty) {
        print('📊 Using existing login response data');
        setState(() {
          loginResponse = authProvider.loginResponse;
          selectedAccount = authProvider.loginResponse!.accounts.first;
          isLoading = false;
          isRefreshing = false;
        });
        
        // Animasyonları başlat
        _fadeController.forward();
        _slideController.forward();
        return;
      }
      
      // Eğer AuthProvider'da veri yoksa, API'den yüklemeyi dene
      final username = authProvider.username ?? '';
      
      if (username.isNotEmpty) {
        print('🔄 Loading accounts from API for user: $username');
        try {
          final response = await _authService.getUserAccounts(username);
          
          // Eğer API'den gerçek veri geldiyse kullan
          if (response.accounts.isNotEmpty) {
            print('✅ API Response received with ${response.accounts.length} accounts');
            
            // AuthProvider'ı da güncelle
            await authProvider.refreshUserAccounts();
            
            setState(() {
              loginResponse = response;
              selectedAccount = response.accounts.first;
              isLoading = false;
              isRefreshing = false;
            });
            
            print('✅ Using real API data');
          } else {
            // API'den boş veri geldiyse mock data kullan
            print('⚠️ API returned empty accounts, using mock data');
            final mockResponse = _createMockData(username);
            
            setState(() {
              loginResponse = mockResponse;
              selectedAccount = mockResponse.accounts.first;
              isLoading = false;
              isRefreshing = false;
            });
            
            print('✅ Using mock data with ${mockResponse.accounts.length} accounts');
          }
        } catch (e) {
          print('⚠️ API failed, using mock data: $e');
          // API başarısız olursa mock data kullan
          final mockResponse = _createMockData(username);
          
          setState(() {
            loginResponse = mockResponse;
            selectedAccount = mockResponse.accounts.first;
            isLoading = false;
            isRefreshing = false;
          });
          
          print('✅ Using mock data after API error');
        }
        
        // Animasyonları başlat
        _fadeController.forward();
        _slideController.forward();
      } else {
        print('⚠️ Username is empty, using mock data');
        // Username boşsa mock data kullan
        final mockResponse = _createMockData('Demo User');
        
        setState(() {
          loginResponse = mockResponse;
          selectedAccount = mockResponse.accounts.first;
          isLoading = false;
          isRefreshing = false;
        });
        
        print('✅ Using mock data for demo user');
        
        // Animasyonları başlat
        _fadeController.forward();
        _slideController.forward();
      }
    } catch (e) {
      print('❌ Error loading user accounts: $e');
      // Hata durumunda da mock data kullan
      final mockResponse = _createMockData('Demo User');
      
      setState(() {
        loginResponse = mockResponse;
        selectedAccount = mockResponse.accounts.first;
        isLoading = false;
        isRefreshing = false;
      });
      
      print('✅ Using mock data after error');
      
      // Animasyonları başlat
      _fadeController.forward();
      _slideController.forward();
    }
  }

  void _selectAccount(AccountInfo account) {
    setState(() {
      selectedAccount = account;
    });
    
    // Seçim animasyonu
    _slideController.reset();
    _slideController.forward();
  }

  void _showErrorSnackBar(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Row(
          children: [
            const Icon(Icons.error_outline, color: Colors.white),
            const SizedBox(width: 8),
            Expanded(child: Text(message)),
          ],
        ),
        backgroundColor: Colors.red.shade600,
        behavior: SnackBarBehavior.floating,
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        margin: const EdgeInsets.all(16),
      ),
    );
  }

  void _showSuccessSnackBar(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Row(
          children: [
            const Icon(Icons.check_circle_outline, color: Colors.white),
            const SizedBox(width: 8),
            Expanded(child: Text(message)),
          ],
        ),
        backgroundColor: Colors.green.shade600,
        behavior: SnackBarBehavior.floating,
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        margin: const EdgeInsets.all(16),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final authProvider = Provider.of<AuthProvider>(context);

    return Scaffold(
      backgroundColor: Colors.grey.shade50,
      appBar: AppBar(
        title: const Text('Ana Sayfa'),
        elevation: 0,
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: _loadUserAccounts,
          ),
          IconButton(
            icon: const Icon(Icons.logout),
            onPressed: () => _showLogoutDialog(context, authProvider),
          ),
        ],
      ),
      body: isLoading
          ? _buildLoadingState()
          : RefreshIndicator(
              onRefresh: _loadUserAccounts,
              child: FadeTransition(
                opacity: _fadeAnimation,
                child: SlideTransition(
                  position: _slideAnimation,
                  child: SingleChildScrollView(
                    padding: const EdgeInsets.all(16.0),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        _buildWelcomeCard(authProvider),
                        const SizedBox(height: 24),
                        
                        if (loginResponse?.accounts.isNotEmpty == true) ...[
                          _buildAccountsSection(),
                          const SizedBox(height: 24),
                        ],
                        
                        if (selectedAccount != null) ...[
                          _buildSelectedAccountCard(),
                          const SizedBox(height: 24),
                        ],
                        
                        _buildActionButtons(),
                        const SizedBox(height: 24),
                        
                        _buildAddAccountButton(authProvider),
                      ],
                    ),
                  ),
                ),
              ),
            ),
    );
  }

  Widget _buildLoadingState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Container(
            width: 80,
            height: 80,
            decoration: BoxDecoration(
              color: const Color(0xFF3F51B5).withOpacity(0.1),
              borderRadius: BorderRadius.circular(40),
            ),
            child: const CircularProgressIndicator(
              valueColor: AlwaysStoppedAnimation<Color>(Color(0xFF3F51B5)),
              strokeWidth: 3,
            ),
          ),
          const SizedBox(height: 16),
          const Text(
            'Hesaplarınız yükleniyor...',
            style: TextStyle(
              fontSize: 16,
              color: Color(0xFF3F51B5),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildWelcomeCard(AuthProvider authProvider) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        gradient: const LinearGradient(
          colors: [Color(0xFF3F51B5), Color(0xFF5C6BC0)],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.1),
            blurRadius: 10,
            offset: const Offset(0, 5),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Container(
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.white.withOpacity(0.2),
                  borderRadius: BorderRadius.circular(12),
                ),
                child: const Icon(
                  Icons.person,
                  color: Colors.white,
                  size: 24,
                ),
              ),
              const SizedBox(width: 16),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    const Text(
                      'Hoş Geldiniz',
                      style: TextStyle(
                        color: Colors.white70,
                        fontSize: 14,
                      ),
                    ),
                    Text(
                      authProvider.username ?? 'Demo User',
                      style: const TextStyle(
                        color: Colors.white,
                        fontSize: 20,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ],
                ),
              ),
              if (isRefreshing)
                const SizedBox(
                  width: 20,
                  height: 20,
                  child: CircularProgressIndicator(
                    strokeWidth: 2,
                    valueColor: AlwaysStoppedAnimation<Color>(Colors.white),
                  ),
                ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildAccountsSection() {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Text(
          'Hesaplarınız',
          style: TextStyle(
            fontSize: 20,
            fontWeight: FontWeight.bold,
            color: Color(0xFF3F51B5),
          ),
        ),
        const SizedBox(height: 16),
        SizedBox(
          height: 140,
          child: ListView.builder(
            scrollDirection: Axis.horizontal,
            itemCount: loginResponse!.accounts.length,
            itemBuilder: (context, index) {
              final account = loginResponse!.accounts[index];
              final isSelected = selectedAccount?.id == account.id;
              
              return GestureDetector(
                onTap: () => _selectAccount(account),
                child: AnimatedContainer(
                  duration: const Duration(milliseconds: 300),
                  width: 220,
                  margin: const EdgeInsets.only(right: 16.0),
                  padding: const EdgeInsets.all(16.0),
                  decoration: BoxDecoration(
                    color: isSelected ? const Color(0xFF3F51B5) : Colors.white,
                    borderRadius: BorderRadius.circular(16.0),
                    border: isSelected 
                        ? null
                        : Border.all(color: Colors.grey.shade300),
                    boxShadow: [
                      BoxShadow(
                        color: Colors.black.withOpacity(0.05),
                        blurRadius: 10,
                        offset: const Offset(0, 5),
                      ),
                    ],
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Row(
                        children: [
                          Icon(
                            Icons.account_balance_wallet,
                            color: isSelected ? Colors.white : const Color(0xFF3F51B5),
                            size: 20,
                          ),
                          const SizedBox(width: 8),
                          Text(
                            'Hesap ${index + 1}',
                            style: TextStyle(
                              fontWeight: FontWeight.bold,
                              color: isSelected ? Colors.white : Colors.black87,
                            ),
                          ),
                          const Spacer(),
                          if (account.isPrimary)
                            Container(
                              padding: const EdgeInsets.symmetric(horizontal: 8.0, vertical: 4.0),
                              decoration: BoxDecoration(
                                color: isSelected ? Colors.white.withOpacity(0.2) : Colors.green,
                                borderRadius: BorderRadius.circular(12.0),
                              ),
                              child: Text(
                                'Ana',
                                style: TextStyle(
                                  color: isSelected ? Colors.white : Colors.white,
                                  fontSize: 10,
                                  fontWeight: FontWeight.bold,
                                ),
                              ),
                            ),
                        ],
                      ),
                      const SizedBox(height: 12.0),
                      Text(
                        '\$${account.balance}',
                        style: TextStyle(
                          fontSize: 20,
                          fontWeight: FontWeight.bold,
                          color: isSelected ? Colors.white : const Color(0xFF3F51B5),
                        ),
                      ),
                      const SizedBox(height: 4.0),
                      Row(
                        children: [
                          Container(
                            width: 8,
                            height: 8,
                            decoration: BoxDecoration(
                              color: account.isActive ? Colors.green : Colors.red,
                              shape: BoxShape.circle,
                            ),
                          ),
                          const SizedBox(width: 6),
                          Text(
                            account.isActive ? 'Aktif' : 'Pasif',
                            style: TextStyle(
                              color: isSelected ? Colors.white70 : (account.isActive ? Colors.green : Colors.red),
                              fontSize: 12,
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              );
            },
          ),
        ),
      ],
    );
  }

  Widget _buildSelectedAccountCard() {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 10,
            offset: const Offset(0, 5),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Container(
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: const Color(0xFF3F51B5).withOpacity(0.1),
                  borderRadius: BorderRadius.circular(12),
                ),
                child: const Icon(
                  Icons.account_balance,
                  color: Color(0xFF3F51B5),
                  size: 24,
                ),
              ),
              const SizedBox(width: 16),
              const Expanded(
                child: Text(
                  'Seçili Hesap',
                  style: TextStyle(
                    fontSize: 18,
                    fontWeight: FontWeight.bold,
                    color: Color(0xFF3F51B5),
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 16),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  const Text(
                    'Bakiye',
                    style: TextStyle(
                      color: Colors.grey,
                      fontSize: 14,
                    ),
                  ),
                  Text(
                    '\$${selectedAccount!.balance}',
                    style: const TextStyle(
                      fontSize: 24,
                      fontWeight: FontWeight.bold,
                      color: Color(0xFF3F51B5),
                    ),
                  ),
                ],
              ),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                decoration: BoxDecoration(
                  color: selectedAccount!.isActive ? Colors.green : Colors.red,
                  borderRadius: BorderRadius.circular(20),
                ),
                child: Text(
                  selectedAccount!.isActive ? 'Aktif' : 'Pasif',
                  style: const TextStyle(
                    color: Colors.white,
                    fontSize: 12,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildActionButtons() {
    return Row(
      children: [
        Expanded(
          child: _buildActionButton(
            icon: Icons.swap_horiz,
            title: 'Transfer',
            subtitle: 'Para Transferi',
            color: const Color(0xFF4CAF50),
            onTap: selectedAccount?.isActive == true ? () {
              Navigator.of(context).push(MaterialPageRoute(
                builder: (context) => const TransferScreen(),
              ));
            } : null,
          ),
        ),
        const SizedBox(width: 16),
        Expanded(
          child: _buildActionButton(
            icon: Icons.history,
            title: 'Geçmiş',
            subtitle: 'İşlem Geçmişi',
            color: const Color(0xFF2196F3),
            onTap: selectedAccount != null ? () {
              Navigator.of(context).push(MaterialPageRoute(
                builder: (context) => const TransactionHistoryScreen(),
              ));
            } : null,
          ),
        ),
      ],
    );
  }

  Widget _buildAddAccountButton(AuthProvider authProvider) {
    return SizedBox(
      width: double.infinity,
      height: 56,
      child: ElevatedButton.icon(
        onPressed: () async {
          try {
            // Mock hesap ekleme
            await Future.delayed(const Duration(seconds: 1));
            
            // Yeni mock hesap oluştur
            final newAccount = AccountInfo(
              id: DateTime.now().millisecondsSinceEpoch.toString(),
              balance: '750.00',
              isActive: true,
              isPrimary: false,
              createdAt: DateTime.now().toIso8601String(),
            );
            
            setState(() {
              loginResponse!.accounts.add(newAccount);
              selectedAccount = newAccount;
            });
            
            if (mounted) {
              _showSuccessSnackBar('Yeni hesap başarıyla oluşturuldu!');
            }
          } catch (e) {
            if (mounted) {
              _showErrorSnackBar('Hesap oluşturulamadı: $e');
            }
          }
        },
        icon: const Icon(Icons.add),
        label: const Text(
          'Yeni Hesap Ekle',
          style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
        ),
        style: ElevatedButton.styleFrom(
          backgroundColor: const Color(0xFF3F51B5),
          foregroundColor: Colors.white,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(12),
          ),
        ),
      ),
    );
  }

  Widget _buildActionButton({
    required IconData icon,
    required String title,
    required String subtitle,
    required Color color,
    required VoidCallback? onTap,
  }) {
    return GestureDetector(
      onTap: onTap,
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 200),
        padding: const EdgeInsets.all(20),
        decoration: BoxDecoration(
          color: onTap != null ? Colors.white : Colors.grey.shade100,
          borderRadius: BorderRadius.circular(16),
          boxShadow: onTap != null ? [
            BoxShadow(
              color: Colors.black.withOpacity(0.05),
              blurRadius: 10,
              offset: const Offset(0, 5),
            ),
          ] : null,
        ),
        child: Column(
          children: [
            Container(
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: onTap != null ? color.withOpacity(0.1) : Colors.grey.withOpacity(0.1),
                borderRadius: BorderRadius.circular(12),
              ),
              child: Icon(
                icon,
                color: onTap != null ? color : Colors.grey,
                size: 24,
              ),
            ),
            const SizedBox(height: 12),
            Text(
              title,
              style: TextStyle(
                fontSize: 16,
                fontWeight: FontWeight.bold,
                color: onTap != null ? Colors.black87 : Colors.grey,
              ),
            ),
            const SizedBox(height: 4),
            Text(
              subtitle,
              style: TextStyle(
                fontSize: 12,
                color: onTap != null ? Colors.grey.shade600 : Colors.grey,
              ),
            ),
          ],
        ),
      ),
    );
  }

  void _showLogoutDialog(BuildContext context, AuthProvider authProvider) {
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        title: const Row(
          children: [
            Icon(Icons.logout, color: Color(0xFF3F51B5)),
            SizedBox(width: 8),
            Text('Çıkış Yap'),
          ],
        ),
        content: const Text('Çıkış yapmak istediğinizden emin misiniz?'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('İptal'),
          ),
          ElevatedButton(
            onPressed: () {
              Navigator.pop(context);
              authProvider.logout();
            },
            style: ElevatedButton.styleFrom(
              backgroundColor: const Color(0xFF3F51B5),
              foregroundColor: Colors.white,
            ),
            child: const Text('Çıkış Yap'),
          ),
        ],
      ),
    );
  }

  void _setupWebSocketListener() {
    final authProvider = Provider.of<AuthProvider>(context, listen: false);
    final websocketService = authProvider.websocketService;
    
    websocketService.messageStream?.listen((message) {
      print('📨 WebSocket message received in dashboard: ${message.type}');
      
      switch (message.type) {
        case 'ProjectionUpdated':
          _handleProjectionUpdate(message.data);
          break;
        case 'AccountCreated':
          _handleAccountCreated(message.data);
          break;
        default:
          print('📨 Unknown WebSocket message type: ${message.type}');
      }
    });
  }

  void _handleProjectionUpdate(Map<String, dynamic> data) {
    print('📊 Handling projection update: $data');
    // Refresh accounts when projection is updated
    _loadUserAccounts();
  }

  void _handleAccountCreated(Map<String, dynamic> data) {
    print('🆕 Handling account created: $data');
    // Show notification and refresh accounts
    _showAccountCreatedNotification(data);
    _loadUserAccounts();
  }

  void _showAccountCreatedNotification(Map<String, dynamic> data) {
    final accountId = data['account_id'] ?? '';
    final balance = data['initial_balance'] ?? '';
    
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('Yeni hesap oluşturuldu! Hesap No: $accountId, Bakiye: \$$balance'),
        backgroundColor: Colors.green,
        duration: const Duration(seconds: 5),
      ),
    );
  }
}
