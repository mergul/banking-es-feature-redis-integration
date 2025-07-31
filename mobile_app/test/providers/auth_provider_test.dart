import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:mobile_app/api/auth_service.dart';
import 'package:mobile_app/models/login_request.dart';
import 'package:mobile_app/models/login_response.dart';
import 'package:mobile_app/providers/auth_provider.dart';

class MockAuthService extends Mock implements AuthService {}

class MockSharedPreferences extends Mock implements SharedPreferences {}

void main() {
  late AuthProvider authProvider;
  late MockAuthService mockAuthService;
  late MockSharedPreferences mockSharedPreferences;

  setUp(() {
    mockAuthService = MockAuthService();
    mockSharedPreferences = MockSharedPreferences();
    authProvider = AuthProvider();
  });

  group('AuthProvider', () {
    test('login success', () async {
      final loginRequest = LoginRequest(username: 'test', password: 'password');
      final loginResponse = LoginResponse(
        message: 'success',
        token: {'access_token': 'test_token'},
        username: 'test',
        accounts: [],
      );

      when(() => mockAuthService.login(any())).thenAnswer((_) async => loginResponse);
      when(() => mockSharedPreferences.setString(any(), any())).thenAnswer((_) async => true);

      await authProvider.login('test', 'password');

      expect(authProvider.isAuthenticated, true);
      expect(authProvider.token, 'test_token');
      expect(authProvider.username, 'test');
    });

    test('logout', () async {
      when(() => mockSharedPreferences.remove(any())).thenAnswer((_) async => true);

      authProvider.logout();

      expect(authProvider.isAuthenticated, false);
      expect(authProvider.token, null);
      expect(authProvider.username, null);
    });

    test('tryAutoLogin success', () async {
      when(() => mockSharedPreferences.containsKey('token')).thenReturn(true);
      when(() => mockSharedPreferences.getString('token')).thenReturn('test_token');
      when(() => mockSharedPreferences.getString('username')).thenReturn('test');

      await authProvider.tryAutoLogin();

      expect(authProvider.isAuthenticated, true);
      expect(authProvider.token, 'test_token');
      expect(authProvider.username, 'test');
    });

    test('tryAutoLogin failure', () async {
      when(() => mockSharedPreferences.containsKey('token')).thenReturn(false);

      await authProvider.tryAutoLogin();

      expect(authProvider.isAuthenticated, false);
      expect(authProvider.token, null);
      expect(authProvider.username, null);
    });
  });
}
