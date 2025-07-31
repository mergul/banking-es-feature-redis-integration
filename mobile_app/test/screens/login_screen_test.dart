import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:provider/provider.dart';
import 'package:mobile_app/providers/auth_provider.dart';
import 'package:mobile_app/screens/login_screen.dart';

class MockAuthProvider extends Mock implements AuthProvider {}

void main() {
  late MockAuthProvider mockAuthProvider;

  setUp(() {
    mockAuthProvider = MockAuthProvider();
  });

  testWidgets('LoginScreen calls login on button press', (WidgetTester tester) async {
    when(() => mockAuthProvider.login(any(), any())).thenAnswer((_) async {});

    await tester.pumpWidget(
      ChangeNotifierProvider<AuthProvider>.value(
        value: mockAuthProvider,
        child: const MaterialApp(
          home: LoginScreen(),
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).at(0), 'test');
    await tester.enterText(find.byType(TextField).at(1), 'password');
    await tester.tap(find.byType(ElevatedButton));
    await tester.pump();

    verify(() => mockAuthProvider.login('test', 'password')).called(1);
  });
}
