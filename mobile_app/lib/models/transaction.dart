class Transaction {
  final String id;
  final String accountId;
  final double amount;
  final String transactionType;
  final DateTime timestamp;
  final String description;

  Transaction({
    required this.id,
    required this.accountId,
    required this.amount,
    required this.transactionType,
    required this.timestamp,
    required this.description,
  });

  factory Transaction.fromJson(Map<String, dynamic> json) {
    return Transaction(
      id: json['id'],
      accountId: json['account_id'] ?? '',
      amount: json['amount'].toDouble(),
      transactionType: json['transaction_type'],
      timestamp: DateTime.parse(json['timestamp']),
      description: json['description'] ?? '',
    );
  }
}
