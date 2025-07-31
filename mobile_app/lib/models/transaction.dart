class Transaction {
  final String id;
  final double amount;
  final String transactionType;
  final DateTime timestamp;

  Transaction({
    required this.id,
    required this.amount,
    required this.transactionType,
    required this.timestamp,
  });

  factory Transaction.fromJson(Map<String, dynamic> json) {
    return Transaction(
      id: json['id'],
      amount: json['amount'].toDouble(),
      transactionType: json['transaction_type'],
      timestamp: DateTime.parse(json['timestamp']),
    );
  }
}
