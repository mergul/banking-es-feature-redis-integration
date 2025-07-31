class DepositRequest {
  final double amount;

  DepositRequest({required this.amount});

  Map<String, dynamic> toJson() {
    return {
      'amount': amount,
    };
  }
}
