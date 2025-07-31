class WithdrawRequest {
  final double amount;

  WithdrawRequest({required this.amount});

  Map<String, dynamic> toJson() {
    return {
      'amount': amount,
    };
  }
}
