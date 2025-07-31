class Account {
  final String id;
  final String ownerName;
  final double balance;

  Account({required this.id, required this.ownerName, required this.balance});

  factory Account.fromJson(Map<String, dynamic> json) {
    return Account(
      id: json['id'],
      ownerName: json['owner_name'],
      balance: json['balance'].toDouble(),
    );
  }
}
