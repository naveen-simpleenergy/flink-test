class Transaction:
    def __init__(self, account_id, amount):
        self.account_id = account_id
        self.amount = amount
    
    def __str__(self):
        return f"Transaction(account_id={self.account_id}, amount={self.amount})"
    
    def __repr__(self):
        return self.__str__()

class Alert:
    def __init__(self, account_id):
        self.account_id = account_id
    
    def __str__(self):
        return f"FRAUD ALERT for account {self.account_id}"
    
    def __repr__(self):
        return self.__str__()