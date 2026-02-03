class KeyNotFoundError(Exception):
    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Key {key} not found in store")


class KeyValidationError(Exception):
    def __init__(self, key: str, message: str):
        self.key = key
        super().__init__(f"Key {key} is invalid: {message}")
