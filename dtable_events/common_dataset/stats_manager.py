class StatsManager:

    def __init__(self) -> None:
        self.info = {}

    def __getitem__(self, key):
        return self.info.get(key)

    def __setitem__(self, key, value):
        self.info[key] = value

    def __delitem__(self, key):
        del self.info[key]

    def save(self):
        pass
