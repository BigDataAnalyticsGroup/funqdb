class in_subset:
    def __init__(self, whitelist: set[str]):
        self.whitelist = whitelist

    def __call__(self, *args, **kwargs) -> bool:
        return args[0] in self.whitelist
