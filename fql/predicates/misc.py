from fql.util import Item


class in_subset:
    def __init__(self, whitelist: set[str]):
        self.whitelist = whitelist

    def __call__(self, *args, **kwargs) -> bool:
        return args[0] in self.whitelist


class TF_keys_in_list:
    def __init__(self, whitelist: set[str]):
        self.whitelist = whitelist

    def __call__(self, item: Item) -> bool:
        return all(i.key in self.whitelist for i in item.value)


class attribute_name_equivalence:
    """Predicate that checks if the attribute names of an Item's value match a given set."""

    def __init__(self, attribute_names: set[str]):
        self.attribute_names = attribute_names

    def __call__(self, item: Item) -> bool:
        return self.attribute_names == {i.key for i in item.value}
