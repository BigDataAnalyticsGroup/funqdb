import atexit

from sqlitedict import SqliteDict

from fdm.API import AttributeFunction


class Store:
    """A SQLLite-backed AttributeFunction-store"""

    def __init__(
        self,
        file_name: str = "afstore.sqlite",
        attribute_function_space: str = "attribute_functions",
    ):
        self.attribute_function_buffer: dict[str, AttributeFunction] = {}
        self.file_name: str = file_name

        # DESIGN_CHOICE: should we have
        # one table per item_id?
        # not sure what the performance implications are at this point:
        # looking at the source code of sqlitedict, maybe it is better to have one table for all items?
        # or one table per AttributeFunction type
        # one table for everything for now:
        # note; in SQLiteDict, keys must be strings/text
        # https://github.com/piskvorky/sqlitedict/blob/96e81621fd6ab094efdd86e70fd57efe9d40ca12/sqlitedict.py#L228
        self.sqlite_dict: SqliteDict = SqliteDict(
            self.file_name, tablename=attribute_function_space, autocommit=True
        )

        # register to be called at exit:
        # atexit.register(self.close)

    def close(self):
        """Close the underlying SQLite dict."""
        self.sqlite_dict.close()

    def get(self, af_id_str: str) -> AttributeFunction:
        """Retrieve an AttributeFunction by its ID.
        @param af_id_str: The ID of the AttributeFunction to retrieve.
        @return: The AttributeFunction associated with the given ID.
        """
        if af_id_str not in self.attribute_function_buffer:
            self.load(af_id_str)  # store uses str keys

        return self.attribute_function_buffer[af_id_str]

    def put(self, af: AttributeFunction):
        """Store an AttributeFunction in the persistent store.
        @param af: The AttributeFunction to store.
        """

        self.sqlite_dict[af.uuid] = af
        self.sqlite_dict.commit()
        self.attribute_function_buffer[str(af.uuid)] = af

    def load(self, af_id_str: str) -> None:
        """Load an af_id_str from the persistent store into the buffer.
        @param item_id: The ID of the item to load.
        """

        try:
            self.attribute_function_buffer[af_id_str] = self.sqlite_dict[af_id_str]
        except KeyError as e:
            raise KeyError(f"ID '{af_id_str}' not found in the store.") from e

    def __len__(self) -> int:
        """Return the number of items in the store.
        @return: The number of items in the store.
        """
        return len(self.sqlite_dict)
