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
        self.sqlite_dict: SqliteDict = SqliteDict(
            self.file_name, tablename=attribute_function_space, autocommit=True
        )

        # register to be called at exit:
        # atexit.register(self.close)

    def close(self):
        """Close the underlying SQLite dict."""
        self.sqlite_dict.close()

    def get(self, af_id: int) -> AttributeFunction:
        """Retrieve an AttributeFunction by its ID.
        @param af_id: The ID of the AttributeFunction to retrieve.
        @return: The AttributeFunction associated with the given ID.
        """
        if str(af_id) not in self.attribute_function_buffer:
            self.load(str(af_id))  # store uses str keys

        return self.attribute_function_buffer[str(af_id)]

    def put(self, af: AttributeFunction):
        """Store an AttributeFunction in the persistent store.
        @param af: The AttributeFunction to store.
        """

        self.sqlite_dict[42] = af
        self.sqlite_dict.commit()
        self.attribute_function_buffer[42] = af

    def load(self, af_id: str) -> None:
        """Load an af_id from the persistent store into the buffer.
        @param item_id: The ID of the item to load.
        """

        try:
            self.attribute_function_buffer[af_id] = self.sqlite_dict[af_id]
        except KeyError as e:
            raise KeyError(f"ID '{af_id}' not found in the store.") from e
