#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#


import atexit

from sqlitedict import SqliteDict

from fdm.API import AttributeFunction


class Store:
    """A SQLLite-backed AttributeFunction-store"""

    def __init__(
        self,
        file_name: str = "function_store.sqlite",
        attribute_function_space: str = "attribute_functions",
        add_reference_to_store_on_read: bool = True,
    ):
        self.attribute_function_buffer: dict[int, AttributeFunction] = {}
        self.file_name: str = file_name
        self.add_reference_to_store_on_read = add_reference_to_store_on_read

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
        atexit.register(self.close)

    def close(self):
        """Close the underlying SQLite dict."""
        self.sqlite_dict.close()

    def get(self, fid: int) -> AttributeFunction:
        """Retrieve an AttributeFunction by its ID.
        @param fid: The ID of the AttributeFunction to retrieve.
        @return: The AttributeFunction associated with the given ID.
        """
        if fid not in self.attribute_function_buffer:
            self.load(fid)  # store uses str keys

        return self.attribute_function_buffer[fid]

    def put(self, af: AttributeFunction):
        """Store an AttributeFunction in the persistent store.
        @param af: The AttributeFunction to store.
        """

        self.sqlite_dict[af.uuid] = af
        self.sqlite_dict.commit()
        self.attribute_function_buffer[af.uuid] = af

    def load(self, fid: int) -> None:
        """Load an fid from the persistent store into the buffer.
        @param item_id: The ID of the item to load.
        """

        try:
            af: AttributeFunction = self.sqlite_dict[fid]
            if self.add_reference_to_store_on_read:
                af.__dict__["store"] = self

            self.attribute_function_buffer[fid] = af
        except KeyError as e:
            raise KeyError(f"ID '{fid}' not found in the store.") from e

    def __len__(self) -> int:
        """Return the number of items in the store.
        @return: The number of items in the store.
        """
        return len(self.sqlite_dict)
