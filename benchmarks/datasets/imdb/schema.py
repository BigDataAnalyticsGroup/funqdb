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
"""The IMDB schema the Join Order Benchmark queries are written against, shared by
every IMDB-based dataset."""

from benchmarks.datasets.schema import PrimaryKey
from benchmarks.datasets.dataset_types import References, Reference

IMDB_REFERENCES: References = {
    "aka_name": {"person_id": Reference("person_id", "name")},
    "aka_title": {
        "movie_id": Reference("movie_id", "title"),
        "kind_id": Reference("kind_id", "kind_type"),
        "episode_of_id": Reference("episode_of_id", "title"),
    },
    "cast_info": {
        "person_id": Reference("person_id", "name"),
        "movie_id": Reference("movie_id", "title"),
        "person_role_id": Reference("person_role_id", "char_name"),
        "role_id": Reference("role_id", "role_type"),
    },
    "complete_cast": {
        "movie_id": Reference("movie_id", "title"),
        "subject_id": Reference("subject_id", "comp_cast_type"),
        "status_id": Reference("status_id", "comp_cast_type"),
    },
    "movie_companies": {
        "movie_id": Reference("movie_id", "title"),
        "company_id": Reference("company_id", "company_name"),
        "company_type_id": Reference("company_type_id", "company_type"),
    },
    "movie_info": {
        "movie_id": Reference("movie_id", "title"),
        "info_type_id": Reference("info_type_id", "info_type"),
    },
    "movie_info_idx": {
        "movie_id": Reference("movie_id", "title"),
        "info_type_id": Reference("info_type_id", "info_type"),
    },
    "movie_keyword": {
        "movie_id": Reference("movie_id", "title"),
        "keyword_id": Reference("keyword_id", "keyword"),
    },
    "movie_link": {
        "movie_id": Reference("movie_id", "title"),
        "linked_movie_id": Reference("linked_movie_id", "title"),
        "link_type_id": Reference("link_type_id", "link_type"),
    },
    "person_info": {
        "person_id": Reference("person_id", "name"),
        "info_type_id": Reference("info_type_id", "info_type"),
    },
    "title": {
        "kind_id": Reference("kind_id", "kind_type"),
        "episode_of_id": Reference("episode_of_id", "title"),
    },
}

IMDB_PRIMARY_KEYS: dict[str, PrimaryKey] = {
    table: PrimaryKey.of("id")
    for table in (
        "aka_name",
        "aka_title",
        "cast_info",
        "char_name",
        "comp_cast_type",
        "company_name",
        "company_type",
        "complete_cast",
        "info_type",
        "keyword",
        "kind_type",
        "link_type",
        "movie_companies",
        "movie_info_idx",
        "movie_keyword",
        "movie_link",
        "name",
        "role_type",
        "title",
        "movie_info",
        "person_info",
    )
}
