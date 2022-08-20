# test_process.py
import pytest
from process.process import parser, convert_to_tuple, Database


@pytest.fixture
def data():
    return {
        "complete": [
            {
                "alias": "Nichole Barnett",
                "date": "16-07-2016",
                "username": "jubilantPup7",
                "address": "5140 Esther Cir",
                "user": {
                    "first_name": "Edgar",
                    "middle_name": "Chrystal",
                    "last_name": "Easley",
                    "zip_code": 71704,
                },
            },
            {
                "date": "27-09-2016",
                "address": "8739 Michelina St",
                "alias": "Charlie Perry",
                "id": 762339548,
                "first_name": "Thomas",
                "middle_name": "Lisa",
                "last_name": "Flory",
                "zip_code": 63628,
            },
        ],
        "some": [
            {
                "date": "19-03-2015",
                "id": 805481914,
                "username": "ardentMallard7",
                "alias": "Sherie Kauffman",
                "user": {
                    "middle_name": "Andrew",
                    "last_name": "Cloninger",
                    "zip_code": 50257,
                },
            },
            {
                "id": 287342803,
                "username": "curiousThrushe4",
                "address": "4503 Winston Dr",
                "date": "20-06-2017",
                "user": {"info": {"middle_name": "John", "last_name": "Anthony"}},
            },
            {
                "alias": "Fannie Marcinkiewicz",
                "id": 295211970,
                "date": "30-01-2014",
                "address": "9479 Alba St",
                "user": {"info": {"zip_code": 60673}},
            },
        ],
        "none": [
            {
                "address": "9958 Antonio Ave",
                "username": "crushedBuzzard1",
                "date": "01-05-2010",
                "alias": "Karen Robyn",
            }
        ],
    }


@pytest.fixture
def parsed_data():
    return [
        {
            "first_name": "Thomas",
            "middle_name": "Lisa",
            "last_name": "Flory",
            "zip_code": 63628,
        },
        {"middle_name": "Andrew", "last_name": "Cloninger", "zip_code": 50257},
        {"middle_name": "John", "last_name": "Anthony"},
        {"zip_code": 60673},
        {},
    ]


@pytest.fixture
def file_path():
    return "altius", "./data/altius/group00/client00/13583.json"


def test_parser(data):
    # testing parsing of json
    # should return None if no elements are found
    # else return first, middle, last, zip code
    # partial data should be omitted?
    # pass
    keys = ["first_name", "middle_name", "last_name", "zip_code"]

    # missing all
    rows = []
    for d in data["none"]:
        row = parser(d, keys, vals={})
        rows.append(row)
    print(rows)
    assert rows == [{}]

    # missing some
    rows = []
    for d in data["some"]:
        row = parser(d, keys, vals={})
        rows.append(row)
    assert rows == [
        {"middle_name": "Andrew", "last_name": "Cloninger", "zip_code": 50257},
        {"middle_name": "John", "last_name": "Anthony"},
        {"zip_code": 60673},
    ]

    # missing none
    rows = []
    for d in data["complete"]:
        row = parser(d, keys, vals={})
        rows.append(row)
    print(rows)
    assert rows == [
        {
            "first_name": "Edgar",
            "middle_name": "Chrystal",
            "last_name": "Easley",
            "zip_code": 71704,
        },
        {
            "first_name": "Thomas",
            "middle_name": "Lisa",
            "last_name": "Flory",
            "zip_code": 63628,
        },
    ]


def test_convert_to_tuple(parsed_data):
    # given a dict should covert it to list
    # choose csv for encoding and special char around names
    # flat files dont include wrapping of columns
    rows = []
    for p in parsed_data:
        row = convert_to_tuple(p)
        rows.append(row)

    assert rows == [
        ("Thomas", "Lisa", "Flory", 63628),
        (None, "Andrew", "Cloninger", 50257),
        (None, "John", "Anthony", None),
        (None, None, None, 60673),
        None,
    ]


def test_database_execute():
    manifold = Database()
    sql = "insert into test values (1, 'abc');"
    manifold.execute(sql)

    sql = "select count(*) from test;"
    result = manifold.execute(sql, fetch=True, all_results=False)

    assert result == (1,)

    sql = "delete from test where id = 4;"
    manifold.execute(sql)
    manifold.close()
