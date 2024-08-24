import datetime

from bson import Timestamp, ObjectId, encode, decode
from bson.raw_bson import RawBSONDocument


def insert() -> dict:
    return {
        '_id': {
            '_data': '826692B4A3000000032B042C0100296E5A1004FC22C3AF7E40428CBD5D09FF3'
                     '547FFF2463C6F7065726174696F6E54797065003C696E736572740046646F63'
                     '756D656E744B65790046645F696400646692B4A31EDE014D28852865000004'
        },
        'clusterTime': Timestamp(1720890531, 1),
        'documentKey': {'_id': ObjectId('6692b4a31ede014d28852865')},
        'fullDocument': {
            '_id': ObjectId('6692b4a31ede014d28852865'),
            'a': 1,
            'c': 2,
        },
        'ns': {'coll': 'TestCollection', 'db': 'test-database'},
        'operationType': 'insert',
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=8, second=51,
            microsecond=823000
        )
    }


def update() -> dict:
    return {
        '_id': {
            '_data': '826692B55E000000022B042C0100296E5A1004FC22C3AF7E40428CBD5D09FF3'
                     '547FFF2463C6F7065726174696F6E54797065003C7570646174650046646F63'
                     '756D656E744B65790046645F696400646692B4A31EDE014D28852865000004'
        },
        'clusterTime': Timestamp(1720890718, 2),
        'documentKey': {'_id': ObjectId('6692b4a31ede014d28852865')},
        'fullDocument': {
            '_id': ObjectId('6692b4a31ede014d28852865'),
            'a': 2
        },
        'fullDocumentBeforeChange': {
            '_id': ObjectId('6692b4a31ede014d28852865'),
            'a': 1,
            'c': 2,
        },
        'ns': {'coll': 'TestCollection', 'db': 'test-database'},
        'operationType': 'update',
        'updateDescription': {
            'removedFields': ["c"],
            'truncatedArrays': [],
            'updatedFields': {'a': 2}
        },
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=11, second=58,
            microsecond=473000
        )
    }


def replace() -> dict:
    return {
        '_id': {
            '_data': '826692B5A8000000022B042C0100296E5A1004FC22C3AF7E40428CBD5D09FF3'
                     '547FFF2463C6F7065726174696F6E54797065003C7265706C6163650046646F'
                     '63756D656E744B65790046645F696400646692B4A31EDE014D28852865000004'
        },
        'clusterTime': Timestamp(1720890792, 2),
        'documentKey': {'_id': ObjectId('6692b4a31ede014d28852865')},
        'fullDocument': {
            '_id': ObjectId('6692b4a31ede014d28852865'),
            'a': 3
        },
        'fullDocumentBeforeChange': {
            '_id': ObjectId('6692b4a31ede014d28852865'),
            'a': 2
        },
        'ns': {'coll': 'TestCollection', 'db': 'test-database'},
        'operationType': 'replace',
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=13, second=12,
            microsecond=626000
        )
    }


def delete() -> dict:
    return {
        '_id': {
            '_data': '826692B66E000000012B042C0100296E5A1004FC22C3AF7E40428CBD5D09FF'
                     '3547FFF2463C6F7065726174696F6E54797065003C64656C6574650046646F'
                     '63756D656E744B65790046645F696400646692B4A31EDE014D28852865000004'
        },
        'clusterTime': Timestamp(1720890990, 2),
        'documentKey': {'_id': ObjectId('6692b4a31ede014d28852865')},
        'fullDocumentBeforeChange': {
            '_id': ObjectId('6692b4a31ede014d28852865'),
            'a': 3
        },
        'ns': {'coll': 'TestCollection', 'db': 'test-database'},
        'operationType': 'delete',
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=16, second=30,
            microsecond=724000
        )
    }


def drop() -> dict:
    return {
        '_id': {
            '_data': '826692B6D9000000012B042C0100296E5A1004FC22C3AF7E40428CB'
                     'D5D09FF3547FFF2463C6F7065726174696F6E54797065003C64726F'
                     '70000004'
        },
        'clusterTime': Timestamp(1720891097, 1),
        'ns': {'coll': 'TestCollection', 'db': 'test-database'},
        'operationType': 'drop',
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=18, second=17,
            microsecond=561000
        )
    }


def drop_database() -> dict:
    return {
        '_id': {
            '_data': '826692B807000000022B042C0100296E14463C6F70657'
                     '26174696F6E54797065003C64726F704461746162617365000004'
        },
        'clusterTime': Timestamp(1720891097, 1),
        'ns': {'db': 'test-database'},
        'operationType': 'dropDatabase',
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=18, second=17,
            microsecond=561000
        )
    }


def invalidate() -> dict:
    return {
        '_id': {
            '_data': '826692B6D9000000012B042C0100296F5A1004FC22C3AF7E40428CBD5'
                     'D09FF3547FFF2463C6F7065726174696F6E54797065003C64726F7000'
                     '0004'},
        'clusterTime': Timestamp(1720891399, 1),
        'operationType': 'invalidate',
        'wallTime': datetime.datetime(
            year=2024, month=7, day=13,
            hour=17, minute=18, second=17,
            microsecond=561000
        )
    }


def to_raw_bson(value: dict) -> RawBSONDocument:
    return RawBSONDocument(bson_bytes=encode(value))


def from_raw_bson(value: RawBSONDocument) -> dict:
    return decode(value.raw)


def events() -> list[dict]:
    return [
        insert(),  update(), replace(), delete(), drop(), drop_database(), invalidate()
    ]


def events_raw_bson() -> list[RawBSONDocument]:
    return [to_raw_bson(i) for i in events()]


def get_resume_token_from_event(event: dict) -> dict:
    return event['_id']


def get_resume_token_from_raw_event(event: RawBSONDocument) -> RawBSONDocument:
    return to_raw_bson(get_resume_token_from_event(from_raw_bson(event)))
