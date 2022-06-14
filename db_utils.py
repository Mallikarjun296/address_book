from db_conf import database, address_table
from typing import List, Dict


async def fetch_all_address():
    query = address_table.select().where(address_table.c.is_deleted == False)
    data = await database.fetch_all(query)
    return data


async def fetch_address_by_id(address_id: int):
    query = address_table.select().where(address_table.c.id == address_id)
    return await database.fetch_one(query)


async def insert_new_address(data: Dict):
    """
    Inserts single row
    :param data: dictionary
    :return: None

    :param data:
    :return:
    """
    query = address_table.insert().values(**data)
    last_record_id = await database.execute(query)
    return last_record_id


async def bulk_insert_new_address(data: List[Dict]):
    """
    Insert multile rows at once
    :param data: list of data to be inserted
    :return: None
    """
    query = address_table.insert().values(data)
    await database.execute(query)


async def update_address(address_id: int, data: Dict):
    """
    Update name, and coordinates of a address
    :param address_id: PK of address
    :param data: Data to update
    :return: None
    """
    query = address_table.update().where(address_table.c.id == address_id)
    query = query.values({key: value for key, value in data.items() if value is not None})
    await database.execute(query)


async def delete_address(address_id: int):
    """
    Soft deletes a given address
    :param address_id:
    :return:
    """
    query = address_table.update().where(address_table.c.id == address_id).values(is_deleted=True)
    await database.execute(query)


async def delete_all_address():
    """
    Hard deletes all address rows form table
    :return: None
    """
    query = address_table.delete().where()
    await database.execute(query)
