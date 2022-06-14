from typing import List, Dict
from haversine import Unit
from fastapi import FastAPI
import pandas as pd
import haversine as hs
from models import AddressModel, CreateAddressModel, UpdateAddressModel
from db_conf import database
from db_utils import (fetch_all_address, fetch_address_by_id, insert_new_address,
                      update_address, delete_address, delete_all_address, bulk_insert_new_address)

app = FastAPI()


@app.on_event("startup")
async def startup():
    """
    Called when the server is started
    :return:
    """
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    """
    Called when the server is shutting down
    :return:
    """
    await database.disconnect()


@app.get("/list", response_model=Dict[str, List[AddressModel]])
async def list_all_address():
    """
    Return a list of all addresses
    :return:
    """
    data = await fetch_all_address()
    return {"address": data}


@app.get("/retrieve", response_model=Dict[str, List])
async def retrieve(distance: float, lat: float, lng: float):
    """
    returns a list of address that are within the given distance
    :param distance: Distance for which the adress has to be fetched
    :param lat: Latitude form which the distance has to calculated
    :param lng: Longitude form which the distance has to calculated
    """

    def distance_from(loc1: tuple, loc2: tuple):
        """
        Euclidean Distance works for the flat surface like a Cartesian plain however,
        Earth is not flat. So we have to use a special type of formula known as Haversine Distance.

        :param loc1: coordinates of point 1
        :param loc2: coordinates of point 2
        :return: list of address within given distance
        """
        dist = hs.haversine(loc1, loc2, unit=Unit.KILOMETERS)
        return round(dist, 2)

    data = await fetch_all_address()

    df = pd.DataFrame(data, columns=["id", 'name', 'latitude', 'longitude', "deleted"])
    df['coordinates'] = list(zip(df.latitude, df.longitude))
    df['distance'] = df.apply(lambda row: distance_from(row['coordinates'], (lat, lng)), axis=1)
    df["distance_unit"] = Unit.KILOMETERS
    df.pop("deleted")

    # uncomment to print the first few rows
    # print(df.head())

    result_df = df.loc[df['distance'] <= distance]

    response = result_df.to_dict("records")
    return {"address": response}


@app.post("/create", response_model=AddressModel)
async def create(body: CreateAddressModel):
    """
    Creates an address
    """
    body = body.dict()
    last_record_id = await insert_new_address(body)
    return await fetch_address_by_id(last_record_id)


@app.put("/update/{address_id}", response_model=AddressModel)
async def update(address_id: int, body: UpdateAddressModel):
    """
    Updates an address
    """
    body = body.dict()
    await update_address(address_id, body)
    return await fetch_address_by_id(address_id)


@app.delete("/delete/{address_id}")
async def delete(address_id: int):
    """
    Soft delete an address
    """
    await delete_address(address_id)
    return {"success": True}


@app.delete("/delete_all")
async def delete_all():
    """
    Hard deletes all address from table
    :return:
    """
    await delete_all_address()
    return {"success": True}


@app.post("/load_data")
async def load_initial_data():
    """
    loads test data
    """
    cust_loc = pd.read_csv('customer_location.csv')
    cust_loc["is_deleted"] = False

    response = cust_loc.to_dict("records")
    await bulk_insert_new_address(response)

    return {"success": True}
