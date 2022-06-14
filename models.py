from typing import Optional
from pydantic import BaseModel


class AddressModel(BaseModel):
    id: int
    name: str
    longitude: float
    latitude: float
    is_deleted: bool


class CreateAddressModel(BaseModel):
    name: str
    longitude: float
    latitude: float
    is_deleted: bool = False


class UpdateAddressModel(BaseModel):
    name: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    is_deleted: bool = False
